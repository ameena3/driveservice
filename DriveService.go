package driveservice

import (
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"strings"
	"sync"

	"google.golang.org/api/drive/v3"
)

var mx sync.Mutex

// FileService ...
type FileService struct {
	instance *drive.Service
	wg       sync.WaitGroup
	flist    map[string]*drive.File
	idChilds map[string][]*drive.File
}

// Initialize ...
//Singleton pattern for getting the fileservice instance.
func (fs *FileService) Initialize(client *http.Client) (*drive.Service, error) {
	if fs.instance != nil {
		return fs.instance, nil
	}
	mx.Lock()
	defer mx.Unlock()
	var err error
	fs.instance, err = drive.New(client)
	return fs.instance, err

}

//UploadDriveBatched ...
// Crud operations
func (fs *FileService) UploadDriveBatched(batchSize int, dir string) {
	var filenameswithpath []string
	// Get the latest file list to check what dirctory needs to be created.
	fs.GetAllFilesOnDrive()

	err := filepath.Walk(dir,
		func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}
			if !info.IsDir() {
				filenameswithpath = append(filenameswithpath, path)
			}
			return nil
		})
	if err != nil {
		log.Println(err)
	}
	for i, file := range filenameswithpath {
		fs.wg.Add(1)
		go fs.uploadFile(file)
		if (i+1)%batchSize == 0 {
			fs.wg.Wait()
		}
	}

	fs.wg.Wait()
}

// Deletes the files to be called only from the batched files delete
func (fs *FileService) uploadFile(fileName string) {
	var dirstructure []string
	var parentID string
	//Check the runtime info for the operating system.
	if runtime.GOOS == "linux" {
		dirstructure = strings.Split(path.Dir(fileName), "/")
	} else {
		dirstructure = strings.Split(path.Dir(fileName), "\\")
	}
	// Check if the directory exists if not create it.
	parentID, exists := fs.checkIfDirExists(dirstructure)
	if !exists {
		parentID, err := fs.createDirStructure(dirstructure)
		log.Printf("Directory not found created directory with id %v \n", parentID)
		checkError("Error not able to create the directort structure", err)
	}
	up, err := os.Open(fileName)
	if err != nil {
		log.Printf("Unable to read file: %v", err)
	}
	defer up.Close()
	ssz, _ := up.Stat()
	fmt.Printf("uploading file %v , Size is  %v \n", ssz.Name(), ssz.Size())
	file := drive.File{Name: up.Name(), Parents: []string{parentID}}

	_, err = fs.instance.Files.Create(&file).ResumableMedia(nil, up, ssz.Size(), "").ProgressUpdater(call).Do()
	if err != nil {
		log.Fatalf("Unable to upload file: %v", err)
	}
	fs.wg.Done()
}
func call(current, total int64) {
	fmt.Printf("Percent uploaded: %v \n", ((current * 100) / total))
}

//DeleteFileBatched ...
func (fs *FileService) DeleteFileBatched(prefix string, batchSize int) {
	r, err := fs.instance.Files.List().Fields("nextPageToken, files(id, name)").Do()
	if err != nil {
		log.Fatalf("cannot retrieve the file list the error is: %v", err)
	}
	for {
		if len(r.Files) == 0 {
			fmt.Println("No files found.")
		} else {
			for _, i := range r.Files {

				fmt.Printf("name is: %s The ID is: %s\n ", i.Name, i.Id)
				if len(i.Name) > len(prefix) {
					if i.Name[:len(prefix)] == prefix {
						fmt.Printf("Deleting file %v \n", i.Name)
						err := fs.instance.Files.Delete(i.Id).Do()
						fmt.Println("Deleted file")
						if err != nil {
							log.Fatalf("cannot delete file the error is: %v\n", err)
						}
					}
				}

			}
		}
		if len(r.NextPageToken) == 0 {
			break
		} else {
			r, err = fs.instance.Files.List().Fields("nextPageToken, files(id, name)").PageToken(r.NextPageToken).Do()
		}
	}

}

// GetAllFilesOnDrive ...
func (fs *FileService) GetAllFilesOnDrive() map[string]*drive.File {
	// Remove the original file list as we always want to be in sync
	if len(fs.flist) != 0 {
		for filename := range fs.flist {
			delete(fs.flist, filename)
		}
	}
	if len(fs.idChilds) != 0 {
		for filename := range fs.idChilds {
			delete(fs.idChilds, filename)
		}
	}
	fs.flist = make(map[string]*drive.File)
	fs.idChilds = make(map[string][]*drive.File)
	r, err := fs.instance.Files.List().Fields("nextPageToken, files(id, name, parents, kind, mimeType)").Do()
	checkError("Unable to retrieve the file list", err)
	for {
		if len(r.Files) == 0 {
			log.Println("No files found")
			break
		} else {
			for _, file := range r.Files {
				fs.flist[file.Name] = file
				//If there is a parent.
				if len(file.Parents) != 0 {
					// If the parent id exists then append to the child value
					if _, ok := fs.idChilds[file.Parents[0]]; ok {
						fs.idChilds[file.Parents[0]] = append(fs.idChilds[file.Parents[0]], file)
					} else {
						filed := make([]*drive.File, 1)
						filed = append(filed, file)
						fs.idChilds[file.Parents[0]] = filed
					}
				}
			}
			if len(r.NextPageToken) == 0 {
				break
			} else {
				r, err = fs.instance.Files.List().Fields("nextPageToken, files(id, name, parents,kind, mimeType)").PageToken(r.NextPageToken).Do()
			}
		}
	}
	return fs.flist
}

//Checks if the Directory structure exists on gdrive
func (fs *FileService) checkIfDirExists(directories []string) (string, bool) {
	exists := false
	var parent string
	var index int
	for i, dir := range directories {
		index = i
		// If the parent files exists
		if _, ok := fs.flist[dir]; ok {
			found := false
			// Find all the childs and match with
			for _, value := range fs.idChilds[fs.flist[dir].Id] {
				// We have confirmed the directory exists
				if i == (len(directories) - 1) {
					found = true
					break
				}
				if value.Name == directories[i+1] {
					found = true
					break
				}
			}

		}
	}
	if exists {
		parent = fs.flist[directories[len(directories)-1]].Id
		log.Printf("Found the directory structure pareent where the file will be inserted is: %v \n", parent)
	} else {
		log.Println("Dir structure not found creating one ...")
		parent = ""
	}
	return parent, exists
}

func (fs *FileService) createDirStructure(directories []string) (string, error) {
	return "", errors.New("not implemented")
}

// As we are checking the error toomany times.
func checkError(message string, err error) {
	if err != nil {
		log.Fatalf("%v : %v \n", message, err)
	}
}
