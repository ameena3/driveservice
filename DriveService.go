package driveservice

import (
	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"sync"

	"google.golang.org/api/drive/v3"
)

var mx sync.Mutex

type hashSetMap map[string]struct{}

// FileService ...
type FileService struct {
	instance *drive.Service
	wg       sync.WaitGroup
	fl       hashSetMap
}

// Initialize ...
//Singleton pattern for getting the fileservice instance.
func (fs *FileService) Initialize(client *http.Client) (*drive.Service, error) {
	fs.fl = make(hashSetMap)
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
func (fs *FileService) UploadDriveBatched(parentID string, batchSize int, dir string) {
	var filenameswithpath []string
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
		go fs.uploadFile(parentID, file)
		if (i+1)%batchSize == 0 {
			fs.wg.Wait()
		}
	}

	fs.wg.Wait()
}

// Deletes the files to be called only from the batched files delete
func (fs *FileService) uploadFile(parentID string, fileName string) {

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
func (fs *FileService) GetAllFilesOnDrive() []string {
	var list []string
	r, err := fs.instance.Files.List().Fields("nextPageToken, files(id, name)").Do()
	checkError("Unable to retrieve the file list", err)
	for {
		if len(r.Files) == 0 {
			log.Println("No files found")
			break
		} else {
			for _, i := range r.Files {
				list = append(list, i.Name)
			}
			if len(r.NextPageToken) == 0 {
				break
			} else {
				r, err = fs.instance.Files.List().Fields("nextPageToken, files(id, name)").PageToken(r.NextPageToken).Do()
			}
		}
	}
	return list
}

// As we are checking the error toomany times.
func checkError(message string, err error) {
	if err != nil {
		log.Fatalf("%v : %v", message, err)
	}
}
