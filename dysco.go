package dysco

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/url"

	az "github.com/Azure/azure-storage-blob-go/azblob"
	log "github.com/sirupsen/logrus"
)

const VolumeBlockSize int = 1024

type AzureBlobVolume struct {
	blobUrl  az.PageBlobURL
	blobSize int64
}

func handleErrors(err error) {
	if err != nil {
		if serr, ok := err.(az.StorageError); ok { // This error is a Service-specific
			switch serr.ServiceCode() { // Compare serviceCode to ServiceCodeXxx constants
			case az.ServiceCodeContainerAlreadyExists:
				log.Error("Received 409. Container already exists")
				return
			}
		}
		log.Fatal(err)
	}
}

func CreatePageBlob(name string, blocks int64, container string, account string, key string) (*AzureBlobVolume, error) {
	return openPageBlob(name, int64(VolumeBlockSize)*blocks, true, container, account, key)
}

func OpenPageBlob(name string, container string, account string, key string) (*AzureBlobVolume, error) {
	return openPageBlob(name, 0, false, container, account, key)
}

func openPageBlob(name string,
	blobSize int64,
	newBlob bool,
	container string,
	account string,
	key string) (*AzureBlobVolume, error) {
	c, err := az.NewSharedKeyCredential(account, key)
	if err != nil {
		log.Error("Invalid credentials with error: " + err.Error())
	}
	p := az.NewPipeline(c, az.PipelineOptions{})

	url, _ := url.Parse(fmt.Sprintf("http://%s.blob.core.windows.net/%s", account, container))

	contUrl := az.NewContainerURL(*url, p)
	blobUrl := contUrl.NewPageBlobURL(name)

	ctx := context.Background()
	var marker az.Marker
	var found bool

	listBlob, err := contUrl.ListBlobsFlatSegment(ctx, marker, az.ListBlobsSegmentOptions{})
	if err != nil {
		log.Errorf("ListBlobsFlatSegment failed: [%s]: %v", contUrl.String(), err.Error())
		return nil, err
	}
	for _, blobInfo := range listBlob.Segment.BlobItems {
		if blobInfo.Properties.BlobType == az.BlobPageBlob && blobInfo.Name == name {
			log.Infof("Found page blob [%s] with size: %d", blobInfo.Name, blobInfo.Properties.ContentLength)
			blobSize = *blobInfo.Properties.ContentLength
			found = true
			break
		}
	}

	if newBlob {
		log.Infof("Going to Create page blob: [%s] with size: %d", blobUrl.String(), blobSize)

		_, err = blobUrl.Create(ctx, blobSize, 0, az.BlobHTTPHeaders{}, az.Metadata{}, az.BlobAccessConditions{})
		handleErrors(err)
		if err != nil {
			log.Errorf("Create page blob failed: [%s]: %v", blobUrl.String(), err.Error())
			return nil, err
		}
	} else if !found {
		err = fmt.Errorf("Page blob not found")
		log.Errorf("Page blob not found: [%s]: %v", blobUrl.String(), err.Error())
		return nil, err
	}

	v := AzureBlobVolume{
		blobUrl:  blobUrl,
		blobSize: blobSize,
	}
	return &v, nil
}

func (v *AzureBlobVolume) GetSize() int64 {
	return v.blobSize
}

func (v *AzureBlobVolume) ReadAt(p []byte, off int64) (n int, err error) {
	//log.Debugf("read: [%d], len: [%d]", off, len(p))
	len := len(p)
	ctx := context.Background()

	dr, err := v.blobUrl.Download(ctx, off, int64(len), az.BlobAccessConditions{}, false)
	if err != nil {
		log.Errorf("Download failed: [%d], len: [%d]: %v", off, len, err.Error())
		return 0, err
	}

	body := dr.Body(az.RetryReaderOptions{MaxRetryRequests: 0})
	pos := 0

	for err == nil {
		n, err = body.Read(p[pos:len])
		if err != nil && err != io.EOF {
			log.Errorf("Download failed: [%d], len: [%d]: %v", off, len, err.Error())
			return 0, err
		}
		pos = pos + n
	}

	return pos, nil
}

func (v *AzureBlobVolume) WriteAt(p []byte, off int64) (n int, err error) {
	//log.Debugf("write: [%d], len: [%d]", off, len(p))
	ctx := context.Background()

	r := bytes.NewReader(p)
	_, err = v.blobUrl.UploadPages(ctx, off, r, az.PageBlobAccessConditions{}, nil)
	if err != nil && err != io.EOF {
		log.Errorf("UploadPages failed: [%d], len: [%d]: %v", off, len(p), err.Error())
		return 0, err
	}

	return len(p), err
}
