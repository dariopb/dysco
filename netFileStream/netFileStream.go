package netFileStream

import (
	"context"
	"io"
	"os"

	log "github.com/sirupsen/logrus"
)

const VolumeBlockSize int = 1024

type NetFileVolume struct {
	f        *os.File
	n        *NetStream
	blobSize int64
}

func NewNetFileVolume(filename string, ctx context.Context) (*NetFileVolume, error) {
	f, err := os.OpenFile(filename, os.O_RDWR, 0700)
	if err != nil {
		return nil, err
	}

	fi, _ := f.Stat()

	v := &NetFileVolume{
		f:        f,
		blobSize: fi.Size(),
	}

	v.n, err = NewNetStream(f, 8888, ctx)
	if err != nil {
		f.Close()
		return nil, err
	}

	return v, nil
}

func (v *NetFileVolume) GetSize() int64 {
	return v.blobSize
}

func (v *NetFileVolume) Close() error {
	return v.f.Close()
}

func (v *NetFileVolume) ReadAt(p []byte, off int64) (n int, err error) {
	log.Debugf("read: [%d], len: [%d]", off, len(p))
	len := len(p)
	pos := 0

	for err == nil {
		n, err = v.f.ReadAt(p[pos:], off+int64(pos))
		if err != nil && err != io.EOF {
			log.Errorf("read file failed: [%d], len: [%d]: %v", off, len, err.Error())
			return 0, err
		}
		pos = pos + n
	}

	return pos, nil
}

func (v *NetFileVolume) WriteAt(p []byte, off int64) (n int, err error) {
	log.Debugf("write: [%d], len: [%d]", off, len(p))
	len := len(p)
	pos := 0

	for err == nil {
		n, err = v.f.WriteAt(p[pos:], off+int64(pos))
		if err != nil && err != io.EOF {
			log.Errorf("write file failed: [%d], len: [%d]: %v", off, len, err.Error())
			return 0, err
		}
		pos = pos + n
	}

	return pos, nil
}
