package netFileStream

import (
	"context"
	"encoding/gob"
	"io"
	"net"
	"os"
	"time"

	log "github.com/sirupsen/logrus"
)

type NetStream struct {
	f        *os.File
	conn     *net.Conn
	port     int
	blobSize int64
}

func NewNetStream(f *os.File, port int, ctx context.Context) (*NetStream, error) {
	fi, _ := f.Stat()

	v := &NetStream{
		f:        f,
		port:     port,
		blobSize: fi.Size(),
	}

	err := v.listenAndServe(ctx)
	return v, err
}

func (v *NetStream) listenAndServe(ctx context.Context) error {
	addr, err := net.ResolveTCPAddr("tcp", "0.0.0.0:8888")
	if err != nil {
		return err
	}
	l, err := net.ListenTCP("tcp", addr)
	if err != nil {
		return err
	}

	go func() {
		for {
			select {
			case <-ctx.Done():
				l.Close()
				return
			default:
				l.SetDeadline(time.Now().Add(10 * time.Second))
				conn, err := l.Accept()
				if err != nil {
					continue
				}
				handleConnection(conn, ctx)
			}
		}
	}()

	return nil
}

type Frame struct {
	cmd     int32
	payload []byte
}

func handleConnection(conn net.Conn, ctx context.Context) {
	log.Infof("New connection from: [%s]", conn.RemoteAddr())

	defer conn.Close()
	var data Frame

	dec := gob.NewDecoder(conn)
	for {
		select {
		case <-ctx.Done():
			return
		default:
			conn.SetReadDeadline(time.Now().Add(10 * time.Second))
			err := dec.Decode(&data)
			if err != nil {
				if err == io.EOF {

				}
				return
			}
		}
	}
}

func (v *NetStream) GetSize() int64 {
	return v.blobSize
}

func (v *NetStream) Close() error {
	return nil
}

func (v *NetStream) ReadAt(p []byte, off int64) (n int, err error) {
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

func (v *NetStream) WriteAt(p []byte, off int64) (n int, err error) {
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
