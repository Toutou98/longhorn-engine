package dataconn

import (
	"net"
	"time"

	"github.com/longhorn/longhorn-engine/pkg/types"
	"github.com/sirupsen/logrus"
	"libguestfs.org/libnbd"
)

type nbdClient struct {
	conn      net.Conn
	opTimeout time.Duration
	h         *libnbd.Libnbd
	types.ReaderWriterUnmapperAt
}

func NewNBDClient(conn net.Conn, engineToReplicaTimeout time.Duration) *nbdClient {
	c := &nbdClient{
		conn:      conn,
		opTimeout: engineToReplicaTimeout,
	}
	go c.handle()

	return c
}

func (c *nbdClient) WriteAt(buf []byte, offset int64) (int, error) {
	err := c.h.Pwrite(buf, uint64(offset), nil)
	if err != nil {
		return 0, err
	}
	return len(buf), nil
}

func (c *nbdClient) UnmapAt(length uint32, offset int64) (int, error) {
	return 0, nil
}

func (c *nbdClient) ReadAt(buf []byte, offset int64) (int, error) {
	err := c.h.Pread(buf, uint64(offset), nil)
	if err != nil {
		return 0, err
	}
	return len(buf), nil
}

func (c *nbdClient) handle() {
	h, err := libnbd.Create()
	if err != nil {
		panic(err)
	}
	defer h.Close()

	c.h = h
	uri := "nbd://127.0.0.1:9503"
	err = c.h.ConnectUri(uri)
	if err != nil {
		panic(err)
	}
	size, err := c.h.GetSize()
	if err != nil {
		panic(err)
	}
	logrus.Infof("Size of %s = %d\n", uri, size)

}
