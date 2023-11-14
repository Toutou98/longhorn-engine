package dataconn

import (
	"net"
	"time"

	"github.com/sirupsen/logrus"
	"libguestfs.org/libnbd"
)

type nbdClient struct {
	conn      net.Conn
	opTimeout time.Duration
	h         *libnbd.Libnbd
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

/*
Fill later
func (c *nbdClient) Ping() error{

}
*/

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
	//c.printInfo()
	for {
		// loop waiting for requests
		i := 1
		if i == 2 {
			break
		}
	}
}

func (c *nbdClient) printInfo() {
	uri, err := c.h.GetUri()
	if err != nil {
		panic(err)
	}
	size, err := c.h.GetSize()
	if err != nil {
		panic(err)
	}
	blockSizeMin, err := c.h.GetBlockSize(0)
	if err != nil {
		panic(err)
	}
	blockSizePref, err := c.h.GetBlockSize(1)
	if err != nil {
		panic(err)
	}
	blockSizeMax, err := c.h.GetBlockSize(2)
	if err != nil {
		panic(err)
	}
	canonicalExportName, err := c.h.GetCanonicalExportName()
	if err != nil {
		panic(err)
	}
	exportName, err := c.h.GetExportName()
	if err != nil {
		panic(err)
	}
	exportDescription, err := c.h.GetExportName()
	if err != nil {
		panic(err)
	}
	handleName, err := c.h.GetHandleName()
	if err != nil {
		panic(err)
	}
	preadInitializer, err := c.h.GetPreadInitialize()
	if err != nil {
		panic(err)
	}
	protocol, err := c.h.GetProtocol()
	if err != nil {
		panic(err)
	}
	isReadOnly, err := c.h.IsReadOnly()
	if err != nil {
		panic(err)
	}
	logrus.Infof("Size of %s = %d\n", *uri, size)
	logrus.Infof("BlockSizeMin of %s = %d\n", *uri, blockSizeMin)
	logrus.Infof("BlockSizePref of %s = %d\n", *uri, blockSizePref)
	logrus.Infof("BlockSizeMax of %s = %d\n", *uri, blockSizeMax)
	logrus.Infof("CanonicalExportName of %s = %s\n", *uri, *canonicalExportName)
	logrus.Infof("ExportName of %s = %s\n", *uri, *exportName)
	logrus.Infof("ExportDescription of %s = %s\n", *uri, *exportDescription)
	logrus.Infof("Handle Name of %s = %s\n", *uri, *handleName)
	logrus.Infof("Pread is init of %s = %t\n", *uri, preadInitializer)
	logrus.Infof("Protocol of %s = %s\n", *uri, *protocol)
	logrus.Infof("IsReadOnly of %s = %t\n", *uri, isReadOnly)
}
