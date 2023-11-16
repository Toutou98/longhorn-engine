package dataconn

import (
	"net"
	"time"

	"github.com/sirupsen/logrus"
	"libguestfs.org/libnbd"
)

const MAX_NUMBER_OF_CONNECTIONS = 8

type nbdClientWrapper struct {
	clients []nbdClient
	next    int
}

type nbdClient struct {
	conn      net.Conn
	opTimeout time.Duration
	h         *libnbd.Libnbd
}

func NewNBDClientWrapper(conn net.Conn, engineToReplicaTimeout time.Duration, nbdEnabled int) *nbdClientWrapper {
	clientList := []nbdClient{}
	wrapper := &nbdClientWrapper{
		clients: clientList,
	}

	client := NewNBDClient(conn, engineToReplicaTimeout)
	h, err := libnbd.Create()
	if err != nil {
		panic(err)
	}

	client.h = h
	uri := "nbd://127.0.0.1:9503"
	err = client.h.ConnectUri(uri)
	if err != nil {
		panic(err)
	}
	wrapper.clients = append(wrapper.clients, *client)

	multiConnEnabled, err := client.h.CanMultiConn()
	if err != nil {
		panic(err)
	}
	if multiConnEnabled == true {
		for i := 1; i < MAX_NUMBER_OF_CONNECTIONS; i++ {
			client := NewNBDClient(conn, engineToReplicaTimeout)
			h, err := libnbd.Create()
			if err != nil {
				panic(err)
			}

			client.h = h
			uri := "nbd://127.0.0.1:9503"
			err = client.h.ConnectUri(uri)
			if err != nil {
				panic(err)
			}
			wrapper.clients = append(wrapper.clients, *client)
		}
	}

	return wrapper
}

func NewNBDClient(conn net.Conn, engineToReplicaTimeout time.Duration) *nbdClient {
	c := &nbdClient{
		conn:      conn,
		opTimeout: engineToReplicaTimeout,
	}
	return c
}

func (w *nbdClientWrapper) WriteAt(buf []byte, offset int64) (int, error) {
	w.next = (w.next + 1) % MAX_NUMBER_OF_CONNECTIONS
	index := w.next
	for i := 0; i < MAX_NUMBER_OF_CONNECTIONS; i++ {
		err := w.clients[index].h.Pwrite(buf, uint64(offset), nil)
		if err == nil {
			break
		} else {
			return 0, err
		}
	}

	return len(buf), nil
}

func (w *nbdClientWrapper) UnmapAt(length uint32, offset int64) (int, error) {
	return int(length), nil
}

func (w *nbdClientWrapper) ReadAt(buf []byte, offset int64) (int, error) {
	w.next = (w.next + 1) % MAX_NUMBER_OF_CONNECTIONS
	index := w.next
	for i := 0; i < MAX_NUMBER_OF_CONNECTIONS; i++ {
		err := w.clients[index].h.Pread(buf, uint64(offset), nil)
		if err == nil {
			break
		} else {
			return 0, err
		}
	}

	return len(buf), nil
}

/*
Fill later
func (c *nbdClient) Ping() error{

}
*/

func (c *nbdClient) handle() {

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
