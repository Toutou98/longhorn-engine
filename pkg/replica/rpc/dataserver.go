package rpc

import (
	"flag"
	"fmt"
	"net"

	"github.com/sirupsen/logrus"

	"github.com/longhorn/longhorn-engine/pkg/dataconn"
	"github.com/longhorn/longhorn-engine/pkg/replica"
	"github.com/longhorn/longhorn-engine/pkg/types"

	"github.com/pojntfx/go-nbd/pkg/server"
)

type DataServer struct {
	protocol types.DataServerProtocol
	address  string
	s        *replica.Server
	frontend string
}

func NewDataServer(protocol types.DataServerProtocol, address string, s *replica.Server, frontend string) *DataServer {
	return &DataServer{
		protocol: protocol,
		address:  address,
		s:        s,
		frontend: frontend,
	}
}

func (s *DataServer) ListenAndServe() error {
	switch s.protocol {
	case types.DataServerProtocolTCP:
		return s.listenAndServeTCP()
	case types.DataServerProtocolUNIX:
		return s.listenAndServeUNIX()
	default:
		return fmt.Errorf("unsupported protocol: %v", s.protocol)
	}
}

func (s *DataServer) listenAndServeTCP() error {
	addr, err := net.ResolveTCPAddr("tcp", s.address)
	if err != nil {
		return err
	}

	l, err := net.ListenTCP("tcp", addr)
	if err != nil {
		return err
	}

	for {
		conn, err := l.AcceptTCP()
		if err != nil {
			logrus.WithError(err).Error("failed to accept tcp connection")
			continue
		}

		logrus.Infof("New connection from: %v", conn.RemoteAddr())

		switch s.frontend {
		case "default":
			go func(conn net.Conn) {
				server := dataconn.NewServer(conn, s.s)
				server.Handle()
			}(conn)
		case "nbd":
			go func() {
				b := NewNBDFileBackend(s.s)
				name := flag.String("name", "default", "Export name")
				description := flag.String("description", "The default export", "Export description")
				readOnly := s.s.GetReadOnly()
				blockSize := s.s.Replica().Info().SectorSize
				if err := server.Handle(
					conn,
					[]*server.Export{
						{
							Name:        *name,
							Description: *description,
							Backend:     b,
						},
					},
					&server.Options{
						ReadOnly:           readOnly,
						MinimumBlockSize:   uint32(blockSize),
						PreferredBlockSize: uint32(blockSize),
						MaximumBlockSize:   uint32(blockSize),
						SupportsMultiConn:  true,
					}); err != nil {
					panic(err)
				}
			}()
		}
	}
}

func (s *DataServer) listenAndServeUNIX() error {
	unixAddr, err := net.ResolveUnixAddr("unix", s.address)
	if err != nil {
		return err
	}

	l, err := net.ListenUnix("unix", unixAddr)
	if err != nil {
		return err
	}

	for {
		conn, err := l.AcceptUnix()
		if err != nil {
			logrus.WithError(err).Error("failed to accept unix-domain-socket connection")
			continue
		}
		logrus.Infof("New connection from: %v", conn.RemoteAddr())

		switch s.frontend {
		case "default":
			go func(conn net.Conn) {
				server := dataconn.NewServer(conn, s.s)
				server.Handle()
			}(conn)
		case "nbd":
			go func(conn net.Conn) {
				b := NewNBDFileBackend(s.s)
				name := flag.String("name", "default", "Export name")
				description := flag.String("description", "The default export", "Export description")
				readOnly := s.s.GetReadOnly()
				blockSize := s.s.Replica().Info().SectorSize
				if err := server.Handle(
					conn,
					[]*server.Export{
						{
							Name:        *name,
							Description: *description,
							Backend:     b,
						},
					},
					&server.Options{
						ReadOnly:           readOnly,
						MinimumBlockSize:   uint32(blockSize),
						PreferredBlockSize: uint32(blockSize),
						MaximumBlockSize:   uint32(blockSize),
						SupportsMultiConn:  true,
					}); err != nil {
					panic(err)
				}
			}(conn)
		}
	}
}

type nbdFileBackend struct {
	s *replica.Server
}

func NewNBDFileBackend(s *replica.Server) *nbdFileBackend {
	return &nbdFileBackend{s}
}

func (b *nbdFileBackend) ReadAt(p []byte, off int64) (n int, err error) {
	n, err = b.s.ReadAt(p, off)
	return
}

func (b *nbdFileBackend) WriteAt(p []byte, off int64) (n int, err error) {
	n, err = b.s.WriteAt(p, off)
	return
}

func (b *nbdFileBackend) Size() (int64, error) {
	_, info := b.s.Status()
	return info.Size, nil
}

func (b *nbdFileBackend) Sync() error {
	return nil
}
