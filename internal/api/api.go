package api

import (
	"context"
	"github.com/zeusmq/internal/domain"
	"net"
)

func Start(isReady chan bool) error {
	listener, err := net.Listen("tcp", "localhost:9998")
	if err != nil {
		return err
	}
	isReady <- true
	for {
		conn, err := listener.Accept()
		if err != nil {
			return err
		}
		domain.AddConnection(conn)
	}

}

func controller(ctx context.Context) {
	select {}
}
