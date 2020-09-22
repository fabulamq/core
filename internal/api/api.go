package api

import (
	"context"
	"github.com/zeusmq/internal/domain"
	"log"
	"net"
)

func Start(ctx context.Context) error {
	listener, err := net.Listen("tcp", "localhost:9998")
	if err != nil {
		return err
	}
	go func() {
		for {
			select {
			case <-ctx.Done():
				log.Fatal("finish")
			case conn := <-connectionPool(listener):
				err = domain.AddSubscriber(ctx, conn)
				if err != nil {
					return
				}
			}
		}
	}()
	return nil
}

func connectionPool(listener net.Listener) chan net.Conn {
	connChan := make(chan net.Conn)
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				continue
			}
			connChan <- conn
		}
	}()
	return connChan

}

func controller(ctx context.Context) {
	select {}
}
