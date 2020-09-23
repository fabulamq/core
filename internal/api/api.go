package api

import (
	"context"
	"github.com/zeusmq/internal/domain"
	"log"
	"net"
)

func Start(ctx context.Context) (error, chan bool) {
	isReady := make(chan bool)
	controller, err := domain.Start(ctx)
	if err != nil {
		return err, nil
	}
	listener, err := net.Listen("tcp", "localhost:9998")
	if err != nil {
		return err, nil
	}

	connPool := make(chan net.Conn)
	go func() {
		isReady <- true
		for {
			conn, err := listener.Accept()
			if err != nil {
				continue
			}
			connPool <- conn
		}
	}()
	go func() {
		for {
			select {
			case <-ctx.Done():
				log.Fatal("finish")
			case conn := <-connPool:
				err := controller.InitSubscriber(ctx, conn)
				if err != nil {
					return
				}
			}
		}
	}()
	return nil, isReady
}

func controller(ctx context.Context) {
	select {}
}
