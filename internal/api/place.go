package api

import (
	"fmt"
	"net"
	"sync"
)

type Place interface {
	accept()(net.Conn, error)
	acceptConn(net.Conn)
}

func DeployPlace(c Config)(Place,chan apiStatus){
	chStatus := make(chan apiStatus)

	book, err := startBook(bookConfig{
		MaxLinerPerChapter: c.OffsetPerChapter,
		Folder:             c.Folder,
	})
	if err != nil {
		chStatus <- apiStatus{Err: err, IsReady: false}
	}

	defaultPort := "9998"
	if c.Port != "" {
		defaultPort = c.Port
	}

	listener, err := net.Listen("tcp", fmt.Sprintf("localhost:%s", defaultPort))
	if err != nil {
		chStatus <- apiStatus{Err: err, IsReady: false}
	}

	if len(c.Hosts) == 0{
		return &publisher{
			book:     book,
			listener: listener,
			locker:   sync.Mutex{},
		}, chStatus
	}
	// ask who is the...
	//for {
	//	conn, Err := listener.Accept()
	//}
	return nil, chStatus
}

type placeStatus struct {
	Weight int
	Mark   mark
	Status string
}