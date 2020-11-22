package api

import (
	"fmt"
	"net"
	"sync"
)

func deployPublisher(c Config) *publisher {
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

	totalInstances := len(c.Hosts) + 1
	if totalInstances == 1 {
		return &publisher{
			ID:              c.ID,
			publisherKind:   Unique,
			Status:          chStatus,
			book:            book,
			weight:          c.Weight,
			listener:        listener,
			storyReaderMap:  sync.Map{},
			storyWriterMap:  sync.Map{},
			gainBranch:      make(chan bool),
			looseBranch:     make(chan bool),
			promoteElection: make(chan bool),
			Hosts:           c.Hosts,
		}
	}


	// read and decide

	return &publisher{
		ID:            c.ID,
		Status:        chStatus,
		publisherKind: Undefined,
		book:          book,
		weight:        c.Weight,
		listener:      listener,
		Hosts:         c.Hosts,
		gainBranch:    make(chan bool),
		looseBranch:   make(chan bool),
		promoteElection: make(chan bool),
	}
}