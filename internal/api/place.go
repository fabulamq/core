package api

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"
)

type hostInfo struct {
	host   string
	weight int
	mark   *mark
}

type hostsInfo map[string]hostInfo

func (hinfos hostsInfo) getTheOne() *hostInfo {
	topMark := new(mark)
	// get the top mark
	for _, hostInfo := range hinfos {
		if topMark.isBefore(*hostInfo.mark) {
			topMark = hostInfo.mark
		}
	}
	selectedHost := new(hostInfo)
	for _, hostInfo := range hinfos {
		if !hostInfo.mark.isEqual(*topMark) {
			continue
		}
		if selectedHost.weight > hostInfo.weight {
			continue
		}
		selectedHost = &hostInfo
	}
	return selectedHost
}

func deployPlace(c Config) (chan *publisher, chan apiStatus) {
	ctx := context.Background()
	chStatus := make(chan apiStatus)
	chPlace := make(chan *publisher)

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
		go func() {
			chPlace <- &publisher{
				publisherKind: Unique,
				book:          book,
				listener:      listener,
				locker:        sync.Mutex{},
			}
		}()
		return chPlace, chStatus
	}
	// ask who is the...

	hostsInfo := make(hostsInfo, 0)

	hostsInfo["local"] = hostInfo{
		host:   "local",
		weight: c.Weight,
		mark:   book.mark,
	}

	// write to other hosts
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			_, err = conn.Write([]byte(fmt.Sprintf("%d;%d;%d\n", c.Weight, book.mark.chapter, book.mark.line)))
			if err != nil {
				return
			}
		}
	}()

	// read and decide
	go func() {
		for {
			activeInstances := 0
			for _, host := range c.Hosts {
				conn, err := net.Dial("tcp", host)
				if err != nil {
					continue
				}
				scanner := bufio.NewScanner(conn)
				scanner.Split(bufio.ScanLines)

				if scanner.Scan() {
					txtSpl := strings.Split(scanner.Text(), ";")
					if len(txtSpl) != 3 {
						continue
					}
					weight, err := strconv.Atoi(txtSpl[0])
					if err != nil {
						continue
					}
					chapter, err := strconv.ParseUint(txtSpl[1], 10, 64)
					if err != nil {
						continue
					}
					line, err := strconv.ParseUint(txtSpl[2], 10, 64)
					if err != nil {
						continue
					}
					hostsInfo[host] = hostInfo{
						host:   host,
						weight: weight,
						mark: &mark{
							chapter: chapter,
							line:    line,
						},
					}
					activeInstances++
				}
			}
			quo := float32(activeInstances) / float32(totalInstances)
			if quo < 0.5 {
				time.Sleep(1 * time.Second)
				continue
			}
			err = listener.Close()
			if err != nil {
				log.Fatal(ctx, err.Error())
			}
			// check map status
			hostInfo := hostsInfo.getTheOne()
			if hostInfo.host == "local" {
				chPlace <- &publisher{
					publisherKind: Master,
					book:          book,
					listener:      listener,
					locker:        sync.Mutex{},
				}
			}else {
				chPlace <- &publisher{
					publisherKind: Replica,
					book:          book,
					listener:      listener,
					locker:        sync.Mutex{},
				}
			}
			return
		}

	}()
	return chPlace, chStatus
}

type placeStatus struct {
	Weight int
	Mark   mark
	Status string
}
