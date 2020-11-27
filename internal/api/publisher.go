package api

import (
	"bufio"
	"context"
	"fmt"
	log "github.com/sirupsen/logrus"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"
)

type publisher struct {
	ID            string
	Status        chan apiStatus
	publisherKind publisherKind
	HqUrl         string

	locker   sync.Mutex
	book     *book
	weight   int
	listener net.Listener
	// general locker

	storyReaderMap  sync.Map
	storyWriterMap  sync.Map
	branchMap       sync.Map
	Hosts           []string

	looseBranch     chan bool
	gainBranch      chan bool
	promoteElection chan bool
}


func (publisher publisher) accept() (net.Conn, error) {
	return publisher.listener.Accept()
}

func (publisher *publisher) acceptConn(conn net.Conn) {
	go func() {
		ctx := context.Background()

		chRes := readLine(conn)
		res := <- chRes
		if res.err != nil {
			return
		}
		lineSpl := strings.Split(string(res.b), ";")

		switch lineSpl[0] {
		case "sr":
			if !publisher.acceptStoryReader() {
				break
			}
			storyReader := newStoryReader(ctx, lineSpl, publisher)
			publisher.storyReaderMap.Store(storyReader.ID, storyReader)
			err := storyReader.Listen(conn)
			publisher.storyReaderMap.Delete(storyReader.ID)
			log.Warn("storyReader.error", err)
		case "sw":
			if !publisher.acceptStoryWriter() {
				break
			}
			storyWriter := newStoryWriter(ctx, conn, publisher)
			publisher.storyWriterMap.Store(storyWriter.ID, storyWriter)
			err := storyWriter.listen()
			publisher.storyWriterMap.Delete(storyWriter.ID)
			log.Warn(fmt.Sprintf("storyWriter.error: %s", err.Error()))
		case "branch":
			if !publisher.acceptBranch() {
				break
			}
			publisher.gainBranch <- true
			branch := newBranch(ctx, lineSpl, publisher)
			publisher.branchMap.Store(branch.ID, branch)
			err := branch.Listen(conn)
			publisher.branchMap.Delete(branch.ID)
			publisher.looseBranch <- true
			log.Warn(fmt.Sprintf("branch.error: %s", err.Error()))
			case "info":
			log.Info(fmt.Sprintf("(%s) sending info", publisher.ID))
			conn.Write([]byte(fmt.Sprintf("%s;%s;%d;%d;%d;%s\n",
				Undefined,
				publisher.ID,
				publisher.weight,
				publisher.book.mark.getChapter(),
				publisher.book.mark.getLine(),
				publisher.publisherKind,
			)))
		}
		conn.Close()
	}()
}

func (publisher *publisher) reset() {
	publisher.setPublisherKind(Undefined)
	publisher.locker.Lock()
	defer publisher.locker.Unlock()
	publisher.storyReaderMap.Range(func(key, value interface{}) bool {
		storyReader := value.(*storyReader)
		log.Info(fmt.Sprintf("(%s) reset.storyReaderMap on ID=%s", publisher.ID, storyReader.ID))
		storyReader.cancel()
		return true
	})
	publisher.branchMap.Range(func(key, value interface{}) bool {
		branch := value.(*branch)
		log.Info(fmt.Sprintf("(%s) reset.branchMap on ID=%s", publisher.ID, branch.ID))
		branch.cancel()
		return true
	})
	publisher.storyWriterMap.Range(func(key, value interface{}) bool {
		storyWriter := value.(*storyWriter)
		log.Info(fmt.Sprintf("(%s) reset.storyWriterMap on ID=%s", publisher.ID, storyWriter.ID))
		storyWriter.cancel()
		return true
	})
}

func (publisher *publisher) Stop() {
	publisher.reset()
	publisher.listener.Close()
}

func (publisher *publisher) acceptStoryReader()bool{
	publisher.locker.Lock()
	defer publisher.locker.Unlock()
	if publisher.publisherKind == Unique || publisher.publisherKind == Branch {
		return true
	}
	if publisher.publisherKind == HeadQuarter && publisher.quo() > 0.5 {
		return true
	}
	return false
}

func (publisher *publisher) isUndefined()bool{
	publisher.locker.Lock()
	defer publisher.locker.Unlock()
	if publisher.publisherKind == Undefined {
		return true
	}
	return false
}

func (publisher *publisher) isHeadQuarter()bool{
	publisher.locker.Lock()
	defer publisher.locker.Unlock()
	if publisher.publisherKind == HeadQuarter {
		return true
	}
	return false
}

func (publisher *publisher) isFundingAHeadQuarter()bool{
	publisher.locker.Lock()
	defer publisher.locker.Unlock()
	if publisher.publisherKind == FundHeadQuarter {
		return true
	}
	return false
}

func (publisher *publisher) acceptStoryWriter()bool{
	publisher.locker.Lock()
	defer publisher.locker.Unlock()
	if publisher.publisherKind == HeadQuarter || publisher.publisherKind == Unique {
		return true
	}
	return false
}

func (publisher *publisher) acceptBranch()bool{
	publisher.locker.Lock()
	defer publisher.locker.Unlock()
	if publisher.publisherKind == HeadQuarter || publisher.publisherKind == FundHeadQuarter {
		return true
	}
	return false
}


func (publisher *publisher) setPublisherKind(pk publisherKind){
	publisher.locker.Lock()
	defer publisher.locker.Unlock()
	publisher.publisherKind = pk
}

func (publisher *publisher) quo() float32 {
	totalInstances := len(publisher.Hosts) + 1

	totalActiveBranch := 1
	publisher.branchMap.Range(func(key, value interface{}) bool {
		totalActiveBranch++
		return true
	})
	return float32(totalActiveBranch) / float32(totalInstances)
}

func (publisher *publisher) electionController() {
	ctx := context.Background()
	go func() {
		log.Info(fmt.Sprintf("(%s) starting election", publisher.ID))
		for {
			totalInstances := len(publisher.Hosts) + 1

			hostsInfo := publisher.getHostsInfo()

			quo := float32(len(hostsInfo)) / float32(totalInstances)
			if quo <= 0.5 {
				log.Info(fmt.Sprintf("(%s) cannot elect by quo", publisher.ID))
				time.Sleep(1 * time.Second)
				continue
			}
			// check map status
			hostInfo := hostsInfo.getTheOne()
			log.Info(fmt.Sprintf("(%s) selected publisher: %s", publisher.ID, hostInfo.id))
			if hostInfo.host == "local" {
				publisher.startHeadQuarter(ctx)
			}else {
				err := publisher.startBranch(ctx, hostInfo.host)
				log.Warn(fmt.Sprintf("(%s) error on branch start: %s", publisher.ID, err.Error()))
			}
			time.Sleep(100 * time.Millisecond)
		}
	}()
}

func (publisher *publisher) getHostsInfo() hostsInfo {
	hostsInfo := make(hostsInfo, 0)

	hostsInfo["local"] = hostInfo{
		id:     publisher.ID,
		host:   "local",
		weight: publisher.weight,
		mark:   publisher.book.mark,
		kind:   Undefined,
	}

	for _, host := range publisher.Hosts {
		log.Info(fmt.Sprintf("(%s) dialing to host: %s", publisher.ID, host))
		conn, err := net.DialTimeout("tcp", host, time.Millisecond * 200)
		if err != nil {
			continue
		}
		_, err = conn.Write([]byte("info\n"))
		if err != nil {
			continue
		}
		scanner := bufio.NewScanner(conn)
		scanner.Split(bufio.ScanLines)

		if scanner.Scan() {
			txtSpl := strings.Split(scanner.Text(), ";")

			id := txtSpl[1]
			weight, err := strconv.Atoi(txtSpl[2])
			if err != nil {
				continue
			}
			chapter, err := strconv.ParseUint(txtSpl[3], 10, 64)
			if err != nil {
				continue
			}
			line, err := strconv.ParseUint(txtSpl[4], 10, 64)
			if err != nil {
				continue
			}
			hostsInfo[host] = hostInfo{
				id:     id,
				kind:   GetPublisherKind(txtSpl[5]),
				host:   host,
				weight: weight,
				mark: &mark{
					chapter: chapter,
					line:    line,
				},
			}
		}
	}
	return hostsInfo
}

func (publisher *publisher) startBranch(ctx context.Context, hqUrl string)error {
	publisher.Status <- apiStatus{
		Err:     nil,
		IsReady: true,
		kind:    Branch,
	}
	log.Info(fmt.Sprintf("(%s) startBranch", publisher.ID))
	publisher.setPublisherKind(Branch)
	conn, err := net.DialTimeout("tcp", hqUrl, time.Millisecond * 200)
	if err != nil {
		return err
	}
	_, err = conn.Write([]byte(fmt.Sprintf("branch;%s;%d;%d\n", publisher.ID, publisher.book.mark.chapter, publisher.book.mark.line)))
	if err != nil {
		return err
	}
	scanner := bufio.NewScanner(conn)
	scanner.Split(bufio.ScanLines)

	for {
		scanner.Scan()
		txt := scanner.Text()
		log.Info(fmt.Sprintf("startBranch.getMessage (%s): %s", publisher.ID, txt))
		if txt == ""{
			return fmt.Errorf("connection closed")
		}
		txtSpl := strings.Split(txt, ";")
		if txtSpl[0] != "msg" {
			continue
		}

		mark, err := publisher.book.Write([]byte(fmt.Sprintf("%s",txtSpl[2])))
		if err != nil {
			conn.Write([]byte("nok\n"))
			return err
		}
		publisher.book.mark = mark
		_, err = conn.Write([]byte("ok\n"))
		if err != nil {
			return err
		}
	}
}


func (publisher *publisher) startHeadQuarter(ctx context.Context) {
	log.Info(fmt.Sprintf("(%s) trying to stabilish a head quarter", publisher.ID))
	publisher.setPublisherKind(FundHeadQuarter)

	L: for {
		select {
		case <-time.After(5 * time.Second):
			if publisher.publisherKind == HeadQuarter {
				continue
			}
			// finish
			log.Warn(fmt.Sprintf("(%s) timeout to stabilish a head quarter", publisher.ID))
			break L
		case <- publisher.looseBranch:
			log.Info(fmt.Sprintf("(%s) loose all branches, minory", publisher.ID))
			if publisher.quo() <= 0.5 {
				break L
			}
		case <- publisher.gainBranch:
			if publisher.publisherKind == HeadQuarter {
				continue
			}
			log.Info(fmt.Sprintf("(%s) new branch on HQ, current quo=%v", publisher.ID, publisher.quo()))
			if publisher.quo() <= 0.5 {
				continue
			}
			log.Info(fmt.Sprintf("(%s) start a new head quarter", publisher.ID))
			publisher.Status <- apiStatus{
				IsReady:      true,
				kind:         HeadQuarter,
				AcceptWriter: true,
			}
			publisher.setPublisherKind(HeadQuarter)
		case <- publisher.promoteElection:
			log.Info(fmt.Sprintf("(%s) promote new election", publisher.ID))
			// promote new election
			break L
		}
	}
	log.Warn(fmt.Sprintf("(%s) reset all headquarter", publisher.ID))
	publisher.reset()
}

func (publisher *publisher) PromoteElection() {
	if publisher.isHeadQuarter() {
		publisher.promoteElection <- true
	}
}