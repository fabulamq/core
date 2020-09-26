package api

import (
	"bytes"
	"context"
	"fmt"
	"github.com/hpcloud/tail"
	"github.com/zeusmq/internal/infra/log"
	"io"
	"net"
	"os"
	"strings"
	"sync"
)

type apiStatus struct {
	err     error
	isReady bool
}

func Start() (*Controller, chan apiStatus) {
	controller := &Controller{
		file:    file{},
		pLocker: sync.Mutex{},
	}
	chStatus := make(chan apiStatus)
	listener, err := net.Listen("tcp", "localhost:9998")
	if err != nil {
		chStatus <- apiStatus{err: err, isReady: false}
	}

	go func() {
		chStatus <- apiStatus{err: nil, isReady: true}
		for {
			conn, err := listener.Accept()

			if err != nil {
				chStatus <- apiStatus{err: err, isReady: false}
			}
			controller.start(conn)
		}
	}()

	return controller, chStatus
}

type Controller struct {
	file        file
	pLocker     sync.Mutex
	consumerMap sync.Map
}

func (controller *Controller) start(conn net.Conn) {
	go func() {
		ctx := context.Background()

		line, err := readLine(conn)
		if err != nil {
			return
		}
		lineSpl := strings.Split(string(line), ";")

		ctxWithId := context.WithValue(context.Background(), "id", lineSpl[1])
		ctx = context.WithValue(ctxWithId, "ch", lineSpl[2])

		switch lineSpl[0] {
		case "c":
			consumerInfo := &consumerInfo{
				ID:       lineSpl[1],
				Ch:       lineSpl[2],
				Topic:    lineSpl[3],
				Strategy: lineSpl[4],
				conn:     conn,
				Ctx:      ctx,
			}
			err = consumerInfo.listen(ctx, controller)
			controller.consumerMap.Delete(consumerInfo.makeKey())
			log.Warn(ctx, "producer.listen.error", err)
		case "p":
			producer := producer{conn: conn}
			err = producer.listen(ctx, controller)
			controller.pLocker.Unlock()
			log.Warn(ctx, "producer.listen.err", err)
		case "r":
		}
		conn.Close()
	}()
}

type producer struct {
	conn net.Conn
}

func (producer producer) listen(ctx context.Context, controller *Controller) error {
	log.Info(ctx, "producer.listen")
	write(producer.conn, []byte("ok"))
	for {
		producerMsg, err := readLine(producer.conn)
		controller.pLocker.Lock()
		if err != nil {
			return err
		}

		producerMsg = append([]byte(fmt.Sprintf("%d;", controller.file.GetOffset())), producerMsg...)
		log.Info(ctx, fmt.Sprintf("producer.listen.Read: [%s]", producerMsg))

		// perform save here "topic:msg"
		err = controller.file.WriteFile(ctx, producerMsg)
		if err != nil {
			return err
		}

		controller.file.AddOffset()

		err = write(producer.conn, []byte("ok"))
		if err != nil {
			return err
		}
		controller.pLocker.Unlock()
		log.Info(ctx, fmt.Sprintf("producer.listen.SendedOK: [%s]", producerMsg))
	}
}

func readLine(conn io.Reader) ([]byte, error) {
	buf := make([]byte, 0, 128) // big buffer
	tmp := make([]byte, 1024)   // using small tmo buffer for demonstrating
	for {
		n, err := conn.Read(tmp)
		if err != nil {
			if err != io.EOF {
				return nil, err
			}
			break
		}
		idx := bytes.Index(tmp, []byte("\n"))
		if idx == -1 {
			buf = append(buf, tmp[:n]...)
		} else {
			buf = append(buf, tmp[:idx]...)
			break
		}
	}
	return buf, nil
}

func write(writer io.Writer, msg []byte) error {
	_, err := writer.Write(append(msg, []byte("\n")...))
	return err
}

type file struct {
	file   *os.File
	m      sync.Mutex
	offset uint64
	once   sync.Once
}

func (f *file) createFile() {
	if _, err := os.Stat("/var/tmp/queue.txt"); os.IsNotExist(err) {
		var err error
		f.file, err = os.Create("/var/tmp/queue.txt")
		if err != nil {
			log.Fatal(context.Background(), err.Error())
		}
	}
}

func (f *file) AddOffset() {
	f.offset++
}

func (f *file) GetOffset() uint64 {
	f.once.Do(func() {
		f.file.Seek(0, 2)
	})
	return f.offset
}

func (f file) TailFile() (chan *tail.Line, error) {
	t, err := tail.TailFile("/var/tmp/queue.txt", tail.Config{Follow: true})
	if err != nil {
		return nil, err
	}
	return t.Lines, nil
}

func (f file) OpenFile() (*os.File, error) {
	file, err := os.Open("/var/tmp/queue.txt")
	if err != nil {
		return nil, err
	}
	return file, nil
}

func (f *file) WriteFile(ctx context.Context, b []byte) error {
	f.createFile()
	_, err := f.file.Write(append(b, []byte("\n")...))
	if err != nil {
		return err
	}
	return nil
}

func (f *file) CleanFile() {
	f.offset = 0
	os.Remove("/var/tmp/queue.txt")
	f.createFile()
}

type consumerInfo struct {
	ID       string
	Ch       string
	Topic    string
	Ctx      context.Context
	Strategy string
	conn     net.Conn
}

func (consumer *consumerInfo) listen(ctx context.Context, controller *Controller) error {
	log.Info(ctx, "consumer.listen")
	write(consumer.conn, []byte("ok"))

	_, ok := controller.consumerMap.Load(consumer.makeKey())
	if !ok {
		controller.consumerMap.Store(consumer.makeKey(), consumer)
	}

	tail, err := controller.file.TailFile()
	if err != nil {
		return err
	}

	for {
		line := <-tail
		l := []byte(fmt.Sprintf("%s", line.Text))
		log.Info(ctx, fmt.Sprintf("consumer.listen.readLine: [%s]", l))
		if err != nil {
			return err
		}
		err = write(consumer.conn, l)
		if err != nil {
			return err
		}
		consumerRes, err := readLine(consumer.conn)
		if err != nil {
			return err
		}
		if string(consumerRes) != "ok" {
			return fmt.Errorf("error NOK")
		}
		log.Info(ctx, fmt.Sprintf("consumer.listen.readLine.complete: [%s]", l))
	}
}

func (c consumerInfo) makeKey() string {
	return fmt.Sprintf("%s_%s", c.ID, c.Ch)
}
