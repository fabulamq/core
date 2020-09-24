package domain

import (
	"context"
	"fmt"
	"github.com/zeusmq/internal/infra/log"
	"github.com/zeusmq/internal/services"
	"io"
	"net"
	"strings"
	"sync"
)

type subscriberInfo struct {
	ID    string
	Ch    string
	Kind  string
	Topic string
}

type NewMessage struct {
	Topic   string
	Message string
}

type consumerInfo struct {
	ID    string
	Ch    string
	Topic string
	Conn  net.Conn
	Ctx   context.Context

	r *io.PipeReader
	w *io.PipeWriter
}

var once = sync.Once{}
var mapLocker = sync.Mutex{}
var publishLocker = sync.Mutex{}
var consumersMap map[string]map[string]*consumerInfo

var publishReader *io.PipeReader
var publishWriter *io.PipeWriter
var currOffset uint64

func AddConnection(conn net.Conn) {
	once.Do(func() {
		consumersMap = make(map[string]map[string]*consumerInfo)
		publishReader, publishWriter = io.Pipe()
		currOffset = 0
		go publisherCentral()
	})

	go func() {
		ctx := context.Background()

		line, err := services.Get().ReadLine(ctx, conn)
		if err != nil {
			return
		}
		lineSpl := strings.Split(string(line), ";")
		subInfo := subscriberInfo{
			Kind:  lineSpl[0],
			ID:    lineSpl[1],
			Ch:    lineSpl[2],
			Topic: lineSpl[3],
		}
		ctxWithId := context.WithValue(context.Background(), "id", subInfo.ID)
		ctx = context.WithValue(ctxWithId, "ch", subInfo.Ch)

		switch subInfo.Kind {
		case "c":
			r, w := io.Pipe()
			c := consumerInfo{
				ID:    subInfo.ID,
				Ch:    subInfo.Ch,
				Topic: subInfo.Topic,
				Ctx:   ctx,
				Conn:  conn,
				r:     r,
				w:     w,
			}
			err = consumerStep(ctx, c)
			mapLocker.Lock()
			delete(consumersMap[c.ID], c.Ch)
			mapLocker.Unlock()
		case "p":
			err = producerStep(ctx, conn)
		case "r":
		}
		conn.Close()
	}()
}

func consumerStep(ctx context.Context, consInfo consumerInfo) error {
	log.Info(ctx, "consumerStep")
	consInfo.Conn.Write([]byte("ok\n"))
	mapLocker.Lock()
	if _, ok := consumersMap[consInfo.ID]; !ok {
		consumersMap[consInfo.ID] = make(map[string]*consumerInfo)
	}
	consumersMap[consInfo.ID][consInfo.Ch] = &consInfo
	mapLocker.Unlock()

	for {
		l, err := services.Get().ReadLine(ctx, consInfo.r)
		log.Info(ctx, fmt.Sprintf("consumerStep.readLine: [%s]", l))
		if err != nil {
			log.Warn(ctx, "domain.writeToConsumer.readLineError", err)
			return err
		}
		err = services.Get().Write(ctx, consInfo.Conn, l)
		if err != nil {
			log.Warn(ctx, "domain.writeToConsumer.writeError", err)
			return err
		}
		consumerRes, err := services.Get().ReadLine(ctx, consInfo.Conn)
		if err != nil {
			log.Warn(ctx, "domain.writeToConsumer.readError", err)
			return err
		}
		if string(consumerRes) != "ok" {
			log.Warn(ctx, "domain.writeToConsumer.notOK", err)
			return fmt.Errorf("error NOK")
		}

	}
}

func producerStep(ctx context.Context, conn net.Conn) error {
	log.Info(ctx, "producerStep")
	conn.Write([]byte("ok\n"))
	for {
		producerMsg, err := services.Get().ReadLine(ctx, conn)
		publishLocker.Lock()
		if err != nil {
			return err
		}

		producerMsg = append([]byte(fmt.Sprintf("%d;", currOffset)), producerMsg...)
		log.Info(ctx, fmt.Sprintf("producerStep.Read: [%s]", producerMsg))

		// perform save here "topic:msg"

		// write to publisher

		err = services.Get().Write(ctx, publishWriter, producerMsg)
		if err != nil {
			return err
		}
		currOffset++
		publishLocker.Unlock()

		err = services.Get().Write(ctx, conn, []byte("ok"))
		if err != nil {
			return err
		}
	}
}

func publisherCentral() {
	ctx := context.Background()
	for {
		line, err := services.Get().ReadLine(context.Background(), publishReader)
		if err != nil {
			log.Fatal(ctx, err.Error())
		}
		mapLocker.Lock()
		for id, _ := range consumersMap {
			for ch, _ := range consumersMap[id] {
				log.Info(consumersMap[id][ch].Ctx, fmt.Sprintf("publisherCentral: [%s]", line))
				services.Get().Write(ctx, consumersMap[id][ch].w, line)
			}
		}
		mapLocker.Unlock()
	}
}
