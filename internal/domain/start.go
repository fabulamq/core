package domain

import (
	"context"
	"fmt"
	"github.com/google/uuid"
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
	m sync.Mutex
}

// reordenar
// unlock no lugar certo
// cid??

var once = sync.Once{}
var mapLocker = sync.RWMutex{}
var publishLocker = sync.Mutex{}
var mapRelation map[string]bool
var consumerMap = sync.Map{}

//var publishReader *io.PipeReader
//var publishWriter *io.PipeWriter
var msgChan chan []byte

func AddConnection(conn net.Conn) {
	once.Do(func() {
		mapRelation = make(map[string]bool)
		msgChan = make(chan []byte)
		//publishReader, publishWriter = io.Pipe()
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
			c := &consumerInfo{
				ID:    subInfo.ID,
				Ch:    subInfo.Ch,
				Topic: subInfo.Topic,
				Ctx:   ctx,
				Conn:  conn,
				r:     r,
				w:     w,
			}
			err = consumerStep(ctx, c)
			consumerMap.Delete(makeKey(c))
			c.m.Unlock()
			log.Warn(ctx, "consumerStep.error", err)
		case "p":
			err = producerStep(ctx, conn)
			publishLocker.Unlock()
			log.Warn(ctx, "producerStep.err", err)
		case "r":
		}
		conn.Close()
	}()
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

		producerMsg = append([]byte(fmt.Sprintf("%d;", services.Get().GetOffset())), producerMsg...)
		log.Info(ctx, fmt.Sprintf("producerStep.Read: [%s]", producerMsg))

		// perform save here "topic:msg"

		// write to central publisher
		msgChan <- producerMsg

		log.Info(ctx, fmt.Sprintf("producerStep.WriteToCentral: [%s]", producerMsg))
		services.Get().AddOffset()
		publishLocker.Unlock()

		err = services.Get().Write(ctx, conn, []byte("ok"))
		if err != nil {
			return err
		}
		log.Info(ctx, fmt.Sprintf("producerStep.SendedOK: [%s]", producerMsg))
	}
}

func publisherCentral() {
	ctx := context.Background()
	for {
		line := <-msgChan
		uid := uuid.New()
		log.Info(ctx, fmt.Sprintf("publisherCentral.init: %s", uid))
		for key, _ := range mapRelation {
			consInfo, ok := consumerMap.Load(key)
			if ok {
				info := consInfo.(*consumerInfo)
				info.m.Lock()
				if _, ok := consumerMap.Load(key); !ok {
					continue
				}
				log.Info(info.Ctx, fmt.Sprintf("publisherCentral: [%s]", line))
				err := services.Get().Write(ctx, info.w, line)
				if err != nil {
					log.Warn(ctx, "publisherCentral.remove", err)
				}
			}
		}

		log.Info(ctx, fmt.Sprintf("publisherCentral.finish: %s", uid))
	}
}

func makeKey(consInfo *consumerInfo) string {
	return fmt.Sprintf("%s_%s", consInfo.ID, consInfo.Ch)
}

func consumerStep(ctx context.Context, consInfo *consumerInfo) error {
	log.Info(ctx, "consumerStep")
	consInfo.Conn.Write([]byte("ok\n"))

	_, ok := consumerMap.Load(makeKey(consInfo))
	if !ok {
		consumerMap.Store(makeKey(consInfo), consInfo)
	}
	mapRelation[makeKey(consInfo)] = true
	for {
		l, err := services.Get().ReadLine(ctx, consInfo.r)
		log.Info(ctx, fmt.Sprintf("consumerStep.readLine: [%s]", l))
		if err != nil {
			return err
		}
		err = services.Get().Write(ctx, consInfo.Conn, l)
		if err != nil {
			return err
		}
		consumerRes, err := services.Get().ReadLine(ctx, consInfo.Conn)
		if err != nil {
			return err
		}
		if string(consumerRes) != "ok" {
			return fmt.Errorf("error NOK")
		}
		consInfo.m.Unlock()
	}
}
