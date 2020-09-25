package domain

import (
	"context"
	"fmt"
	"github.com/zeusmq/internal/infra/log"
	"github.com/zeusmq/internal/services"
	"net"
	"strings"
	"sync"
)

type subscriberInfo struct {
	ID       string
	Ch       string
	Kind     string
	Topic    string
	Strategy string
}

type NewMessage struct {
	Topic   string
	Message string
}

type consumerInfo struct {
	ID       string
	Ch       string
	Topic    string
	Conn     net.Conn
	Ctx      context.Context
	Strategy string
}

// reordenar
// unlock no lugar certo
// cid??

var once = sync.Once{}
var publishLocker = sync.Mutex{}
var consumerMap = sync.Map{}

func AddConnection(conn net.Conn) {
	//once.Do(func() {
	//	msgChan = make(chan []byte)
	//})

	go func() {
		ctx := context.Background()

		line, err := services.Get().ReadLine(ctx, conn)
		if err != nil {
			return
		}
		lineSpl := strings.Split(string(line), ";")
		subInfo := subscriberInfo{
			Kind:     lineSpl[0],
			ID:       lineSpl[1],
			Ch:       lineSpl[2],
			Topic:    lineSpl[3],
			Strategy: lineSpl[4],
		}
		ctxWithId := context.WithValue(context.Background(), "id", subInfo.ID)
		ctx = context.WithValue(ctxWithId, "ch", subInfo.Ch)

		switch subInfo.Kind {
		case "c":
			c := &consumerInfo{
				ID:       subInfo.ID,
				Ch:       subInfo.Ch,
				Topic:    subInfo.Topic,
				Strategy: subInfo.Strategy,

				Ctx:  ctx,
				Conn: conn,
			}
			err = consumerStep(ctx, c)
			consumerMap.Delete(makeKey(c))
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
		err = services.Get().WriteFile(ctx, producerMsg)
		if err != nil {
			return err
		}

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

	tail, err := services.Get().TailFile()
	if err != nil {
		return err
	}

	for {
		line := <-tail
		l := []byte(fmt.Sprintf("%s", line.Text))
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
		log.Info(ctx, fmt.Sprintf("consumerStep.readLine.complete: [%s]", l))
	}
}
