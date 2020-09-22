package domain

import (
	"context"
	"fmt"
	"github.com/zeusmq/internal/services"
	"net"
	"time"
)

type PublishMessageRequest struct {
	Msg          string
	Topic        string
	Offset       uint64
}

type PublishMessageResponse struct {
	Consumers    []string
	Offset       uint64
}

func PublishMessage(ctx context.Context, req PublishMessageRequest)(*PublishMessageResponse, error){
	consumers, err := services.Get().GetConsumers(ctx)
	if err != nil {
		return nil, err
	}


	msg := []byte(fmt.Sprintf("%s;%s", req.Topic, req.Msg))
	chNames := make(chan string)
	mapNames := make(map[string]bool)

	ctxTo, _ := context.WithTimeout(ctx, time.Second * 3)

	for _, consumer := range consumers {
		for _, conn := range consumer.Channels {
			go func(consumer services.ConsumerResponse, c net.Conn){
				if !consumer.HasTopic(req.Topic){
					chNames <- consumer.Name
					return
				}
				err := services.Get().Write(ctx, c, msg)
				if err != nil{
					return
				}
				line := services.Get().ReadLine(ctx, c)
				if line == "ok"{
					chNames <- consumer.Name
				}
				return
			}(consumer, conn)
		}
	}

	L: for {
		select {
		case name := <- chNames:
			mapNames[name] = true
			err = services.Get().SaveConsumerState(ctx, services.ConsumerStateRequest{
				Name:   name,
				Offset: req.Offset,
			})
			if err != nil {
				mapNames[name] = false
			}
			if len(mapNames) == len(consumers){
				break L
			}
		case <- ctxTo.Done():
			break L
		}
	}

	successConsumers := make([]string, 0)
	for k,v := range mapNames {
		if v == true {
			successConsumers = append(successConsumers, k)
		}
	}
	return &PublishMessageResponse{
		Consumers: successConsumers,
		Offset:    req.Offset,
	}, nil

}