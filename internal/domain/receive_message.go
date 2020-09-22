package domain

import (
	"context"
	"github.com/zeusmq/internal/services"
	"net"
)

type ReceiveNewMessageRequest struct {
	Msg          string
	Topic        string
	producerConn net.Conn
}


func ReceiveNewMessage(ctx context.Context, req ReceiveNewMessageRequest)error{
	res, err := services.Get().SaveMsgToQueue(ctx, services.SaveMsgRequest{
		Msg:    req.Msg,
		Topics: req.Topic,
	})
	if err != nil {
		return err
	}

	if err := services.Get().Ack(ctx, req.producerConn); err != nil {
		return err
	}

	if err := services.Get().PublishStep(ctx, res); err != nil {
		return err
	}
	return nil
}
