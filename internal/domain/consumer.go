package domain

import "net"

type Consumer struct {
	ID   string
	Ch   string
	Conn net.Conn
}

var consumerChan chan Consumer
