package domain

import "net"

type SaveMessageRequest struct {
	Msg          string
	producerConn net.Conn
}
