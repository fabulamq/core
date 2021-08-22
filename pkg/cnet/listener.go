package cnet

import (
	"net"
)

var chConn chan net.Conn

func init() {
	chConn = make(chan net.Conn)
}

type Listener interface {
	Accept() (net.Conn, error)
}

type TcpListener struct {
	l net.Listener
}

func (fl TcpListener) Accept()(net.Conn, error) {
	conn, err := fl.l.Accept()
	if err != nil {
		return nil, err
	}
	return conn, nil
}

type FakeListener struct {

}

func (fl FakeListener) Accept()(net.Conn, error) {
	serverConn := <- chConn
	return serverConn, nil
}

type ListenerConfig struct {
	Host   string
}

func Listen(c ListenerConfig)(Listener, error){
	if c.Host == "-" {
		return FakeListener{}, nil
	}
	listener, err := net.Listen("tcp", c.Host)
	if err != nil {
		return nil, err
	}
	return TcpListener{l: listener}, nil
}

func Dial(host string)(net.Conn, error){
	if host == "-" {
		client, server := net.Pipe()
		chConn <- server
		return client, nil
	}
	return net.Dial("tcp", host)
}