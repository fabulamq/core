package localdb

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"strings"
)

type Config struct {
	ID      string
	Uri     string
	Handler func(s string) error
}

type localDb struct {
	f      func(s string) error
	r      io.Reader
	OConn  net.Conn
	Master net.Conn
}

var ldb *localDb

//func performScan(){
//	scanner := bufio.NewScanner(ldb.r)
//	scanner.Split(bufio.ScanLines)
//	for scanner.Scan(){
//		txt := scanner.Text()
//	}
//}

type message struct {
	Msg  interface{}
	Kind string
}

func (l localDb) Publish(msg interface{}) error {
	m := message{
		Msg:  msg,
		Kind: "message",
	}
	b, _ := json.Marshal(m)
	l.r.Read(b)
	l.r.Read([]byte("\n"))
}

func Get() *localDb {
	return ldb
}

func Start(c Config) error {
	if ldb != nil {
		return nil
	}
	conn, err := net.Dial("tcp", c.Uri)
	if err != nil {
		return err
	}

	// write first message
	conn.Write([]byte(""))

	ldb.c = conn
	ldb.f = c.Handler
	ldb.r = strings.NewReader("")

	return listen()
}

func listen() error {
	listener, err := net.Listen("tcp", "localhost:9999")
	if err != nil {
		return err
	}
	for {
		conn, err := listener.Accept()
		if err != nil {
			return err
		}
		go func() {
			scanner := bufio.NewScanner(conn)
			scanner.Split(bufio.ScanLines)
			scanner.Scan()
			firstMsg := scanner.Text()
			fmt.Println(firstMsg)
		}()

	}

}
