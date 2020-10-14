package main

import (
	"fmt"
	"io"
	"time"
)

func main() {
	r, w := io.Pipe()
	go func() {
		for {
			time.Sleep(2 * time.Second)
			w.Write([]byte("hello"))
			fmt.Println("writing")
		}

	}()


	go func() {
		k := make([]byte, 10)
		r.Read(k)
		fmt.Println(string(k))
	}()

	time.Sleep(10 * time.Second)

}
