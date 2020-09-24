package connection

import (
	"bufio"
	"context"
	"fmt"
	"io"
)

func ReadLine(ctx context.Context, c io.Reader) ([]byte, error) {
	scanner := bufio.NewScanner(c)
	scanner.Split(bufio.ScanLines)
	if scanner.Scan() {
		return scanner.Bytes(), nil
	}
	return nil, fmt.Errorf("connection closed")
}

func WriteLine(ctx context.Context, c io.Writer, msg []byte) error {
	_, err := c.Write(append(msg, []byte("\n")...))
	return err
}
