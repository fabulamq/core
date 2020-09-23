package connection

import (
	"bufio"
	"context"
	"fmt"
	"net"
)

func ReadLine(ctx context.Context, c net.Conn) (string, error) {
	scanner := bufio.NewScanner(c)
	scanner.Split(bufio.ScanLines)
	if scanner.Scan() {
		return scanner.Text(), nil
	}
	return "", fmt.Errorf("connection closed")
}
