package connection

import (
	"bufio"
	"context"
	"net"
)

func ReadLine(ctx context.Context, c net.Conn)string{
	scanner := bufio.NewScanner(c)
	scanner.Split(bufio.ScanLines)
	return scanner.Text()
}
