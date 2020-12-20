package election

import (
	"fmt"
	"github.com/fabulamq/core/internal/api"
	"github.com/fabulamq/core/internal/entity"
	log "github.com/sirupsen/logrus"
	"net"
)

type Status struct {
	Err            error
	Kind           entity.PublisherKind
	FinishElection bool
}

func Start(c api.Config, m *entity.Mark) chan Status{
	chStatus := make(chan Status)

	var listener net.Listener
	go func() {
		defaultPort := "9998"
		if c.Port != "" {
			defaultPort = c.Port
		}
		var err error
		listener, err = net.Listen("tcp", fmt.Sprintf("localhost:%s", defaultPort))
		if err != nil {
			chStatus <- Status{Err: err}
		}
		for {
			conn, err := listener.Accept()
			if err != nil {
				break
			}
			log.Info(fmt.Sprintf("(%s) sending info", c.ID))
			_, _ = conn.Write([]byte(fmt.Sprintf("%s;%s;%d;%d;%d;%s\n",
				entity.Undefined,
				c.ID,
				c.Weight,
				m.GetChapter(),
				m.GetLine(),
				entity.Undefined,
			)))
		}
		chStatus <- Status{
			FinishElection: true,
		}
	}()

	go func() {
		for {
			totalInstances := len(c.Hosts) + 1

			hostsInfo := getHostsInfo(c, m)

			quo := float32(len(hostsInfo)) / float32(totalInstances)
			if quo <= 0.5 {
				log.Info(fmt.Sprintf("(%s) cannot elect by quo", c.ID))
				continue
			}
			// check map status
			hostInfo := hostsInfo.getTheOne()
			if hostInfo.host == "local" {
				chStatus <- Status{
					Err:  nil,
					Kind: entity.HeadQuarter,
				}
			}else {
				chStatus <- Status{
					Err:  nil,
					Kind: entity.Branch,
				}
			}
			break
		}
		_ = listener.Close()
	}()

	return chStatus
}