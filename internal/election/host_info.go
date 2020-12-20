package election

import (
	"bufio"
	"fmt"
	"github.com/fabulamq/core/internal/api"
	"github.com/fabulamq/core/internal/entity"
	log "github.com/sirupsen/logrus"
	"net"
	"strconv"
	"strings"
	"time"
)

type hostInfo struct {
	id       string
	host     string
	weight   int
	mark     *entity.Mark
	kind     entity.PublisherKind
	isFundHq bool
}

func getHostsInfo(c api.Config, m *entity.Mark) hostsInfo {
	hostsInfo := make(hostsInfo, 0)

	hostsInfo["local"] = hostInfo{
		id:     c.ID,
		host:   "local",
		weight: c.Weight,
		mark:   m,
		kind:   entity.Undefined,
	}

	for _, host := range c.Hosts {
		log.Info(fmt.Sprintf("(%s) dialing to host: %s", c.ID, host))
		conn, err := net.DialTimeout("tcp", host, time.Millisecond * 200)
		if err != nil {
			continue
		}
		_, err = conn.Write([]byte("info\n"))
		if err != nil {
			continue
		}
		scanner := bufio.NewScanner(conn)
		scanner.Split(bufio.ScanLines)

		if scanner.Scan() {
			txtSpl := strings.Split(scanner.Text(), ";")

			id := txtSpl[1]
			weight, err := strconv.Atoi(txtSpl[2])
			if err != nil {
				continue
			}
			chapter, err := strconv.ParseUint(txtSpl[3], 10, 64)
			if err != nil {
				continue
			}
			line, err := strconv.ParseUint(txtSpl[4], 10, 64)
			if err != nil {
				continue
			}
			hostsInfo[host] = hostInfo{
				id:     id,
				kind:   entity.GetPublisherKind(txtSpl[5]),
				host:   host,
				weight: weight,
				mark:   entity.NewMark(chapter, line),
			}
		}
	}
	return hostsInfo
}

type hostsInfo map[string]hostInfo

func (hinfos hostsInfo) getMyself() *hostInfo {
	for _, hostInfo := range hinfos {
		if hostInfo.host == "local"{
			return &hostInfo
		}
	}
	return nil
}

func (hinfos hostsInfo) getTheOne() *hostInfo {

	// find one that is HQ or in fund status
	for _, hqHostInfo := range hinfos {
		if hqHostInfo.kind == entity.FundHeadQuarter {
			myInfo := hinfos.getMyself()
			if myInfo.mark.IsBefore(*hqHostInfo.mark){
				return &hqHostInfo
			}
			if myInfo.weight < hqHostInfo.weight{
				return &hqHostInfo
			}
			return myInfo
		}
		if hqHostInfo.kind == entity.HeadQuarter {
			return &hqHostInfo
		}
	}

	topMark := new(entity.Mark)
	// get the top mark
	for _, hostInfo := range hinfos {
		if topMark.IsBefore(*hostInfo.mark) {
			topMark = hostInfo.mark
		}
	}
	selectedHost := hostInfo{}
	for _, hostInfo := range hinfos {
		if !hostInfo.mark.IsEqual(*topMark) {
			continue
		}
		if selectedHost.weight > hostInfo.weight {
			continue
		}
		selectedHost = hostInfo
	}
	return &selectedHost
}
