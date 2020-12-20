package api

import "github.com/fabulamq/core/internal/entity"

type hostInfo struct {
	id       string
	host     string
	weight   int
	mark     *entity.Mark
	kind     entity.PublisherKind
	isFundHq bool
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
	// get the top Mark
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
