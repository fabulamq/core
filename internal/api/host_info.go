package api

type hostInfo struct {
	id       string
	host     string
	weight   int
	mark     *mark
	kind     publisherKind
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
		if hqHostInfo.kind == FundHeadQuarter {
			myInfo := hinfos.getMyself()
			if myInfo.mark.isBefore(*hqHostInfo.mark){
				return &hqHostInfo
			}
			if myInfo.weight < hqHostInfo.weight{
				return &hqHostInfo
			}
			return myInfo
		}
		if hqHostInfo.kind == HeadQuarter {
			return &hqHostInfo
		}
	}

	topMark := new(mark)
	// get the top mark
	for _, hostInfo := range hinfos {
		if topMark.isBefore(*hostInfo.mark) {
			topMark = hostInfo.mark
		}
	}
	selectedHost := hostInfo{}
	for _, hostInfo := range hinfos {
		if !hostInfo.mark.isEqual(*topMark) {
			continue
		}
		if selectedHost.weight > hostInfo.weight {
			continue
		}
		selectedHost = hostInfo
	}
	return &selectedHost
}
