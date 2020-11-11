package main

import (
	"flag"
	"fmt"
	"github.com/fabulamq/core/internal/api"
	"strings"
)

func main(){
	hostsStr := flag.String("hosts", "", "hosts of application")
	path := flag.String("path", "", "folder")
	port := flag.String("port", "9998", "port of application")
	perPage := flag.Uint64("perPage", 100000, "Offset per page")
	weight := flag.Int("weight", 100, "Priority rules")
	flag.Parse()

	hosts := make([]string, 0)
	for _, host := range strings.Split(*hostsStr, ","){
		if host == ""{
			continue
		}
		hosts = append(hosts, host)
	}


	_, status := api.Start(api.Config{
		Folder: *path,
		Port:  *port,
		Hosts: hosts,
		OffsetPerChapter: *perPage,
		Weight: *weight,
	})
	L: for {
		select{
		case s := <- status:
			if s.IsReady {
				fmt.Println(fmt.Sprintf("Ready for reading your book at port %s", *port))
			}
			if s.Err != nil {
				break L
			}
		}
	}
}
