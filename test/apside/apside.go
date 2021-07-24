package main

import (
	"flag"
	"log"
	"net/http"
	_ "net/http/pprof"
	"time"

	"github.com/Clivebi/rplib"
)

var routeAddress = flag.String("r", "", "route server address")
var backendAddress = flag.String("b", "", "the backend server address that runing behind NAT")
var help = flag.Bool("h", false, "show help information")

func main() {
	flag.Parse()
	go http.ListenAndServe("127.0.0.1:9101", nil)
	if *help || len(*routeAddress) == 0 || len(*backendAddress) == 0 {
		flag.Usage()
		return
	}
	tq := rplib.NewTaskQueue(32)
	for {
		log.Println("try connect to " + *routeAddress)
		ap, err := rplib.NewAP(*routeAddress, *backendAddress, time.Minute*2, tq)
		if err != nil {
			time.Sleep(time.Second * 30)
			continue
		}
		log.Println("connect " + *routeAddress + " success")
		ap.Run()
		ap.Close()
	}
}
