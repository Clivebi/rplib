package main

import (
	"flag"
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
	rplib.RunRouteServer(*routeAddress, *backendAddress, time.Minute*4)
}
