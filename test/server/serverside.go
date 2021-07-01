package main

import (
	"flag"
	"net/http"
	_ "net/http/pprof"

	"github.com/Clivebi/rplib"
)

var serverAddress = flag.String("s", "", "the addess(with port eg:192.168.1.100:9000) accept client connection")
var routeAddress = flag.String("r", "", "the addess(with port eg:192.168.1.100:9001) accept route server connection")
var readTimeout = flag.Int("t", 30, "the connection read timeout second,default 30s")
var clientExpireTimeout = flag.Int("e", 16*60, "the connection max idle time in second,default 960s")
var multiMode = flag.Bool("m", false, "multi route server mode (client need send route server ip address before any normal traffic)")
var help = flag.Bool("h", false, "show help information")

func main() {
	go http.ListenAndServe("127.0.0.1:9100", nil)
	flag.Parse()
	if *help {
		flag.Usage()
		return
	}
	if len(*serverAddress) == 0 || len(*routeAddress) == 0 {
		flag.Usage()
		return
	}
	route := rplib.Route{}
	route.ServerAddress = *serverAddress
	route.RouteAddress = *routeAddress
	route.ClientExpireTimeoutSecond = *clientExpireTimeout
	route.ReadTimeoutSecond = *readTimeout
	route.ServerExpireTimeoutSecond = 8 * 60
	if *multiMode {
		route.Hijack(&rplib.ProtocolSelectorHijack{})
	}
	route.Run()
}
