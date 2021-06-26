package rplib_test

import (
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/Clivebi/rplib"
)

const (
	req = "GET / HTTP/1.1\r\nHost: www.baidu.com\r\nUser-Agent: Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.114 Safari/537.36\r\nAccept: text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9\r\nAccept-Language: zh-CN,zh;q=0.9,en;q=0.8\r\nConnection: close\r\n\r\n"
)

func TestWithIPAddressHeader(t *testing.T) {
	route := rplib.Route{}
	route.ServerAddress = "localhost:9000"
	route.RouteAddress = "localhost:9001"
	route.ClientExpireTimeoutSecond = 60 * 2
	route.ReadTimeoutSecond = 30
	route.ServerExpireTimeoutSecond = 8 * 60
	route.Hijack(&rplib.IPWithAddressHeaderHijack{})
	go route.Run()
	defer route.Close()
	time.Sleep(time.Second * 3) //wait route serve runing
	// route request to www.baidu.com:80
	ap, err := rplib.NewAP("localhost:9001", "www.baidu.com:80", time.Minute*4)
	if err != nil {
		t.Error(err)
		return
	}
	defer ap.Close()
	go ap.Run()

	ip := net.ParseIP("127.0.0.1").To4()
	con, err := net.Dial("tcp", "localhost:9000")
	if err != nil {
		t.Error(err)
		return
	}
	//write destIP (use witch ap(access point))
	con.Write([]byte(ip))
	con.Write([]byte(req))
	buf := make([]byte, 4096)
	con.Read(buf)
	fmt.Println(string(buf))
}

func TestDirect(t *testing.T) {
	route := rplib.Route{}
	route.ServerAddress = "localhost:9000"
	route.RouteAddress = "localhost:9001"
	route.ClientExpireTimeoutSecond = 60 * 2
	route.ReadTimeoutSecond = 30
	route.ServerExpireTimeoutSecond = 8 * 60
	go route.Run()
	defer route.Close()
	time.Sleep(time.Second * 3) //wait route serve runing
	// route request to www.baidu.com:80
	ap, err := rplib.NewAP("localhost:9001", "www.baidu.com:80", time.Minute*4)
	if err != nil {
		t.Error(err)
		return
	}
	defer ap.Close()
	go ap.Run()

	con, err := net.Dial("tcp", "localhost:9000")
	if err != nil {
		t.Error(err)
		return
	}
	con.Write([]byte(req))
	buf := make([]byte, 4096)
	con.Read(buf)
	fmt.Println(string(buf))
}

func TestWithPolicy(t *testing.T) {
	route := rplib.Route{}
	route.ServerAddress = "localhost:9000"
	route.RouteAddress = "localhost:9001"
	route.ClientExpireTimeoutSecond = 60 * 2
	route.ReadTimeoutSecond = 30
	route.ServerExpireTimeoutSecond = 8 * 60

	//add route policy
	policy := &rplib.IPWithPolicyHijack{}
	_, im, _ := net.ParseCIDR("127.0.0.1/32")
	policy.AddPolicy(im, "127.0.0.1")
	route.Hijack(policy)

	go route.Run()
	defer route.Close()
	time.Sleep(time.Second * 3) //wait route serve runing
	// route request to www.baidu.com:80
	ap, err := rplib.NewAP("localhost:9001", "www.baidu.com:80", time.Minute*4)
	if err != nil {
		t.Error(err)
		return
	}
	defer ap.Close()
	go ap.Run()

	con, err := net.Dial("tcp", "localhost:9000")
	if err != nil {
		t.Error(err)
		return
	}
	con.Write([]byte(req))
	buf := make([]byte, 4096)
	con.Read(buf)
	fmt.Println(string(buf))
}
