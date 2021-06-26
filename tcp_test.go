package rplib

import (
	"fmt"
	"net"
	"testing"
)

const (
	req = "GET / HTTP/1.1\r\nHost: www.baidu.com\r\nUser-Agent: Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.114 Safari/537.36\r\nAccept: text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9\r\nAccept-Language: zh-CN,zh;q=0.9,en;q=0.8\r\nConnection: close\r\n\r\n"
)

func TestRP(t *testing.T) {
	ip := net.ParseIP("127.0.0.1").To4()
	con, err := net.Dial("tcp", "localhost:9000")
	if err != nil {
		t.Error(err)
		return
	}
	con.Write([]byte(ip))
	con.Write([]byte(req))
	buf := make([]byte, 4096)
	con.Read(buf)
	fmt.Println(string(buf))
}
