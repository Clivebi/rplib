package rplib

import (
	"errors"
	"log"
	"net"
	"sync"
	"time"
)

//IPWithAddressHeaderHijack implement ip address(IPv4) header protocol
//the client must send the route server address before normal traffic
//example:
//
//	ip := net.ParseIP("127.0.0.1").To4()
//	con, err := net.Dial("tcp", "localhost:9000")
//	if err != nil {
//		t.Error(err)
//		return
//	}
//	con.Write([]byte(ip))  //write the route server ip address
//	con.Write([]byte(req)) //write the normal data
type IPWithAddressHeaderHijack struct {
}

func (h *IPWithAddressHeaderHijack) SelectRouteConnection(o *Route, con net.Conn) (*APConnection, error) {
	con.SetReadDeadline(time.Now().Add(time.Duration(o.ReadTimeoutSecond) * time.Second))
	ipV4 := make([]byte, 4)
	_, err := con.Read(ipV4)
	if err != nil {
		log.Println("read ip address failed", err)
		return nil, err
	}
	con.SetReadDeadline(time.Now().Add(time.Duration(o.ClientExpireTimeoutSecond) * time.Second))
	key := ((net.IP)(ipV4)).String()
	rc := o.LookupConnection(key)
	if rc == nil {
		log.Println("no routeconnection avaliable for key:", key)
		return nil, errors.New("no routeconnection avaliable for key:" + key)
	}
	return rc, nil
}

func (h *IPWithAddressHeaderHijack) GetRouteConnectionKey(o *Route, con net.Conn) (string, error) {
	key, _, _ := net.SplitHostPort(con.RemoteAddr().String())
	return key, nil
}

type routeItem struct {
	src *net.IPNet
	dst string
}

//IPWithPolicyHijack policy implement
//implement ip route policy
type IPWithPolicyHijack struct {
	routeItemsLock sync.RWMutex
	routeItems     []*routeItem
}

//AddPolicy add policy
//when the src contain the client ip address,use the dst address route server
func (h *IPWithPolicyHijack) AddPolicy(src *net.IPNet, dst string) {
	item := &routeItem{src: src, dst: dst}
	h.routeItemsLock.Lock()
	defer h.routeItemsLock.Unlock()
	if h.routeItems == nil {
		h.routeItems = []*routeItem{item}
	} else {
		h.remove(src)
		h.routeItems = append(h.routeItems, item)
	}
}

func (h *IPWithPolicyHijack) remove(src *net.IPNet) {
	news := make([]*routeItem, len(h.routeItems))
	i := 0
	for _, v := range h.routeItems {
		if v.src.String() == src.String() {
			continue
		}
		news[i] = v
		i++
	}
}

func (h *IPWithPolicyHijack) RemovePolicy(src *net.IPNet) {
	h.routeItemsLock.Lock()
	defer h.routeItemsLock.Unlock()
	if h.routeItems == nil {
		return
	}
	h.remove(src)
}

func (h *IPWithPolicyHijack) LookupDestAddress(ip net.IP) (string, error) {
	h.routeItemsLock.Lock()
	defer h.routeItemsLock.Unlock()
	for _, v := range h.routeItems {
		if v.src.Contains(ip) {
			return v.dst, nil
		}
	}
	return "", errors.New("policy not found for " + ip.String())
}

func (h *IPWithPolicyHijack) SelectRouteConnection(o *Route, con net.Conn) (*APConnection, error) {
	srcAddress, _, _ := net.SplitHostPort(con.RemoteAddr().String())
	address := net.ParseIP(srcAddress)
	key, err := h.LookupDestAddress(address)
	if err != nil {
		return nil, err
	}
	rc := o.LookupConnection(key)
	if rc == nil {
		log.Println("no routeconnection avaliable for key:", key)
		return nil, errors.New("no routeconnection avaliable for key:" + key)
	}
	return rc, nil
}

func (h *IPWithPolicyHijack) GetRouteConnectionKey(o *Route, con net.Conn) (string, error) {
	key, _, _ := net.SplitHostPort(con.RemoteAddr().String())
	return key, nil
}
