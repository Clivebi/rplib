package rplib

import (
	"errors"
	"log"
	"net"
	"sync"
	"time"
)

//ProtocolSelectorHijack implement ip address(IPv4) header protocol
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
type ProtocolSelectorHijack struct {
}

func (h *ProtocolSelectorHijack) SelectRouteConnection(o *Route, con net.Conn) (*APConnection, error) {
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

func (h *ProtocolSelectorHijack) GetRouteConnectionKey(o *Route, con net.Conn) (string, error) {
	key, _, _ := net.SplitHostPort(con.RemoteAddr().String())
	return key, nil
}

type routeItem struct {
	src *net.IPNet
	dst string
}

//PolicySelectorHijack policy implement
//implement ip route policy
type PolicySelectorHijack struct {
	routeItemsLock sync.RWMutex
	routeItems     []*routeItem
}

//AddPolicy add policy
//when the src contain the client ip address,use the dst address route server
func (h *PolicySelectorHijack) AddPolicy(src *net.IPNet, dst string) {
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

func (h *PolicySelectorHijack) remove(src *net.IPNet) {
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

func (h *PolicySelectorHijack) RemovePolicy(src *net.IPNet) {
	h.routeItemsLock.Lock()
	defer h.routeItemsLock.Unlock()
	if h.routeItems == nil {
		return
	}
	h.remove(src)
}

func (h *PolicySelectorHijack) LookupDestAddress(ip net.IP) (string, error) {
	h.routeItemsLock.Lock()
	defer h.routeItemsLock.Unlock()
	for _, v := range h.routeItems {
		if v.src.Contains(ip) {
			return v.dst, nil
		}
	}
	return "", errors.New("policy not found for " + ip.String())
}

func (h *PolicySelectorHijack) SelectRouteConnection(o *Route, con net.Conn) (*APConnection, error) {
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

func (h *PolicySelectorHijack) GetRouteConnectionKey(o *Route, con net.Conn) (string, error) {
	key, _, _ := net.SplitHostPort(con.RemoteAddr().String())
	return key, nil
}

//Authenticator Authenticator interface
type Authenticator interface {
	//DoAuthenticate verify username - password & pair
	DoAuthenticate(user string, key string, ip string) error
}

//AuthenticateHijack provider base user-key authenticate implement
//protocol
// namesize(1byte)+name(namesize)+keysize(1byte)+key(keysize)
type AuthenticateHijack struct {
	//Auth the Authenticate implement i
	Auth Authenticator
}

func (h *AuthenticateHijack) SelectRouteConnection(o *Route, con net.Conn) (*APConnection, error) {
	var user, key string
	size := make([]byte, 1)
	buf := make([]byte, 255)
	con.SetReadDeadline(time.Now().Add(time.Duration(o.ReadTimeoutSecond) * time.Second))
	_, err := con.Read(size)
	if err != nil {
		return nil, err
	}
	con.SetReadDeadline(time.Now().Add(time.Duration(o.ReadTimeoutSecond) * time.Second))
	_, err = con.Read(buf[:int(size[0])])
	if err != nil {
		return nil, err
	}
	user = string(buf[:int(size[0])])
	_, err = con.Read(size)
	if err != nil {
		return nil, err
	}
	con.SetReadDeadline(time.Now().Add(time.Duration(o.ReadTimeoutSecond) * time.Second))
	_, err = con.Read(buf[:int(size[0])])
	if err != nil {
		return nil, err
	}
	key = string(buf[:int(size[0])])
	con.SetReadDeadline(time.Now().Add(time.Duration(o.ClientExpireTimeoutSecond) * time.Second))
	if h.Auth != nil {
		err = h.Auth.DoAuthenticate(user, key, con.RemoteAddr().String())
		if err != nil {
			return nil, err
		}
	}
	rc := o.PickupMinimumStreamConnection()
	if rc == nil {
		return nil, errors.New("no routeconnection avaliable")
	}
	return rc, nil
}

func (h *AuthenticateHijack) GetRouteConnectionKey(o *Route, con net.Conn) (string, error) {
	return con.RemoteAddr().String(), nil
}
