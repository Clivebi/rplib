package rplib

import (
	"errors"
	"io"
	"log"
	"net"
	"sync"
	"time"
)

type clientContext struct {
	addr       string
	conn       net.Conn
	activeTime time.Time
	sendBytes  int64
	recvBytes  int64
	sendSpeed  int64
	recvSpeed  int64
}

type BasicRouteHijack interface {
	//SelectRouteConnection
	//called when new client connection accept
	SelectRouteConnection(*BasicRoute, net.Conn) (*RouteConnection, error)
	//GetRouteConnectionKey
	//called when new route server connection accept
	GetRouteConnectionKey(*BasicRoute, net.Conn) (string, error)
}

type BasicRoute struct {
	//ServerAddress the address accept client connection
	ServerAddress string
	//RouteAddress the address accept RouteServer connection
	RouteAddress string
	//ReadTimeoutSecond
	//the first packet read time out maybe a auth packet
	ReadTimeoutSecond int
	//ClientExpireTimeoutSecond
	//the client connection expire timeout,
	//if in this timeout not have any traffic,close the connection
	ClientExpireTimeoutSecond int
	//ServerExpireTimeoutSecond
	//the route connection expire timeout,
	//if in this timeout not have any traffic,close the connection
	ServerExpireTimeoutSecond int
	route                     *Route
	clientsLock               sync.RWMutex
	clients                   map[string]*clientContext
	hijack                    BasicRouteHijack
}

func (o *BasicRoute) Hijack(h BasicRouteHijack) *BasicRoute {
	o.hijack = h
	return o
}

func (o *BasicRoute) expireTimeoutBackgroundThread() {
	for {
		time.Sleep(time.Duration(o.ClientExpireTimeoutSecond/2) * time.Second)
		now := time.Now()
		o.clientsLock.Lock()
		for k, v := range o.clients {
			if now.After(v.activeTime) && now.Sub(v.activeTime) > time.Duration(o.ClientExpireTimeoutSecond)*time.Second {
				v.conn.Close()
				delete(o.clients, k)
			}
			v.sendSpeed = 0
			v.recvSpeed = 0
		}
		o.clientsLock.Unlock()
	}
}

func (o *BasicRoute) addClientConnection(ctx *clientContext) {
	o.clientsLock.Lock()
	defer o.clientsLock.Unlock()
	ctx.addr = ctx.conn.RemoteAddr().String()
	o.clients[ctx.addr] = ctx
}

func (o *BasicRoute) removeClientConnection(ctx *clientContext) {
	o.clientsLock.Lock()
	defer o.clientsLock.Unlock()
	delete(o.clients, ctx.addr)
}

func (o *BasicRoute) copyIO(dst io.Writer, src io.Reader, ctx *clientContext, send bool) {
	buf := make([]byte, 1024*32)
	for {
		n, err := src.Read(buf)
		if err != nil {
			break
		}
		_, err = dst.Write(buf[:n])
		if err != nil {
			break
		}
		if send {
			ctx.sendSpeed += int64(n)
			ctx.sendBytes += int64(n)
		} else {
			ctx.recvSpeed += int64(n)
			ctx.recvBytes += int64(n)
		}
		ctx.activeTime = time.Now()
	}
}

func (o *BasicRoute) serve(addr string, handler func(net.Conn)) error {
	ls, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	defer ls.Close()
	for {
		con, err := ls.Accept()
		if err != nil {
			return err
		}
		go handler(con)
	}
}

func (o *BasicRoute) selectRouteConnection(con net.Conn) (*RouteConnection, error) {
	if o.hijack != nil {
		return o.hijack.SelectRouteConnection(o, con)
	}
	rc := o.route.pickupConnection()
	if rc == nil {
		return nil, errors.New("not routeconnection avaliable")
	}
	return rc, nil
}

func (o *BasicRoute) getRouteConnectionKey(con net.Conn) (string, error) {
	if o.hijack != nil {
		return o.hijack.GetRouteConnectionKey(o, con)
	}
	return con.RemoteAddr().String(), nil
}

func (o *BasicRoute) routeConnectionHandler(con net.Conn) {
	key, err := o.getRouteConnectionKey(con)
	if err != nil {
		log.Println("get routeconnection key failed:", err)
		con.Close()
		return
	}
	o.route.serverNewConnection(con, time.Duration(o.ServerExpireTimeoutSecond)*time.Second, key)
}

func (o *BasicRoute) serverConnectionHandler(con net.Conn) {
	defer con.Close()
	rc, err := o.selectRouteConnection(con)
	if err != nil {
		log.Println("select routeconnect failed:", err)
		return
	}
	rs, err := rc.AllocateStream(time.Minute)
	if err != nil {
		log.Println("allocate stream failed:", err)
		return
	}
	defer rs.Close()
	ctx := &clientContext{activeTime: time.Now()}
	ctx.conn = con
	o.addClientConnection(ctx)
	go o.copyIO(con, rs, ctx, false)
	o.copyIO(rs, con, ctx, true)
	o.removeClientConnection(ctx)
}

func (o *BasicRoute) StartRoute() error {
	o.route = NewRoute()
	o.clientsLock = sync.RWMutex{}
	o.clients = map[string]*clientContext{}
	go o.expireTimeoutBackgroundThread()
	errorCh := make(chan error)
	go func() {
		errorCh <- o.serve(o.ServerAddress, o.serverConnectionHandler)
	}()
	go func() {
		errorCh <- o.serve(o.RouteAddress, o.routeConnectionHandler)
	}()
	err := <-errorCh
	close(errorCh)
	return err
}
