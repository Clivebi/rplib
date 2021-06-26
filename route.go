package rplib

import (
	"bytes"
	"crypto/md5"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"io"
	"log"
	"net"
	"strconv"
	"sync"
	"time"
)

type RouteStream struct {
	isClosed      bool
	closed        chan int
	id            uint16
	commandWriter CommandWriter
	lock          sync.RWMutex
	cache         *bytes.Buffer
	w             chan []byte
}

func NewRouteStream(id uint16, commandWriter CommandWriter) *RouteStream {
	return &RouteStream{
		isClosed:      false,
		closed:        make(chan int),
		id:            id,
		commandWriter: commandWriter,
		lock:          sync.RWMutex{},
		cache:         bytes.NewBuffer(nil),
		w:             make(chan []byte, 64),
	}
}

func (o *RouteStream) readFromCache(b []byte) (int, error) {
	o.lock.Lock()
	defer o.lock.Unlock()
	if o.cache.Len() > 0 {
		return o.cache.Read(b)
	}
	return 0, nil
}

func (o *RouteStream) readToCache() {
	o.lock.Lock()
	defer o.lock.Unlock()
	if len(o.w) > 0 {
		buf := <-o.w
		o.cache.Write(buf)
	}
}

func (o *RouteStream) Read(b []byte) (int, error) {
	n, err := o.readFromCache(b)
	if n > 0 {
		o.readToCache()
		return n, err
	}
	select {
	case buf := <-o.w:
		o.lock.Lock()
		o.cache.Write(buf)
		o.lock.Unlock()
		return o.readFromCache(b)
	case <-o.closed:
		return 0, io.EOF
	}
}

func (o *RouteStream) Write(b []byte) (int, error) {
	cmd := DataCommand(b, o.id)
	o.commandWriter.AsyncWriteCommand(cmd)
	return len(b), nil
}

func (o *RouteStream) close(sendException bool) {
	o.lock.Lock()
	if o.isClosed {
		return
	}
	o.isClosed = true
	o.lock.Unlock()
	if sendException {
		cmd := ExceptionCommand([]byte("EOF"), o.id)
		o.commandWriter.AsyncWriteCommand(cmd)
	}
	close(o.closed)
	close(o.w)
	return
}

func (o *RouteStream) Close() error {
	o.close(true)
	return nil
}

func (o *RouteStream) onPacketReceived(data []byte) {
	o.lock.Lock()
	defer o.lock.Unlock()
	if o.isClosed {
		return
	}
	for {
		if len(o.w) <= 0 {
			break
		}
		buf := <-o.w
		o.cache.Write(buf)
	}
	o.w <- data
}

type waitResult struct {
	code   byte
	stream *RouteStream
}

type allocateWaitObject struct {
	exit   chan int
	result chan waitResult
	hash   string
}

type RouteConnection struct {
	isClosed      bool
	serverConn    net.Conn
	streams       map[uint16]*RouteStream
	lock          sync.RWMutex
	waitQueue     map[string]*allocateWaitObject
	wCmd          chan Command
	rCmd          chan Command
	expireTimeout time.Duration
}

func NewRouteConnection(con net.Conn, expireTimeout time.Duration) *RouteConnection {
	c := &RouteConnection{
		serverConn:    con,
		streams:       map[uint16]*RouteStream{},
		lock:          sync.RWMutex{},
		waitQueue:     map[string]*allocateWaitObject{},
		wCmd:          make(chan Command, 1024),
		rCmd:          make(chan Command, 1024),
		expireTimeout: expireTimeout,
	}
	go c.processLoop()
	go c.writeLoop()
	return c
}

func (o *RouteConnection) Close() error {
	o.lock.Lock()
	defer o.lock.Unlock()
	if o.isClosed {
		return nil
	}
	o.isClosed = true
	for _, v := range o.streams {
		v.close(false)
	}
	close(o.wCmd)
	close(o.rCmd)
	return nil
}

func (o *RouteConnection) processLoop() {
	for {
		cmd := <-o.rCmd
		if cmd == nil {
			break
		}
		log.Println("process command:", cmd)
		o.processCommand(cmd)
	}
}

func (o *RouteConnection) processCommand(cmd Command) {
	switch cmd.Type() {
	case CommandConnectResponse:
		var waitObject *allocateWaitObject
		var stream *RouteStream
		payload := cmd.Payload()
		code := payload[0]
		hash := payload[1:]
		hashText := hex.EncodeToString(hash)
		o.lock.Lock()
		waitObject = o.waitQueue[hashText]
		delete(o.waitQueue, hashText)
		o.lock.Unlock()
		if waitObject == nil {
			break
		}
		if code == 0 {
			stream = NewRouteStream(cmd.StreamID(), o)
			o.streams[cmd.StreamID()] = stream
		}
		log.Println("allocate stream:", cmd.StreamID(), " error code:", code)
		waitObject.result <- waitResult{
			code:   code,
			stream: stream,
		}
	case CommandData:
		stream := o.streams[cmd.StreamID()]
		if stream != nil {
			stream.onPacketReceived(cmd.Payload())
		}
	case CommandException:
		stream := o.streams[cmd.StreamID()]
		if stream != nil {
			stream.close(false)
			delete(o.streams, cmd.StreamID())
		}
		log.Println("remove stream:", cmd.StreamID())
	case CommandEcho:
		o.AsyncWriteCommand(cmd)
	default:
		log.Println("invalid command type")
	}
}

func (o *RouteConnection) readLoop() {
	for {
		o.serverConn.SetReadDeadline(time.Now().Add(o.expireTimeout))
		buf := make([]byte, MaxPacketSize)
		n, err := o.serverConn.Read(buf[:2])
		if err != nil || n != 2 {
			break
		}
		size := binary.BigEndian.Uint16(buf)
		if size > MaxPacketSize {
			log.Println("out of maxpacketsize")
			break
		}
		n, err = o.serverConn.Read(buf[2 : 2+size])
		if err != nil || n != int(size) {
			break
		}
		cmd := (Command)(buf[:2+size])
		o.rCmd <- cmd
	}
}

func (o *RouteConnection) writeLoop() {
	for {
		cmd := <-o.wCmd
		if cmd == nil {
			break
		}
		if cmd.Type() == CommandException {
			o.rCmd <- cmd
		}
		_, err := o.serverConn.Write([]byte(cmd))
		if err != nil {
			break
		}
	}
}

func (o *RouteConnection) AsyncWriteCommand(cmd Command) {
	o.wCmd <- cmd
}

func (o *RouteConnection) AllocateStream(timeout time.Duration) (stream *RouteStream, err error) {
	hash := md5.Sum([]byte(time.Now().String()))
	waitObject := &allocateWaitObject{
		exit:   make(chan int),
		result: make(chan waitResult),
		hash:   hex.EncodeToString(hash[:]),
	}
	o.lock.Lock()
	o.waitQueue[waitObject.hash] = waitObject
	o.lock.Unlock()
	if timeout != 0 {
		time.AfterFunc(timeout, func() {
			close(waitObject.exit)
			o.lock.Lock()
			delete(o.waitQueue, waitObject.hash)
			o.lock.Unlock()
		})
	}
	cmd := ConnectCommand(hash[:])
	o.AsyncWriteCommand(cmd)
	select {
	case <-waitObject.exit:
		err = errors.New("allocate stream timeout")
	case result := <-waitObject.result:
		if result.code == 0 {
			stream = result.stream
			err = nil
		} else {
			err = errors.New("allocate stream from server failed :" + strconv.Itoa(int(result.code)))
			stream = nil
		}
	}
	close(waitObject.result)
	if timeout == 0 {
		close(waitObject.exit)
	}
	return
}

type APGroup struct {
	lock  sync.RWMutex
	conns map[string]*RouteConnection
}

func NewAPGroup() *APGroup {
	return &APGroup{lock: sync.RWMutex{}, conns: map[string]*RouteConnection{}}
}

func (o *APGroup) lookupConnection(key string) *RouteConnection {
	o.lock.Lock()
	defer o.lock.Unlock()
	return o.conns[key]
}

func (o *APGroup) pickupConnection() *RouteConnection {
	index := (int)(time.Now().UnixNano() & 0xFFFFFF)
	o.lock.Lock()
	defer o.lock.Unlock()
	size := len(o.conns)
	list := make([]*RouteConnection, size)
	i := 0
	for _, v := range o.conns {
		list[i] = v
		i++
	}
	return list[index%size]
}

func (o *APGroup) removeConnection(key string) *RouteConnection {
	o.lock.Lock()
	defer o.lock.Unlock()
	con := o.conns[key]
	delete(o.conns, key)
	return con
}

func (o *APGroup) serverNewConnection(con net.Conn, expireTimeout time.Duration, key string) {
	old := o.removeConnection(key)
	if old != nil {
		old.Close()
	}
	log.Println("add RouteConnection:", con.RemoteAddr().String())
	rc := NewRouteConnection(con, expireTimeout)
	o.lock.Lock()
	o.conns[key] = rc
	o.lock.Unlock()
	rc.readLoop()
	log.Println("remove RouteConnection:", con.RemoteAddr().String())
	o.removeConnection(key)
	rc.Close()
}

func (o *APGroup) AllocateStream(key string, timeout time.Duration) (*RouteStream, error) {
	rc := o.lookupConnection(key)
	if rc == nil {
		return nil, errors.New("RouteConnection not exist for key:" + key)
	}
	return rc.AllocateStream(timeout)
}

type clientContext struct {
	addr       string
	conn       net.Conn
	activeTime time.Time
	sendBytes  int64
	recvBytes  int64
	sendSpeed  int64
	recvSpeed  int64
}

type RouteHijack interface {
	//SelectRouteConnection
	//called when new client connection accept
	SelectRouteConnection(*Route, net.Conn) (*RouteConnection, error)
	//GetRouteConnectionKey
	//called when new route server connection accept
	GetRouteConnectionKey(*Route, net.Conn) (string, error)
}

type Route struct {
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
	route                     *APGroup
	clientsLock               sync.RWMutex
	clients                   map[string]*clientContext
	hijack                    RouteHijack
	serverListener            net.Listener
	routeListener             net.Listener
	exit                      chan int
}

func (o *Route) Hijack(h RouteHijack) *Route {
	o.hijack = h
	return o
}

func (o *Route) expireTimeoutBackgroundThread() {
	ticker := time.NewTicker(time.Duration(o.ClientExpireTimeoutSecond/2) * time.Second)
	for {
		select {
		case <-o.exit:
			ticker.Stop()
			return
		case <-ticker.C:
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
}

func (o *Route) addClientConnection(ctx *clientContext) {
	o.clientsLock.Lock()
	defer o.clientsLock.Unlock()
	ctx.addr = ctx.conn.RemoteAddr().String()
	o.clients[ctx.addr] = ctx
}

func (o *Route) removeClientConnection(ctx *clientContext) {
	o.clientsLock.Lock()
	defer o.clientsLock.Unlock()
	delete(o.clients, ctx.addr)
}

func (o *Route) copyIO(dst io.Writer, src io.Reader, ctx *clientContext, send bool) {
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

func (o *Route) serveClient() error {
	var err error
	o.serverListener, err = net.Listen("tcp", o.ServerAddress)
	if err != nil {
		return err
	}
	defer o.Close()
	for {
		con, err := o.serverListener.Accept()
		if err != nil {
			return err
		}
		go o.serverConnectionHandler(con)
	}
}

func (o *Route) serveAP() error {
	var err error
	o.routeListener, err = net.Listen("tcp", o.RouteAddress)
	if err != nil {
		return err
	}
	defer o.Close()
	for {
		con, err := o.routeListener.Accept()
		if err != nil {
			return err
		}
		go o.routeConnectionHandler(con)
	}
}

func (o *Route) selectRouteConnection(con net.Conn) (*RouteConnection, error) {
	if o.hijack != nil {
		return o.hijack.SelectRouteConnection(o, con)
	}
	rc := o.route.pickupConnection()
	if rc == nil {
		return nil, errors.New("not routeconnection avaliable")
	}
	return rc, nil
}

func (o *Route) getRouteConnectionKey(con net.Conn) (string, error) {
	if o.hijack != nil {
		return o.hijack.GetRouteConnectionKey(o, con)
	}
	return con.RemoteAddr().String(), nil
}

func (o *Route) routeConnectionHandler(con net.Conn) {
	key, err := o.getRouteConnectionKey(con)
	if err != nil {
		log.Println("get routeconnection key failed:", err)
		con.Close()
		return
	}
	o.route.serverNewConnection(con, time.Duration(o.ServerExpireTimeoutSecond)*time.Second, key)
}

func (o *Route) serverConnectionHandler(con net.Conn) {
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

func (o *Route) Close() error {
	if o.serverListener != nil {
		o.serverListener.Close()
		o.serverListener = nil
	}
	if o.routeListener != nil {
		o.routeListener.Close()
	}
	return nil
}

func (o *Route) Run() {
	o.route = NewAPGroup()
	o.clientsLock = sync.RWMutex{}
	o.clients = map[string]*clientContext{}
	o.exit = make(chan int)
	go o.expireTimeoutBackgroundThread()
	wg := sync.WaitGroup{}
	wg.Add(2)
	go func() {
		o.serveClient()
		o.Close()
		wg.Done()
	}()
	go func() {
		o.serveAP()
		o.Close()
		wg.Done()
	}()
	wg.Wait()
	close(o.exit)
	return
}
