package rplib

import (
	"container/list"
	"crypto/md5"
	"encoding/hex"
	"errors"
	"io"
	"log"
	"net"
	"strconv"
	"sync"
	"time"
)

type zcCache struct {
	lock  sync.RWMutex
	chunk []byte
	pages *list.List
}

func (o *zcCache) Read(b []byte) (int, error) {
	o.lock.Lock()
	defer o.lock.Unlock()
	if o.chunk == nil {
		e := o.pages.Front()
		if e == nil {
			return 0, io.EOF
		}
		o.pages.Remove(e)
		o.chunk = e.Value.([]byte)
	}
	copy(b, o.chunk)
	n := len(b)
	if n >= len(o.chunk) {
		n = len(o.chunk)
		o.chunk = nil
	} else {
		o.chunk = o.chunk[n:]
	}
	return n, nil
}

func (o *zcCache) Write(b []byte) (int, error) {
	o.lock.Lock()
	defer o.lock.Unlock()
	o.pages.PushBack(b)
	return len(b), nil
}

func (o *zcCache) IsEmpty() bool {
	o.lock.Lock()
	defer o.lock.Unlock()
	return o.pages.Len() == 0 && o.chunk == nil
}

//RouteAPStream server side AP Stream
type RouteAPStream struct {
	isClosed      bool
	id            uint16
	commandWriter CommandWriter
	cache         *zcCache
	rCond         *sync.Cond
}

//NewRouteAPStream create instance of RouteAPStream
func NewRouteAPStream(id uint16, commandWriter CommandWriter) *RouteAPStream {
	return &RouteAPStream{
		isClosed:      false,
		id:            id,
		commandWriter: commandWriter,
		cache: &zcCache{
			chunk: nil,
			pages: list.New(),
			lock:  sync.RWMutex{},
		},
		rCond: sync.NewCond(&sync.RWMutex{}),
	}
}

//Read implement io.Reader
func (o *RouteAPStream) Read(b []byte) (int, error) {
	o.rCond.L.Lock()
	for o.cache.IsEmpty() {
		o.rCond.Wait()
		if o.isClosed {
			o.rCond.L.Unlock()
			return 0, io.EOF
		}
	}
	n, err := o.cache.Read(b)
	o.rCond.L.Unlock()
	return n, err
}

//Write implement io.Write
func (o *RouteAPStream) Write(b []byte) (int, error) {
	cmd := DataCommand(b, o.id)
	o.commandWriter.AsyncWriteCommand(cmd)
	//logBufferHash("client write ", b)
	return len(b), nil
}

func (o *RouteAPStream) close(sendException bool) {
	if o.isClosed {
		o.rCond.Broadcast()
		return
	}
	o.isClosed = true
	if sendException {
		cmd := ExceptionCommand([]byte("EOF"), o.id)
		o.commandWriter.AsyncWriteCommand(cmd)
	}
	o.rCond.Broadcast()
	return
}

//Close implement the io.Closer
func (o *RouteAPStream) Close() error {
	o.close(true)
	return nil
}

func (o *RouteAPStream) onPacketArrived(data []byte) {
	o.cache.Write(data)
	o.rCond.Signal()
}

type waitResult struct {
	code   byte
	stream *RouteAPStream
}

type allocateWaitObject struct {
	exit   chan int
	result chan waitResult
	hash   string
}

//APConnection the connection of AP
type APConnection struct {
	streamCount   int
	isClosed      bool
	serverConn    net.Conn
	streams       map[uint16]*RouteAPStream
	lock          sync.RWMutex
	waitQueue     map[string]*allocateWaitObject
	wCmd          chan Command
	rCmd          chan Command
	expireTimeout time.Duration
}

func newAPConnection(con net.Conn, expireTimeout time.Duration) *APConnection {
	c := &APConnection{
		streamCount:   0,
		isClosed:      false,
		serverConn:    con,
		streams:       map[uint16]*RouteAPStream{},
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

func (o *APConnection) Close() error {
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

func (o *APConnection) processLoop() {
	for {
		cmd := <-o.rCmd
		if cmd == nil {
			break
		}
		//log.Println("process command:", cmd)
		o.processCommand(cmd)
	}
}

func (o *APConnection) processCommand(cmd Command) {
	switch cmd.Type() {
	case CommandConnectResponse:
		var waitObject *allocateWaitObject
		var stream *RouteAPStream
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
			stream = NewRouteAPStream(cmd.StreamID(), o)
			o.streams[cmd.StreamID()] = stream
			o.streamCount++
		}
		log.Println("allocate stream:", cmd.StreamID(), " error code:", code)
		waitObject.result <- waitResult{
			code:   code,
			stream: stream,
		}
	case CommandData:
		stream := o.streams[cmd.StreamID()]
		//logBufferHash("receive transfer data ", cmd.Payload())
		if stream != nil {
			stream.onPacketArrived(cmd.Payload())
		}
	case CommandException:
		stream := o.streams[cmd.StreamID()]
		if stream != nil {
			stream.close(false)
			delete(o.streams, cmd.StreamID())
			o.streamCount--
		}
		log.Println("remove stream:", cmd.StreamID())
	case CommandEcho:
		o.AsyncWriteCommand(cmd)
	default:
		log.Println("invalid command type")
	}
}

func (o *APConnection) readLoop() {
	for {
		o.serverConn.SetReadDeadline(time.Now().Add(o.expireTimeout))
		cmd, err := readCommand(o.serverConn)
		if err != nil {
			break
		}
		o.rCmd <- cmd
	}
}

func (o *APConnection) writeLoop() {
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

//AsyncWriteCommand implement CommandWriter
func (o *APConnection) AsyncWriteCommand(cmd Command) {
	o.wCmd <- cmd
}

//StreamSize return count of stream on this connection
func (o *APConnection) StreamSize() int {
	return o.streamCount
}

//AllocateStream allocate one stream from this AP
func (o *APConnection) AllocateStream(timeout time.Duration) (stream *RouteAPStream, err error) {
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

//APManger ap connection manger
type APManger struct {
	apConnsLock sync.RWMutex
	apConns     map[string]*APConnection
}

//NewAPManger new instance of APManger
func NewAPManger() *APManger {
	return &APManger{apConnsLock: sync.RWMutex{}, apConns: map[string]*APConnection{}}
}

//ListKey get all connection keys
func (o *APManger) ListKey() []string {
	o.apConnsLock.Lock()
	defer o.apConnsLock.Unlock()
	ret := make([]string, len(o.apConns))
	i := 0
	for k := range o.apConns {
		ret[i] = k
		i++
	}
	return ret
}

//ConnectionSize get the connection count
func (o *APManger) ConnectionSize() int {
	o.apConnsLock.Lock()
	defer o.apConnsLock.Unlock()
	return len(o.apConns)
}

//LookupConnection lookup APConnection
func (o *APManger) LookupConnection(key string) *APConnection {
	o.apConnsLock.Lock()
	defer o.apConnsLock.Unlock()
	return o.apConns[key]
}

//PickupMinimumStreamConnection get the minimum stream count connection
func (o *APManger) PickupMinimumStreamConnection() *APConnection {
	o.apConnsLock.Lock()
	defer o.apConnsLock.Unlock()
	count := 1000000
	var rc *APConnection
	for _, v := range o.apConns {
		if v.StreamSize() == 0 {
			return v
		}
		if v.StreamSize() < count {
			count = v.StreamSize()
			rc = v
		}
	}
	return rc
}

//PickupConnection random pickup APConnection
func (o *APManger) PickupConnection() *APConnection {
	index := (int)(time.Now().UnixNano() & 0xFFFFFF)
	o.apConnsLock.Lock()
	defer o.apConnsLock.Unlock()
	size := len(o.apConns)
	if size == 0 {
		return nil
	}
	list := make([]*APConnection, size)
	i := 0
	for _, v := range o.apConns {
		list[i] = v
		i++
	}
	return list[index%size]
}

func (o *APManger) removeConnection(key string) *APConnection {
	o.apConnsLock.Lock()
	defer o.apConnsLock.Unlock()
	con := o.apConns[key]
	delete(o.apConns, key)
	return con
}

func (o *APManger) addConnection(key string, ac *APConnection) {
	o.apConnsLock.Lock()
	defer o.apConnsLock.Unlock()
	o.apConns[key] = ac
}

//ServerNewConnection start serve new connection
func (o *APManger) ServerNewConnection(con net.Conn, expireTimeout time.Duration, key string) {
	defer con.Close()
	old := o.removeConnection(key)
	if old != nil {
		old.Close()
	}
	log.Println("add RouteConnection:", con.RemoteAddr().String())
	rc := newAPConnection(con, expireTimeout)
	o.addConnection(key, rc)
	rc.readLoop()
	log.Println("remove RouteConnection:", con.RemoteAddr().String())
	o.removeConnection(key)
	rc.Close()
}

//AllocateStream allocate stream use key matched APConnection
func (o *APManger) AllocateStream(key string, timeout time.Duration) (*RouteAPStream, error) {
	rc := o.LookupConnection(key)
	if rc == nil {
		return nil, errors.New("RouteConnection not exist for key:" + key)
	}
	return rc.AllocateStream(timeout)
}

//the client context
type ClientContext struct {
	addr string
	conn net.Conn
	//ActiveTime last active time
	ActiveTime time.Time
	//SendBytes the total bytes of client sended
	SendBytes int64
	//RecvBytes the total bytes of client received
	RecvBytes int64
	//SendSpeed the speed of client send
	SendSpeed int64
	//RecvSpeed the speed of client recv
	RecvSpeed int64
	tempSend  int64
	tempRecv  int64
}

type RouteHijack interface {
	//SelectRouteConnection
	//called when new client connection accept
	SelectRouteConnection(*Route, net.Conn) (*APConnection, error)
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
	//apManger                  *APManger
	clientsLock    sync.RWMutex
	clients        map[string]*ClientContext
	hijack         RouteHijack
	serverListener net.Listener
	routeListener  net.Listener
	exit           chan int
	APManger
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
				if now.After(v.ActiveTime) && now.Sub(v.ActiveTime) > time.Duration(o.ClientExpireTimeoutSecond)*time.Second {
					v.conn.Close()
					delete(o.clients, k)
				}
				v.SendSpeed = v.tempSend / int64(o.ClientExpireTimeoutSecond) / 2
				v.RecvSpeed = v.tempRecv / int64(o.ClientExpireTimeoutSecond) / 2
				v.tempSend = 0
				v.tempRecv = 0
			}
			o.clientsLock.Unlock()

		}
	}
}

func (o *Route) addClientConnection(ctx *ClientContext) {
	o.clientsLock.Lock()
	defer o.clientsLock.Unlock()
	ctx.addr = ctx.conn.RemoteAddr().String()
	o.clients[ctx.addr] = ctx
}

func (o *Route) removeClientConnection(ctx *ClientContext) {
	o.clientsLock.Lock()
	defer o.clientsLock.Unlock()
	delete(o.clients, ctx.addr)
}

func (o *Route) copyIO(dst io.WriteCloser, src io.ReadCloser, ctx *ClientContext, send bool) {
	buf := make([]byte, 4*1024)
	defer dst.Close()
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
			ctx.tempSend += int64(n)
			ctx.SendBytes += int64(n)
		} else {
			ctx.tempRecv += int64(n)
			ctx.RecvBytes += int64(n)
		}
		ctx.ActiveTime = time.Now()
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

func (o *Route) selectRouteConnection(con net.Conn) (*APConnection, error) {
	if o.hijack != nil {
		return o.hijack.SelectRouteConnection(o, con)
	}
	rc := o.PickupConnection()
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
	o.ServerNewConnection(con, time.Duration(o.ServerExpireTimeoutSecond)*time.Second, key)
}

func (o *Route) serverConnectionHandler(con net.Conn) {
	defer con.Close()
	rc, err := o.selectRouteConnection(con)
	if err != nil {
		log.Println("select routeconnect failed:", err)
		con.Close()
		return
	}
	rs, err := rc.AllocateStream(time.Minute)
	if err != nil {
		log.Println("allocate stream failed:", err)
		con.Close()
		return
	}
	ctx := &ClientContext{ActiveTime: time.Now()}
	ctx.conn = con
	o.addClientConnection(ctx)
	go o.copyIO(con, rs, ctx, false)
	o.copyIO(rs, con, ctx, true)
	o.removeClientConnection(ctx)
}

//Close close the route
func (o *Route) Close() error {
	if o.serverListener != nil {
		o.serverListener.Close()
	}
	if o.routeListener != nil {
		o.routeListener.Close()
	}
	return nil
}

//Run the route unitil catch some error
func (o *Route) Run() {
	o.apConnsLock = sync.RWMutex{}
	o.apConns = map[string]*APConnection{}
	o.clientsLock = sync.RWMutex{}
	o.clients = map[string]*ClientContext{}
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
