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

type Route struct {
	lock  sync.RWMutex
	conns map[string]*RouteConnection
}

func NewRoute() *Route {
	return &Route{lock: sync.RWMutex{}, conns: map[string]*RouteConnection{}}
}

func (o *Route) lookupConnection(key string) *RouteConnection {
	o.lock.Lock()
	defer o.lock.Unlock()
	return o.conns[key]
}

func (o *Route) pickupConnection() *RouteConnection {
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

func (o *Route) removeConnection(key string) *RouteConnection {
	o.lock.Lock()
	defer o.lock.Unlock()
	con := o.conns[key]
	delete(o.conns, key)
	return con
}

func (o *Route) serverNewConnection(con net.Conn, expireTimeout time.Duration, key string) {
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

func (o *Route) AllocateStream(key string, timeout time.Duration) (*RouteStream, error) {
	rc := o.lookupConnection(key)
	if rc == nil {
		return nil, errors.New("RouteConnection not exist for key:" + key)
	}
	return rc.AllocateStream(timeout)
}
