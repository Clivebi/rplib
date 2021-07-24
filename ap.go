package rplib

import (
	"log"
	"net"
	"sync"
	"time"
)

//func logBufferHash(prefix string, b []byte) {
//	sh := md5.Sum(b)
//	fmt.Println(prefix, " size=", len(b), " hash=", hex.EncodeToString(sh[:]))
//}

type CommandWriter interface {
	WriteCommand(cmd Command)
}

//APStream access point side stream
type APStream struct {
	isClosed      bool
	id            uint16
	backend       net.Conn
	w             chan Command
	commandWriter CommandWriter
}

//Close close the stream
func (o *APStream) Close() {
	if o.isClosed {
		return
	}
	o.isClosed = true
	o.backend.Close()
	close(o.w)
}

func (o *APStream) exception(err error) {
	if o.isClosed {
		return
	}
	cmd := Command(gPool.Allocate())
	BuildExceptionCommand(cmd, []byte(err.Error()), o.id)
	o.commandWriter.WriteCommand(cmd)
}

func (o *APStream) writeLoop() {
	for {
		cmd := <-o.w
		if cmd == nil {
			break
		}
		//log.Println("write to backend:", cmd)
		err := writeBuffer(o.backend, cmd.Payload())
		gPool.Release(cmd)
		if err != nil {
			o.exception(err)
			break
		}
	}
}

func (o *APStream) readLoop() {
	for {
		buf := gPool.Allocate()
		n, err := o.backend.Read(buf[payloadOffset:])
		if err != nil {
			o.exception(err)
			break
		}
		if o.isClosed {
			break
		}
		//log.Println("backend size:", n)
		cmd := Command(buf)
		BuildDataCommand(cmd, n, o.id)
		o.commandWriter.WriteCommand(cmd)
	}
}

//AP access point
type AP struct {
	isClosed     bool
	echoTick     time.Duration
	conn         net.Conn
	nextStreamID uint16
	lock         sync.RWMutex
	streams      map[uint16]*APStream
	backendAddr  string
	tq           *TaskQueue
	rCmd         chan Command
}

func (o *AP) internalClose() {
	if o.isClosed {
		return
	}
	o.isClosed = true
	o.conn.Close()
	o.lock.Lock()
	for _, v := range o.streams {
		v.Close()
	}
	o.streams = map[uint16]*APStream{}
	o.lock.Unlock()
	close(o.rCmd)
}

//Close close the access point
func (o *AP) Close() error {
	o.tq.PostTask(func() {
		o.internalClose()
	})
	return nil
}

func (o *AP) processLoop() {
	for {
		cmd := <-o.rCmd
		if cmd == nil {
			break
		}
		//log.Println("process command:", cmd)
		o.processCommand(cmd)
	}
}

func (o *AP) processConnect(cmd Command, id uint16) {
	backendConn, err := net.Dial("tcp", o.backendAddr)
	if err != nil {
		rsp := gPool.Allocate()
		BuildConnectResponseCommand(rsp, cmd.Payload(), 0, 1)
		o.WriteCommand(rsp)
		gPool.Release(cmd)
		return
	}
	stream := &APStream{
		id:            id,
		backend:       backendConn,
		w:             make(chan Command, 64),
		commandWriter: o,
	}
	log.Println("allocate stream:", id)
	o.lock.Lock()
	o.streams[id] = stream
	o.lock.Unlock()

	go stream.readLoop()
	rsp := gPool.Allocate()
	BuildConnectResponseCommand(rsp, cmd.Payload(), id, 0)
	o.WriteCommand(rsp)
	gPool.Release(cmd)
	stream.writeLoop()
}

func (o *AP) lookupStream(id uint16) *APStream {
	o.lock.RLock()
	defer o.lock.RUnlock()
	return o.streams[id]
}

func (o *AP) processCommand(cmd Command) {
	switch cmd.Type() {
	case CommandConnect:
		id := o.nextStreamID
		o.nextStreamID++
		go o.processConnect(cmd, id)
	case CommandData:
		st := o.lookupStream(cmd.StreamID())
		//logBufferHash("receive transfer data ", cmd.Payload())
		if st != nil {
			st.w <- cmd
		}
	case CommandException:
		st := o.lookupStream(cmd.StreamID())
		o.lock.Lock()
		delete(o.streams, cmd.StreamID())
		o.lock.Unlock()
		if st != nil {
			st.Close()
			log.Println("remove stream:", cmd.StreamID())
		}
		gPool.Release(cmd)
	case CommandEcho:
		gPool.Release(cmd)
		break
	default:
		log.Println("invalid command type")
	}
}

func (o *AP) readLoop() {
	for {
		o.conn.SetReadDeadline(time.Now().Add(o.echoTick * 2))
		cmd, err := readCommand(o.conn)
		if err != nil {
			log.Println(err)
			break
		}
		o.rCmd <- cmd
	}
}

func (o *AP) sendEcho() {
	if o.isClosed {
		return
	}
	cmd := gPool.Allocate()
	BuildEchoCommand(cmd, []byte{0x01}, 0)
	err := o.internalWriteCommand(cmd)
	if err != nil {
		log.Println(err)
		o.internalClose()
		return
	}
	o.tq.PostDelayTask(func() {
		o.sendEcho()
	}, o.echoTick)
}

func (o *AP) internalWriteCommand(cmd Command) error {
	if o.isClosed {
		return net.ErrClosed
	}
	if cmd.Type() == CommandException {
		o.rCmd <- CloneCommand(cmd)
	}
	//log.Println("write command:", cmd)
	err := writeCommand(o.conn, cmd)
	gPool.Release(cmd)
	return err
}

func (o *AP) WriteCommand(cmd Command) {
	o.tq.PostTask(func() {
		err := o.internalWriteCommand(cmd)
		if err != nil {
			log.Println(err)
			o.internalClose()
		}
	})
}

//Run run the ap unitil catch some error
func (o *AP) Run() {
	go o.processLoop()
	o.tq.PostDelayTask(func() {
		o.sendEcho()
	}, o.echoTick)
	o.readLoop()
}

//NewAP create new AP instance
func NewAP(routeAddress string, backendAddress string, echoTick time.Duration, tq *TaskQueue) (*AP, error) {
	con, err := net.Dial("tcp4", routeAddress)
	if err != nil {
		return nil, err
	}
	s := &AP{
		isClosed:     false,
		echoTick:     echoTick,
		conn:         con,
		nextStreamID: uint16(time.Now().Unix() & 0xFFF),
		lock:         sync.RWMutex{},
		streams:      map[uint16]*APStream{},
		backendAddr:  backendAddress,
		tq:           tq,
		rCmd:         make(chan Command, 32),
	}
	return s, nil
}
