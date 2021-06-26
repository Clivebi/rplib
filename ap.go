package rplib

import (
	"encoding/binary"
	"log"
	"net"
	"sync"
	"time"
)

type CommandWriter interface {
	AsyncWriteCommand(cmd Command)
}

type APStream struct {
	isClosed      bool
	id            uint16
	backend       net.Conn
	w             chan []byte
	commandWriter CommandWriter
}

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
	cmd := ExceptionCommand([]byte(err.Error()), o.id)
	o.commandWriter.AsyncWriteCommand(cmd)
}

func (o *APStream) dataArrived(data []byte) {
	if o.isClosed {
		return
	}
	cmd := DataCommand(data, o.id)
	o.commandWriter.AsyncWriteCommand(cmd)
}

func (o *APStream) writeLoop() {
	for {
		data := <-o.w
		if data == nil {
			break
		}
		_, err := o.backend.Write(data)
		if err != nil {
			o.exception(err)
			break
		}
	}
}

func (o *APStream) readLoop() {
	cache_size := MaxPacketSize - payloadOffset
	for {
		buf := make([]byte, cache_size)
		n, err := o.backend.Read(buf)
		if err != nil {
			o.exception(err)
			break
		}
		o.dataArrived(buf[:n])
	}
}

type AP struct {
	AliveTick    time.Duration
	conn         net.Conn
	nextStreamID uint16
	lock         sync.RWMutex
	streams      map[uint16]*APStream
	backendAddr  string
	wCmd         chan Command
	rCmd         chan Command
}

func (o *AP) Close() error {
	//step 1 raise readloop exit
	//step 2 shutdown all stream
	//step 3 raise processLoop exit
	//step 4 raise writeloop exit
	o.conn.Close()
	o.lock.Lock()
	for _, v := range o.streams {
		v.Close()
	}
	o.streams = map[uint16]*APStream{}
	o.lock.Unlock()
	close(o.rCmd)
	close(o.wCmd)
	return nil
}

func (o *AP) processLoop() {
	for {
		cmd := <-o.rCmd
		if cmd == nil {
			break
		}
		log.Println("process command:", cmd)
		o.processCommand(cmd)
	}
}

func (o *AP) processConnect(cmd Command, id uint16) {
	backendConn, err := net.Dial("tcp", o.backendAddr)
	if err != nil {
		rsp := ConnectResponseCommand(cmd.Payload(), 0, 1)
		o.AsyncWriteCommand(rsp)
		return
	}
	stream := &APStream{
		id:            id,
		backend:       backendConn,
		w:             make(chan []byte, 64),
		commandWriter: o,
	}
	log.Println("allocate stream:", id)
	o.lock.Lock()
	o.streams[id] = stream
	o.lock.Unlock()

	go stream.readLoop()
	rsp := ConnectResponseCommand(cmd.Payload(), id, 0)
	o.AsyncWriteCommand(rsp)
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
		if st != nil {
			st.w <- cmd.Payload()
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
	case CommandEcho:
		break
	default:
		log.Println("invalid command type")
	}
}

func (o *AP) readLoop() {
	for {
		buf := make([]byte, MaxPacketSize)
		n, err := o.conn.Read(buf[:2])
		if err != nil || n != 2 {
			log.Println(err)
			break
		}
		size := binary.BigEndian.Uint16(buf)
		if size > MaxPacketSize {
			log.Println("out of maxpacketsize")
			break
		}
		n, err = o.conn.Read(buf[2 : 2+size])
		if err != nil || n != int(size) {
			log.Println(err)
			break
		}
		cmd := (Command)(buf[:2+size])
		o.rCmd <- cmd
	}
}

func (o *AP) writeLoop() {
	ticker := time.NewTicker(o.AliveTick)
	defer ticker.Stop()
	for {
		select {
		case cmd := <-o.wCmd:
			if cmd == nil {
				return
			}
			if cmd.Type() == CommandException {
				o.rCmd <- cmd
			}
			_, err := o.conn.Write([]byte(cmd))
			if err != nil {
				return
			}
		case <-ticker.C:
			ec := EchoCommand([]byte{0x01}, 0)
			_, err := o.conn.Write([]byte(ec))
			if err != nil {
				return
			}
		}
	}
}

func (o *AP) AsyncWriteCommand(cmd Command) {
	o.wCmd <- cmd
}

func (o *AP) Run() {
	go o.processLoop()
	go o.writeLoop()
	o.readLoop()
}

func NewAP(routeAddress string, backendAddress string, aliveTick time.Duration) (*AP, error) {
	con, err := net.Dial("tcp4", routeAddress)
	if err != nil {
		return nil, err
	}
	s := &AP{
		AliveTick:    aliveTick,
		conn:         con,
		nextStreamID: uint16(time.Now().Unix() & 0xFFF),
		lock:         sync.RWMutex{},
		streams:      map[uint16]*APStream{},
		backendAddr:  backendAddress,
		wCmd:         make(chan Command, 1024),
		rCmd:         make(chan Command, 1024),
	}
	return s, nil
}
