package rplib

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"strconv"
)

const (
	//MaxPacketSize the max packet size
	MaxPacketSize = commandCapSize - 2
	//MinPacketSize the min packet size
	MinPacketSize = 4 // command_type(byte)+stream_id(uint16)+payload(1)
	//CommandConnect connect command used by route side
	CommandConnect = byte(1)
	//CommandConnectResponse connect response used by ap side
	CommandConnectResponse = byte(2)
	//CommandData exchange data packet  route side & ap side
	CommandData = byte(3)
	//CommandException exception data packet route side & ap side
	CommandException = byte(4)
	//CommandEcho  heartbeat data packet ap side send,route side response
	CommandEcho    = byte(5)
	sizeOffset     = 0
	typeOffset     = sizeOffset + 2
	idOffset       = typeOffset + 1
	payloadOffset  = idOffset + 2
	commandCapSize = 8 * 1024
)

//Command command object struct
// packet_size(uint16)+command_type(byte)+stream_id(uint16)+payload(any size)
// the packet_size not include the size of packet_size itself
// when connect command ,the stream_Id is zero ,the ap allcote one stream id
// and return in the connect response command,other command require the stream id
// not equal zero,if ap or route receive a unmanged stream id,the packet will be dropped
type Command []byte

//Size the the packet_size not include the size of packet_size itself
func (o Command) Size() uint16 {
	return binary.BigEndian.Uint16(o[sizeOffset:])
}

//SetSize set the size field
func (o Command) SetSize(size uint16) {
	binary.BigEndian.PutUint16(o[sizeOffset:], size)
}

//Type get the command type field
func (o Command) Type() byte {
	return o[typeOffset]
}

//SetType set the command type field
func (o Command) SetType(t byte) {
	o[typeOffset] = t
}

//StreamID get the command type field
func (o Command) StreamID() uint16 {
	return binary.BigEndian.Uint16(o[idOffset:])
}

//SetStreamID set the command type field
func (o Command) SetStreamID(id uint16) {
	binary.BigEndian.PutUint16(o[idOffset:], id)
}

//SetPayload set the payload field
func (o Command) SetPayload(payload []byte) int {
	return copy([]byte(o)[payloadOffset:], payload)
}

//PayloadSize get payload size
func (o Command) PayloadSize() int {
	size := int(o.Size()) - payloadOffset + typeOffset
	return size
}

//Payload get the payload field
func (o Command) Payload() []byte {
	buf := o[payloadOffset:]
	return buf[:o.PayloadSize()]
}

//String get the debug string
func (o Command) String() string {
	w := bytes.NewBuffer(nil)
	fmt.Fprintf(w, "Type:")
	switch o.Type() {
	case CommandConnect:
		fmt.Fprintf(w, " Connect")
	case CommandConnectResponse:
		fmt.Fprintf(w, " ConnectResponse")
	case CommandData:
		fmt.Fprintf(w, " Data")
	case CommandException:
		fmt.Fprintf(w, " Exception")
	case CommandEcho:
		fmt.Fprintf(w, " Echo")
	}
	fmt.Fprintf(w, " StreamID:%v", o.StreamID())
	fmt.Fprintf(w, " PayloadSize:%v\n", o.PayloadSize())
	//fmt.Fprintf(w, "%s", hex.Dump(o.Payload()))
	return w.String()
}

//CloneCommand clone one command object
func CloneCommand(cmd Command) Command {
	ne := gPool.Allocate()
	copy(ne, cmd[:commandCapSize])
	return ne
}

//BuildConnectCommand create one  connect command,the payload is the hash
func BuildConnectCommand(buf Command, hash []byte) {
	buf.SetSize((uint16)(payloadOffset + len(hash) - typeOffset))
	buf.SetType(CommandConnect)
	buf.SetStreamID(0)
	buf.SetPayload(hash)
}

//ConnectResponseCommand create connect response command
//the payload = error_code(byte)+hash(receive from the connect command)
// error_code == 0 for success
// if error_code == 0,stream id include ap allocate stream ID
func BuildConnectResponseCommand(buf Command, hash []byte, StreamID uint16, errorCode byte) {
	raw := []byte(buf)
	dataSize := len(hash) + 1
	buf.SetSize((uint16)(payloadOffset + dataSize - typeOffset))
	buf.SetType(CommandConnectResponse)
	buf.SetStreamID(StreamID)
	raw[payloadOffset] = errorCode
	copy(raw[payloadOffset+1:], hash)
}

//BuildDataCommand create data command
func BuildDataCommand(buf Command, size int, StreamID uint16) {
	buf.SetSize((uint16)(payloadOffset + size - typeOffset))
	buf.SetType(CommandData)
	buf.SetStreamID(StreamID)
}

//BuildEchoCommand create echo data command
func BuildEchoCommand(buf Command, data []byte, StreamID uint16) {
	buf.SetSize((uint16)(payloadOffset + len(data) - typeOffset))
	buf.SetType(CommandEcho)
	buf.SetStreamID(StreamID)
	buf.SetPayload(data)
}

//BuildExceptionCommand create exception command,
//now,the data is the error object
func BuildExceptionCommand(buf Command, data []byte, StreamID uint16) {
	buf.SetSize((uint16)(payloadOffset + len(data) - typeOffset))
	buf.SetType(CommandException)
	buf.SetStreamID(StreamID)
	buf.SetPayload(data)
}

func writeBuffer(w io.Writer, buf []byte) error {
	totalSize := len(buf)
	writeSize := 0
	for {
		n, err := w.Write(buf[writeSize:])
		if err != nil {
			return err
		}
		writeSize += n
		if writeSize == totalSize {
			return nil
		}
	}
}

func writeCommand(w io.Writer, cmd Command) error {
	totalSize := int(cmd.Size()) + 2
	buf := []byte(cmd)[:totalSize]
	return writeBuffer(w, buf)
}

func readCommand(r io.Reader) (Command, error) {
	buf := gPool.Allocate()
	n, err := r.Read(buf[0:2])
	if err != nil || n != 2 {
		gPool.Release(buf)
		return nil, err
	}
	size := int(binary.BigEndian.Uint16(buf))
	offset := 0
	if size > MaxPacketSize {
		gPool.Release(buf)
		return nil, errors.New("packet size out of limit: " + strconv.Itoa(int(size)))
	}
	if size < MinPacketSize {
		gPool.Release(buf)
		return nil, errors.New("packet too small")
	}
	for {
		n, err := r.Read(buf[offset+2 : size+2])
		if err != nil {
			gPool.Release(buf)
			return nil, err
		}
		offset += n
		if size == offset {
			return buf, nil
		}
	}
}
