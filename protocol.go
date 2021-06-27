package rplib

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

const (
	//MaxPacketSize the max packet size
	MaxPacketSize = 32 * 1024
	//CommandConnect connect command used by route side
	CommandConnect = byte(1)
	//CommandConnectResponse connect response used by ap side
	CommandConnectResponse = byte(2)
	//CommandData exchange data packet  route side & ap side
	CommandData = byte(3)
	//CommandException exception data packet route side & ap side
	CommandException = byte(4)
	//CommandEcho  heartbeat data packet ap side send,route side response
	CommandEcho   = byte(5)
	sizeOffset    = 0
	typeOffset    = sizeOffset + 2
	idOffset      = typeOffset + 1
	payloadOffset = idOffset + 2
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
	return copy(o[payloadOffset:], payload)
}

//Payload get the payload field
func (o Command) Payload() []byte {
	return o[payloadOffset:]
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
	fmt.Fprintf(w, " PayloadSize:%v", len(o.Payload()))
	return w.String()
}

//ConnectCommand create one  connect command,the payload is the hash
func ConnectCommand(hash []byte) Command {
	buf := (Command)(make([]byte, len(hash)+payloadOffset))
	buf.SetSize((uint16)(payloadOffset + len(hash) - typeOffset))
	buf.SetType(CommandConnect)
	buf.SetStreamID(0)
	buf.SetPayload(hash)
	return buf
}

//ConnectResponseCommand create connect response command
//the payload = error_code(byte)+hash(receive from the connect command)
// error_code == 0 for success
// if error_code == 0,stream id include ap allocate stream ID
func ConnectResponseCommand(hash []byte, StreamID uint16, errorCode byte) Command {
	dataSize := len(hash) + 1
	raw := make([]byte, dataSize+payloadOffset)
	buf := (Command)(raw)
	buf.SetSize((uint16)(payloadOffset + dataSize - typeOffset))
	buf.SetType(CommandConnectResponse)
	buf.SetStreamID(StreamID)
	raw[payloadOffset] = errorCode
	copy(raw[payloadOffset+1:], hash)
	return buf
}

//DataCommand create data command
func DataCommand(data []byte, StreamID uint16) Command {
	buf := (Command)(make([]byte, len(data)+payloadOffset))
	buf.SetSize((uint16)(payloadOffset + len(data) - typeOffset))
	buf.SetType(CommandData)
	buf.SetStreamID(StreamID)
	buf.SetPayload(data)
	return buf
}

//EchoCommand create echo data command
func EchoCommand(data []byte, StreamID uint16) Command {
	buf := (Command)(make([]byte, len(data)+payloadOffset))
	buf.SetSize((uint16)(payloadOffset + len(data) - typeOffset))
	buf.SetType(CommandEcho)
	buf.SetStreamID(StreamID)
	buf.SetPayload(data)
	return buf
}

//ExceptionCommand create exception command,
//now,the data is the error object
func ExceptionCommand(data []byte, StreamID uint16) Command {
	buf := (Command)(make([]byte, len(data)+payloadOffset))
	buf.SetSize((uint16)(payloadOffset + len(data) - typeOffset))
	buf.SetType(CommandException)
	buf.SetStreamID(StreamID)
	buf.SetPayload(data)
	return buf
}
