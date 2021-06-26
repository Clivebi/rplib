package rplib

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
)

const (
	MaxPacketSize          = 16 * 1024
	CommandConnect         = byte(1)
	CommandConnectResponse = byte(2)
	CommandData            = byte(3)
	CommandException       = byte(4)
	CommandEcho            = byte(5)
	sizeOffset             = 0
	typeOffset             = sizeOffset + 2
	idOffset               = typeOffset + 1
	payloadOffset          = idOffset + 2
)

type Command []byte

func (o Command) Size() uint16 {
	return binary.BigEndian.Uint16(o[sizeOffset:])
}

func (o Command) SetSize(size uint16) {
	binary.BigEndian.PutUint16(o[sizeOffset:], size)
}

func (o Command) Type() byte {
	return o[typeOffset]
}

func (o Command) SetType(t byte) {
	o[typeOffset] = t
}

func (o Command) StreamID() uint16 {
	return binary.BigEndian.Uint16(o[idOffset:])
}

func (o Command) SetStreamID(id uint16) {
	binary.BigEndian.PutUint16(o[idOffset:], id)
}

func (o Command) SetPayload(payload []byte) int {
	return copy(o[payloadOffset:], payload)
}

func (o Command) Payload() []byte {
	return o[payloadOffset:]
}

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

func ConnectCommand(hash []byte) Command {
	buf := (Command)(make([]byte, len(hash)+payloadOffset))
	buf.SetSize((uint16)(payloadOffset + len(hash) - typeOffset))
	buf.SetType(CommandConnect)
	buf.SetStreamID(0)
	buf.SetPayload(hash)
	return buf
}

func UnpackConnect(payload []byte) (hash []byte, err error) {
	return payload, nil
}

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

func UnpackConnectResponse(payload []byte) (hash []byte, errorCode byte, err error) {
	if len(payload) < 2 {
		return nil, 0, errors.New("invalid payload for ConnectResponse")
	}
	return payload[1:], payload[0], nil
}

func DataCommand(data []byte, StreamID uint16) Command {
	buf := (Command)(make([]byte, len(data)+payloadOffset))
	buf.SetSize((uint16)(payloadOffset + len(data) - typeOffset))
	buf.SetType(CommandData)
	buf.SetStreamID(StreamID)
	buf.SetPayload(data)
	return buf
}

func EchoCommand(data []byte, StreamID uint16) Command {
	buf := (Command)(make([]byte, len(data)+payloadOffset))
	buf.SetSize((uint16)(payloadOffset + len(data) - typeOffset))
	buf.SetType(CommandEcho)
	buf.SetStreamID(StreamID)
	buf.SetPayload(data)
	return buf
}

func ExceptionCommand(data []byte, StreamID uint16) Command {
	buf := (Command)(make([]byte, len(data)+payloadOffset))
	buf.SetSize((uint16)(payloadOffset + len(data) - typeOffset))
	buf.SetType(CommandException)
	buf.SetStreamID(StreamID)
	buf.SetPayload(data)
	return buf
}
