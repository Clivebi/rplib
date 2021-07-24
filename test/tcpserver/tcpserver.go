package main

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
)

func write_packet(b []byte, w io.Writer) error {
	size := len(b)
	err := binary.Write(w, binary.BigEndian, int32(size))
	if err != nil {
		return err
	}
	total := 0
	for {
		n, err := w.Write(b[total:])
		if err != nil {
			return err
		}
		total += n
		if total == size {
			return nil
		}
	}
}

func readLength(r io.Reader, size int) ([]byte, error) {
	buf := make([]byte, size)
	total := 0
	for {
		n, err := r.Read(buf[total:])
		if err != nil {
			return nil, err
		}
		total += n
		if total == size {
			return buf, err
		}
	}
}

func read_packet(r io.Reader) ([]byte, error) {
	buf, err := readLength(r, 4)
	if err != nil {
		return nil, err
	}
	size := int(binary.BigEndian.Uint32(buf))
	if size > 64*1024 {
		return nil, errors.New("invalid packet length=" + strconv.Itoa(size))
	}
	return readLength(r, size)
}

func echo_handler(con net.Conn) {
	defer con.Close()
	i := 0
	for {
		buf, err := read_packet(con)
		if err != nil {
			fmt.Println(err)
			break
		}
		//sh := md5.Sum(buf)
		//fmt.Println("packet i=", i, " size=", len(buf), " hash=", hex.EncodeToString(sh[:]))
		write_packet(buf, con)
		i++
	}
}

func main() {
	addr := ":8878"
	if len(os.Args) > 1 {
		addr = os.Args[1]
	}
	ls, err := net.Listen("tcp", addr)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer ls.Close()
	for {
		con, err := ls.Accept()
		if err != nil {
			fmt.Println(err)
			break
		}
		go echo_handler(con)
	}
}
