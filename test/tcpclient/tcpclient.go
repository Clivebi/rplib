package main

import (
	"bytes"
	"crypto/md5"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net"
	"os"
	"strconv"
)

func rand_buffer(b []byte, max_size int) int {
	size := rand.Int() % max_size
	if size < 8 {
		size = 8
	}
	for i := 0; i < size; i++ {
		b[i] = byte(rand.Int() & 0xFF)
	}
	return size
}

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

func main() {
	addr := "127.0.0.1:8878"
	if len(os.Args) > 1 {
		addr = os.Args[1]
	}
	con, err := net.Dial("tcp", addr)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer con.Close()
	buf := make([]byte, 32*1024)
	for i := 0; i < 1000; i++ {
		size := rand_buffer(buf, 32*1024)
		sh := md5.Sum(buf[:size])
		fmt.Println("write packet size=", size, "hash=", hex.EncodeToString(sh[:]))
		err := write_packet(buf[:size], con)
		if err != nil {
			fmt.Println(err, i)
			break
		}
		recvBuf, err := read_packet(con)
		if err != nil {
			fmt.Println(err, i)
			break
		}
		if len(recvBuf) != size {
			fmt.Println("test size not equal ", i, size, "<==>", len(recvBuf))
			break
		}
		sh = md5.Sum(recvBuf)
		if !bytes.Equal(buf[:size], recvBuf) {
			fmt.Println("content not equal", i)
			fmt.Println("read packet size=", size, "hash=", hex.EncodeToString(sh[:]))
			break
		}
	}
	fmt.Println("test complete")
}
