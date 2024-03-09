package main

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"log"
	"testing"
)

func TestAddFileEvent(t *testing.T) {
	l := AeLoop{FileEvents: map[int]*AeFileEvent{
		1: &AeFileEvent{},
	}}
	log.Println(l.getEpollMask(2))
}

func WriteProc(loop *AeLoop, fd int, extra any) {
	buf := extra.([]byte)
	n, err := Write(fd, buf)
	if err != nil {
		fmt.Printf("write err: %v\n", err)
		return
	}
	fmt.Printf("ae write %v bytes\n", n)
	loop.RemoveFileEvent(fd, AEWriteable)
}

func ReadProc(loop *AeLoop, fd int, extra any) {
	buf := make([]byte, 11)
	n, err := Read(fd, buf)
	if err != nil {
		fmt.Printf("read err: %v\n", err)
		return
	}
	fmt.Printf("ae read %v bytes\n", n)
	loop.AddFileEvent(fd, AEWriteable, WriteProc, buf)
}

func AcceptProc(loop *AeLoop, fd int, extra any) {
	cfd, err := Accept(fd)
	if err != nil {
		fmt.Printf("accept err: %v\n", err)
		return
	}
	loop.AddFileEvent(cfd, AEReadable, ReadProc, nil)
}

func OnceProc(loop *AeLoop, id int, extra interface{}) {
	t := extra.(*testing.T)
	assert.Equal(t, 1, id)
	fmt.Printf("time event %v done\n", id)
}

func NormalProc(loop *AeLoop, id int, extra any) {
	end := extra.(chan struct{})
	fmt.Printf("time event %v done\n", id)
	end <- struct{}{}
}

func TestAe(t *testing.T) {
	loop, err := AeLoopCreate()
	assert.Nil(t, err)
	sfd, err := TcpServer(6666)
	loop.AddFileEvent(sfd, AEReadable, AcceptProc, nil)
	go loop.AEMain()
	host := [4]byte{0, 0, 0, 0}
	cfd, err := Connect(host, 6666)
	assert.Nil(t, err)
	msg := "hello world"
	n, err := Write(cfd, []byte(msg))
	assert.Nil(t, err)
	assert.Equal(t, 11, n)
	buf := make([]byte, 11)
	n, err = Read(cfd, buf)
	assert.Nil(t, err)
	assert.Equal(t, 11, n)
	assert.Equal(t, msg, string(buf))

	loop.AddTimeEvent(AEOnce, 100, OnceProc, t)
	end := make(chan struct{}, 2)
	loop.AddTimeEvent(AENormal, 10, NormalProc, end)
	<-end
	<-end
	loop.stop = true
}
