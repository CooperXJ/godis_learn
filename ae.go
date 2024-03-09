package main

import (
	"log"
	"time"

	"golang.org/x/sys/unix"
)

type FeType int

const (
	AEReadable  FeType = 1
	AEWriteable FeType = 2
)

type TeType int

const (
	AENormal TeType = 1 //常规定时任务（循环的）
	AEOnce   TeType = 2 // 一次性的定时任务
)

type AeFileEvent struct {
	fd    int
	mask  FeType
	proc  FileProc
	extra any
}

type AeTimeEvent struct {
	id       int
	mask     TeType
	when     int64 //何时触发 ms
	interval int64 //触发间隔 ms
	proc     TimeProc
	extra    any
	next     *AeTimeEvent
}

type AeLoop struct {
	FileEvents      map[int]*AeFileEvent //原来redis中是双向list结构
	TimeEvents      *AeTimeEvent
	fileEventFd     int
	timeEventNextID int
	stop            bool
}

type FileProc func(loop *AeLoop, fd int, extra any)
type TimeProc func(loop *AeLoop, fd int, extra any)

// 将fileEvent的事件映射为epoll事件  unix.EPOLLIN 可读事件  unix.EPOLLOUT 可写事件
var fe2ep [3]uint32 = [3]uint32{0, unix.EPOLLIN, unix.EPOLLOUT}

// 获取fileEvent事件的key，一个fd和feType确定一个key
func getFeKey(fd int, mask FeType) int {
	if mask == AEReadable {
		return fd
	}

	return fd * -1
}

// 检查是否已经订阅了相同类型的事件
/*
	https://www.cnblogs.com/zhengerduosi/p/10178530.html
	因为往epoll中增加事件的时候，有两种方式，分别为 add 和 modify。
	当我们将某个fd添加read event的时候，如果该fd的write event已经被添加到了epoll中，那么我们就不能继续add了，只能modify，所以这里要先判断一下write event的状态

*/
func (loop *AeLoop) getEpollMask(fd int) uint32 {
	var ev uint32
	if loop.FileEvents[getFeKey(fd, AEReadable)] != nil {
		ev |= fe2ep[AEReadable]
	}
	if loop.FileEvents[getFeKey(fd, AEWriteable)] != nil {
		ev |= fe2ep[AEWriteable]
	}
	return ev
}

func (loop *AeLoop) AddFileEvent(fd int, mask FeType, proc FileProc, extra any) {
	// 只要fd存在且当前的mask与之前的相同，那么就返回 因为相当于当前的客户端已经注册过了
	ev := loop.getEpollMask(fd)
	if ev&fe2ep[mask] != 0 {
		// event is already registered
		return
	}

	op := unix.EPOLL_CTL_ADD
	if ev != 0 {
		op = unix.EPOLL_CTL_MOD
	}

	ev |= fe2ep[mask] //将当前事件增加到已有的事件中,假设一个事件里面有readable了，那么在基础上可以增加一个writeable
	err := unix.EpollCtl(loop.fileEventFd, op, fd, &unix.EpollEvent{Fd: int32(fd), Events: ev})
	if err != nil {
		log.Printf("epoll ctr err: %v\n", err)
		return
	}

	var fe AeFileEvent
	fe.fd = fd
	fe.mask = mask
	fe.proc = proc
	fe.extra = extra
	loop.FileEvents[getFeKey(fd, mask)] = &fe
	log.Printf("ae add file event fd:%v, mask:%v\n", fd, mask)
}

func (loop *AeLoop) RemoveFileEvent(fd int, mask FeType) {
	op := unix.EPOLL_CTL_DEL
	ev := loop.getEpollMask(fd)
	ev &= ^fe2ep[mask] // 将当前的事件删除掉
	if ev != 0 {
		op = unix.EPOLL_CTL_MOD
	}

	err := unix.EpollCtl(loop.fileEventFd, op, fd, &unix.EpollEvent{Fd: int32(fd), Events: ev})
	if err != nil {
		log.Printf("epoll del err: %v\n", err)
		return
	}
	loop.FileEvents[getFeKey(fd, mask)] = nil
	log.Printf("ae remove file event fd:%v, mask:%v\n", fd, mask)
}

func GetMsTime() int64 {
	return time.Now().UnixNano() / 1e6
}

func (loop *AeLoop) AddTimeEvent(mask TeType, interval int64, proc TimeProc, extra any) int {
	id := loop.timeEventNextID
	loop.timeEventNextID++
	var te AeTimeEvent
	te.id = id
	te.mask = mask
	te.when = GetMsTime() + interval
	te.proc = proc
	te.extra = extra
	te.next = loop.TimeEvents //头插法
	loop.TimeEvents = &te
	return id
}

func (loop *AeLoop) RemoveTimeEvent(id int) {
	p := loop.TimeEvents
	var pre *AeTimeEvent
	for p != nil {
		if p.id == id {
			if pre == nil {
				loop.TimeEvents = p.next
			} else {
				pre.next = p.next
			}
			p.next = nil
			break
		}
		pre = p
		p = p.next
	}
}

// 获取time event触发的最近时间
func (loop *AeLoop) nearestTime() int64 {
	// 限定了一个最小值，防止没有time event或者time event最近的触发时间距离现在太久
	var nearest int64 = GetMsTime() + 1000
	p := loop.TimeEvents
	for p != nil {
		if p.when < nearest {
			nearest = p.when
		}
		p = p.next
	}
	return nearest
}

func (loop *AeLoop) AeProcess(tes []*AeTimeEvent, fes []*AeFileEvent) {
	for _, te := range tes {
		te.proc(loop, te.id, te.extra)
		if te.mask == AEOnce {
			loop.RemoveTimeEvent(te.id)
		} else {
			te.when = GetMsTime() + te.interval
		}
	}
	if len(fes) > 0 {
		log.Println("ae is processing file events")
		for _, fe := range fes {
			fe.proc(loop, fe.fd, fe.extra)
		}
	}
}

// AeWait  获取file events和time events
func (loop *AeLoop) AeWait() (tes []*AeTimeEvent, fes []*AeFileEvent) {
	timeout := loop.nearestTime() - GetMsTime()
	if timeout < 10 {
		timeout = 10 //最少需要等待10ms
	}

	//采集timeout时间内的所有file event事件
	var events [128]unix.EpollEvent
	n, err := unix.EpollWait(loop.fileEventFd, events[:], int(timeout))
	if err != nil {
		log.Printf("epoll wait warnning: %v\n", err)
	}

	if n > 0 {
		log.Printf("ae get %v epoll events\n", n)
	}

	// collect file events
	for i := 0; i < n; i++ {
		if events[i].Events&unix.EPOLLIN != 0 {
			// 获取注册时间中的读事件
			fe := loop.FileEvents[getFeKey(int(events[i].Fd), AEReadable)]
			if fe != nil {
				fes = append(fes, fe)
			}
		}

		if events[i].Events&unix.EPOLLOUT != 0 {
			fe := loop.FileEvents[getFeKey(int(events[i].Fd), AEWriteable)]
			if fe != nil {
				fes = append(fes, fe)
			}
		}
	}

	// collect time events
	now := GetMsTime()
	p := loop.TimeEvents
	for p != nil {
		if p.when <= now {
			tes = append(tes, p)
		}
		p = p.next
	}

	return
}

func AeLoopCreate() (*AeLoop, error) {
	epollFd, err := unix.EpollCreate1(0)
	if err != nil {
		return nil, err
	}

	return &AeLoop{
		FileEvents:      make(map[int]*AeFileEvent),
		fileEventFd:     epollFd,
		timeEventNextID: 1,
		stop:            false,
	}, nil
}

// 事件主函数
func (loop *AeLoop) AEMain() {
	for !loop.stop {
		// 获取当前已经ready的事件
		tes, fes := loop.AeWait()
		// 处理事件
		loop.AeProcess(tes, fes)
	}
}
