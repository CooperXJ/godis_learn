package main

import (
	"errors"
	"fmt"
	"hash/fnv"
	"log"
	"strconv"
	"strings"
	"time"
)

type CmdType int

const (
	CommonUnkonw CmdType = 0x00
	CommonInlie  CmdType = 0x01
	CommonBulk   CmdType = 0x02
)

const (
	GodisIOBuf     int = 1024 * 16
	GodisMaxBulk   int = 1024 * 4
	GodisMaxInline int = 1024 * 4
)

type GodisServer struct {
	fd      int
	port    int
	db      *GodisDB
	clients map[int]*GodisClient
	aeLoop  *AeLoop
}

var server GodisServer

func expireIfNeed(key *Gobj) {
	entry := server.db.expire.Find(key)
	if entry == nil {
		return
	}

	when := entry.Val.IntVal()
	if when > GetMsTime() {
		return
	}
	server.db.expire.Delete(key)
	server.db.data.Delete(key)
}

func findKeyRead(key *Gobj) *Gobj {
	expireIfNeed(key)
	return server.db.data.Get(key)
}

func getCommand(c *GodisClient) {
	key := c.args[1]
	val := findKeyRead(key)
	if val == nil {
		c.AddReplyStr("$-1\r\n")
	} else if val.Type_ != GSTR {
		c.AddReplyStr("-ERR: wrong type\r\n")
	} else {
		str := val.StrVal()
		c.AddReplyStr(fmt.Sprintf("$%d%v\r\n", len(str), str))
	}
}

func setCommand(c *GodisClient) {
	key := c.args[1]
	val := c.args[2]
	if val.Type_ != GSTR {
		c.AddReplyStr("-ERR: wrong type\r\n")
	}
	server.db.data.Set(key, val)
	server.db.expire.Delete(key)
	c.AddReplyStr("+OK\r\n")
}

func expireCommand(client *GodisClient) {
	key := client.args[1]
	val := client.args[2]
	if val.Type_ != GSTR {
		client.AddReplyStr("-ERR: wrong type\r\n")
	}
	expire := GetMsTime() + (val.IntVal() * 1000)
	expireObj := CreateFromInt(expire)
	server.db.expire.Set(key, expireObj)
	expireObj.DecrRefCount()
	client.AddReplyStr("+OK\r\n")
}

var cmdTable []GodisCommand = []GodisCommand{
	{"get", getCommand, 2},
	{"set", setCommand, 3},
	{"expire", expireCommand, 3},
}

type GodisDB struct {
	data   *Dict
	expire *Dict
}

type GodisClient struct {
	fd       int
	db       *GodisDB
	args     []*Gobj
	reply    *List
	sentLen  int    //防止一个reply发送的内容过多，记录下当前已经发送的内容
	queryBuf []byte // 客户端命令缓冲区
	queryLen int    // 客户端命令缓冲区中已读位置的index
	cmdTy    CmdType
	bulkNum  int
	bulkLen  int
}

type CommandProc func(c *GodisClient)

type GodisCommand struct {
	name  string
	proc  CommandProc
	arity int
}

func ReadQueryFromClient(loop *AeLoop, fd int, extra any) {
	client := extra.(*GodisClient)
	// 如果客户端的未读命令长度>客户端的缓冲区 则扩容
	if len(client.queryBuf)-client.queryLen < GodisMaxBulk {
		client.queryBuf = append(client.queryBuf, make([]byte, GodisMaxBulk)...)
	}

	// 将socket中的数据读取到缓冲区中
	n, err := Read(fd, client.queryBuf[client.queryLen:])
	if err != nil {
		// 当前客户端与redis命令不兼容，因此直
		//接释放掉该客户端
		log.Printf("client %v read err: %v\n", fd, err)
		freeClient(client)
		return
	}

	client.queryLen += n // 缓冲区中的index移动
	log.Printf("read %v bytes from client:%v\n", n, client.fd)
	log.Printf("ReadQueryFromClient, queryBuf : %v\n", string(client.queryBuf))
	err = ProcessQueryBuf(client)
	if err != nil {
		log.Printf("process query buf err: %v\n", err)
		freeClient(client)
		return
	}
}

func ProcessQueryBuf(client *GodisClient) error {
	for client.queryLen > 0 {
		if client.cmdTy == CommonUnkonw {
			if client.queryBuf[0] == '*' {
				client.cmdTy = CommonBulk
			} else {
				client.cmdTy = CommonInlie
			}
		}

		var ok bool
		var err error
		if client.cmdTy == CommonInlie {
			// inline命令是 以空格分隔命令
			ok, err = handleInlineBuf(client)
		} else if client.cmdTy == CommonBulk {
			// 处理multiBulk命令  该中命令是string类型的数组 第一个参数表示数组的长度
			ok, err = handleBulkBuf(client)
		} else {
			return errors.New("unknown godis command type")
		}

		if err != nil {
			return err
		}

		if ok {
			if len(client.args) == 0 {
				resetClient(client)
			} else {
				ProcessCommand(client)
			}
		} else {
			// 一次未读完命令
			break
		}
	}

	return nil
}

func handleInlineBuf(client *GodisClient) (bool, error) {
	index, err := client.findLineQuery()
	if index < 0 {
		return false, err
	}

	subs := strings.Split(string(client.queryBuf[:index]), " ")
	client.queryBuf = client.queryBuf[index+1:]
	client.queryLen -= index + 1 //跳过\r\n
	client.args = make([]*Gobj, len(subs))
	for i, v := range subs {
		client.args[i] = CreateObject(GSTR, v)
	}

	return true, nil
}

// 找到inline命令
func (client *GodisClient) findLineQuery() (int, error) {
	index := strings.Index(string(client.queryBuf[:client.queryLen]), "\n")
	if index < 0 && client.queryLen > GodisMaxInline {
		// 不允许传入这么大的inline命令
		return index, errors.New("too big inline cmd")
	}
	return index, nil
}

func handleBulkBuf(client *GodisClient) (bool, error) {
	// 如果bulkNum为0 则说明命令还未开始处理multiBulk命令 先要获取到数组长度
	if client.bulkNum == 0 {
		index, err := client.findLineQuery()
		if index < 0 {
			return false, err
		}

		bnum, err := client.getNumInQuery(1, index)
		if err != nil {
			return false, err
		}

		if bnum == 0 {
			return true, nil
		}
		client.bulkNum = bnum
		client.args = make([]*Gobj, bnum)
	}

	// read every bulk string
	for client.bulkNum > 0 {
		if client.bulkLen == 0 {
			index, err := client.findLineQuery()
			if index < 0 {
				return false, err
			}

			// 每个元素的开头是$后面跟随一个数字表示当前字符元素的长度
			if client.queryBuf[0] != '$' {
				return false, errors.New("expect $ for bulk length")
			}

			blen, err := client.getNumInQuery(1, index)
			if err != nil || blen == 0 {
				return false, err
			}
			if blen > GodisMaxBulk {
				return false, errors.New("too big bulk")
			}
			client.bulkLen = blen
		}

		// read bulk string
		if client.queryLen < client.bulkLen+1 {
			return false, nil
		}

		index := client.bulkLen
		if client.queryBuf[index] != '\r' || client.queryBuf[index+1] != '\n' {
			return false, errors.New("expect CRLF for bulk end")
		}

		client.args[len(client.args)-client.bulkNum] = CreateObject(GSTR, string(client.queryBuf[:index]))
		client.queryBuf = client.queryBuf[index+1:]
		client.queryLen -= index + 1
		client.bulkLen = 0
		client.bulkNum -= 1
	}

	return true, nil
}

// 获取multiBulk类型命令的数组长度
func (client *GodisClient) getNumInQuery(s, e int) (int, error) {
	num, err := strconv.Atoi(string(client.queryBuf[s:e]))
	client.queryBuf = client.queryBuf[e+1:]
	client.queryLen -= e + 1
	return num, err
}

func (client *GodisClient) AddReplyStr(s string) {
	o := CreateObject(GSTR, s)
	client.AddReply(o)
	o.DecrRefCount()
}

func (client *GodisClient) AddReply(o *Gobj) {
	client.reply.Append(o)
	o.IncrRefCount()
	server.aeLoop.AddFileEvent(client.fd, AEWriteable, SendReplyToClient, client)
}

func ProcessCommand(c *GodisClient) {
	cmdStr := c.args[0].StrVal()
	log.Printf("process command: %v\n", cmdStr)

	if cmdStr == "quit" {
		freeClient(c)
		return
	}

	cmd := lookupCommand(cmdStr)
	if cmd == nil {
		c.AddReplyStr("-ERR: unknow command\r\n")
		resetClient(c)
		return
	} else if cmd.arity != len(c.args) {
		c.AddReplyStr("-ERR: wrong number of args\r\n")
		resetClient(c)
		return
	}

	cmd.proc(c)
	resetClient(c)
}

func lookupCommand(cmdStr string) *GodisCommand {
	for _, c := range cmdTable {
		if c.name == cmdStr {
			return &c
		}
	}
	return nil
}

func freeClient(client *GodisClient) {
	freeArgs(client)
	delete(server.clients, client.fd)
	server.aeLoop.RemoveFileEvent(client.fd, AEReadable)
	server.aeLoop.RemoveFileEvent(client.fd, AEWriteable)
	freeReplyList(client)
	Close(client.fd)
}

func resetClient(client *GodisClient) {
	freeArgs(client)
	client.cmdTy = CommonUnkonw
	client.bulkNum = 0
	client.bulkLen = 0
}

func freeArgs(client *GodisClient) {
	for _, v := range client.args {
		v.DecrRefCount()
	}
}

func freeReplyList(client *GodisClient) {
	for client.reply.length != 0 {
		n := client.reply.head
		client.reply.DelNode(n)
		n.Val.DecrRefCount()
	}
}

func SendReplyToClient(loop *AeLoop, fd int, extra any) {
	client := extra.(*GodisClient)
	log.Printf("SendReplyToClient, reply len:%v\n", client.reply.Length())
	for client.reply.Length() > 0 {
		rep := client.reply.First()
		buf := []byte(rep.Val.StrVal())
		bufLen := len(buf)
		if client.sentLen < bufLen {
			n, err := Write(fd, buf[client.sentLen:])
			if err != nil {
				log.Printf("send reply err: %v\n", err)
				freeClient(client)
				return
			}
			client.sentLen += n
			log.Printf("send %v bytes to client:%v\n", n, client.fd)
			if client.sentLen == bufLen {
				client.reply.DelNode(rep)
				rep.Val.DecrRefCount()
				client.sentLen = 0
			} else {
				break
			}
		}
	}

	if client.reply.Length() == 0 {
		client.sentLen = 0
		loop.RemoveFileEvent(fd, AEWriteable)
	}
}

func initServer(config *Config) error {
	server.port = config.Port
	server.clients = make(map[int]*GodisClient)
	server.db = &GodisDB{
		data:   DictCreate(DictType{HashFunc: GStrHash, EqualFunc: GStrEqual}),
		expire: DictCreate(DictType{HashFunc: GStrHash, EqualFunc: GStrEqual}),
	}

	var err error
	if server.aeLoop, err = AeLoopCreate(); err != nil {
		return err
	}

	server.fd, err = TcpServer(server.port)
	return err
}

func GStrEqual(a, b *Gobj) bool {
	if a.Type_ != GSTR || b.Type_ != GSTR {
		return false
	}

	return a.StrVal() == b.StrVal()
}

func GStrHash(key *Gobj) int64 {
	if key.Type_ != GSTR {
		return 0
	}
	hash := fnv.New64()
	hash.Write([]byte(key.StrVal()))
	return int64(hash.Sum64())
}

const EXPIRE_CHECK_COUNT int = 100

func ServerCron(loop *AeLoop, fd int, extra any) {
	for i := 0; i < EXPIRE_CHECK_COUNT; i++ {
		entry := server.db.expire.RandomGet()
		if entry == nil {
			break
		}
		if entry.Val.IntVal() < time.Now().Unix() {
			server.db.data.Delete(entry.Key)
			server.db.expire.Delete(entry.Key)
		}
	}
}

func AcceptHandler(loop *AeLoop, fd int, extra any) {
	cfd, err := Accept(fd)
	if err != nil {
		log.Printf("accept err: %v\n", err)
		return
	}

	client := CreateClient(cfd)
	server.clients[cfd] = client
	server.aeLoop.AddFileEvent(cfd, AEReadable, ReadQueryFromClient, client)
	log.Printf("accept client, fd: %v\n", cfd)
}

func CreateClient(fd int) *GodisClient {
	var client GodisClient
	client.fd = fd
	client.db = server.db
	client.queryBuf = make([]byte, GodisIOBuf)
	client.reply = ListCreate(ListType{EqualFunc: GStrEqual})
	return &client
}

func main() {
	path := "./config.json" //os.Args[1]
	config, err := LoadConfig(path)
	if err != nil {
		log.Printf("config error: %v\n", err)
	}

	err = initServer(config)
	if err != nil {
		log.Printf("init server error: %v\n", err)
	}
	server.aeLoop.AddFileEvent(server.fd, AEReadable, AcceptHandler, nil)
	server.aeLoop.AddTimeEvent(AENormal, 100, ServerCron, nil)
	log.Println("godis server is up.")
	server.aeLoop.AEMain()
}
