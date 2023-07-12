package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"github.com/cbnet/cbrdma"
	"io/ioutil"
	"net"
	"net/http"
	_ "net/http/pprof"
	"strconv"

	"github.com/tiglabs/raft"
	"github.com/tiglabs/raft/proto"
)

var confFile = flag.String("conf", "", "config file path")

type raftServerConfig struct {
	ID          uint64 `json:"id"`
	Addr        string `json:"addr"`
	GroupNum    int    `json:"groupNum"`
	Peers       []peer `json:"peers"`
	HeartPort   string `json:"heartPort"`
	ReplicaPort string `json:"replicaPort"`
	Listen      string `json:"listen"`
	WalDir      string `json:"walDir"`
	DiskNum     int    `json:"diskNum"`
	LogDir      string `json:"logDir"`
	LogLevel    string `json:"level"`
	StoreType   int    `json:"storeType"`
	NumaNode    int    `json:"numaNode"`
	WorkNum     int    `json:"workNum"`
	ReplicaType int    `json:"replicaType"`
}

type peer struct {
	ID   uint64 `json:"id"`
	Addr string `json:"addr"`
}

const (
	Buffer2 = iota
	Buffer4
	Buffer8
	Buffer16
	Buffer32
	Buffer64
	Buffer128
	Buffer256
	Buffer512
	Buffer1K
	Buffer2K
	Buffer4K
	Buffer8K
	Buffer16K
	Buffer32K
	Buffer64K
	Buffer128K
	Buffer256K
	Buffer512K
	Buffer1M
	Buffer2M
	Buffer4M
	BufferMAx
)

var gBuffer [][]byte

func init_global_buffer() {
	gBuffer = make([][]byte, BufferMAx)
	for i := Buffer2; i < BufferMAx; i++ {
		length := 1 << (1 + i)
		gBuffer[i] = make([]byte, length)
		for j := 0; j < length; j++ {
			gBuffer[i][j] = 1
		}
	}
}

func main() {
	init_global_buffer()
	flag.Parse()
	var err error
	profPort := 9999
	var profNetListener net.Listener = nil
	if profNetListener, err = net.Listen("tcp", fmt.Sprintf(":%v", profPort)); err != nil {
		output(fmt.Sprintf("Fatal: listen prof port %v failed: %v", profPort, err))
		return
	}
	// 在prof端口监听上启动http API.
	go func() {
		_ = http.Serve(profNetListener, http.DefaultServeMux)
	}()
	rConf := &raftServerConfig{}
	rConf.parseConfig(*confFile)
	logLevel = rConf.LogLevel
	storageType = rConf.StoreType
	walDir = rConf.WalDir
	diskNum = rConf.DiskNum
	dataType = 1
	logHandler := initRaftLog(rConf.LogDir)

	resolver = initNodeManager()
	peers := make([]proto.Peer, 0)
	for _, p := range rConf.Peers {
		peer := proto.Peer{ID: p.ID}
		peers = append(peers, peer)
		resolver.addNodeAddr(p, rConf.ReplicaPort, rConf.HeartPort)
	}
	logL, _ := strconv.Atoi(rConf.LogLevel)
	if _, err = cbrdma.InitNetEnv(rConf.WorkNum, logL, rConf.NumaNode, rConf.Addr, logHandler); err != nil {
		logHandler.Error("init rdma env failed:%s", err.Error())
		return
	}

	output(fmt.Sprintf("loglevel[%v], storageType[%v], walDir[%v], diskNum[%v], dataType[%v]", logLevel, storageType, walDir, diskNum, dataType))
	raftConfig := &raft.RaftConfig{Peers: peers, Leader: 0, Term: 0, Mode: raft.DefaultMode}
	server := createRaftServer(rConf.ID, true, false, rConf.GroupNum, raftConfig, raft.SocketType(rConf.ReplicaType))
	server.conf = rConf
	server.startHttpService(rConf.Addr, rConf.Listen)
}

func (rConf *raftServerConfig) parseConfig(filePath string) {
	bytes, err := ioutil.ReadFile(filePath)
	if err != nil {
		panic(err)
	}
	err = json.Unmarshal(bytes, rConf)
	if err != nil {
		panic(err)
	}
}
