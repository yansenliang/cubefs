package rdmaInfo

import (
    "container/list"
    "context"
    "encoding/json"
    "fmt"
    "github.com/cubefs/cubefs/proto"
    "net"
    "strings"
    "sync"
    "time"
)
const (
    ConnPointTimeOutSec     = 3600
    GetPeerConfDurSec       = 1800
)

type ConnNode struct {
    NodeId       uint64
    RefCnt       uint64
    CheckTerm    int64
    LastUpdate   int64
    Addr         string
    PeerConf     *RDMAConfInfo
    RdmaEnable   bool
    TcpCnt       uint8
    RdmaCnt      uint8
}

type ConnManager struct {
    localConf   *RDMAConfInfo
    localId     uint64
    connMap     map[string]*ConnNode
    mutex       sync.RWMutex
    stopC       chan struct{}
    rdmaCnt     uint64
    tcpCnt      uint64
    calcTcpCnt  uint64
    calcRdmaCnt uint64
}

func (c *ConnNode) getPeerConfInfo(addr string) error {

    conn, err := net.Dial("tcp", addr)
    if err != nil {
        return err
    }

    req := proto.NewPacket(context.Background())
    req.Opcode = proto.OpGetRdmaInfo
    if err = req.WriteToConn(conn, 1); err != nil {
        return err
    }
    resp := req
    if err = resp.ReadFromConn(conn, 1); err != nil {
        return err
    }
    peerInfo := &RDMASysInfo{}
    if err = json.Unmarshal(resp.Data, peerInfo); err != nil {
        return err
    }
    c.PeerConf =   &peerInfo.Conf
    c.LastUpdate = c.CheckTerm
    return nil
}

func (c *ConnNode) CalcRdmaInfo(localConf *RDMAConfInfo) (rdmaEnable bool) {
    rdmaEnable = false
    //get peer info failed
    if c.PeerConf == nil {
        c.RdmaEnable = false
        return
    }

    if c.PeerConf.IsRDMAConfSame(localConf) {
        c.RdmaEnable = true
        rdmaEnable = true
    }

    return
}

func NewConnManager(localConf *RDMAConfInfo, id uint64) (*ConnManager, error) {
    if localConf == nil {
        return nil , fmt.Errorf("can not get local rdma info")
    }
    manger := &ConnManager{
        connMap:   make(map[string]*ConnNode),
        stopC:     make(chan struct{}),
        localConf: localConf,
        localId:   id,
    }

    if strings.Compare(DriverVersion, localConf.DriverVer) == 0 &&
        strings.Compare(localConf.DriverVer, DriverVersion) == 0 {
        warnF("Start manager schedule success")
        go manger.MainSchedule()
    }
    return manger, nil
}

func (m *ConnManager)Stop() {
    if m.stopC != nil {
        close(m.stopC)
        m.stopC = nil
    }
}

func (m *ConnManager)AddNode(id uint64, addr string, Term int64, tcpCnt, rdmaCnt uint8) (isRdmaErr bool, rdmaErrMsg string) {
    if id == m.localId {
        return
    }
    m.mutex.Lock()
    defer m.mutex.Unlock()
    conn , _ := m.connMap[addr]
    if conn == nil {
        conn = &ConnNode{NodeId: id, CheckTerm: Term, RefCnt: 1, TcpCnt: tcpCnt, RdmaCnt: rdmaCnt, Addr: addr}
        m.connMap[addr] = conn
        return
    }

    if rdmaCnt < conn.RdmaCnt {
        //rdma err, change to tcp mode
        rdmaErrMsg = fmt.Sprintf("rdma count[%d-->%d]", conn.RdmaCnt, rdmaCnt)
        isRdmaErr = true
    }

    if conn.CheckTerm != Term {
        //new term, init only first node
        conn.RefCnt    = 0
        conn.TcpCnt    = tcpCnt
        conn.RdmaCnt   = rdmaCnt
        m.calcRdmaCnt += uint64(rdmaCnt)
        m.calcTcpCnt  += uint64(tcpCnt)
    }
    conn.CheckTerm = Term
    conn.RefCnt += 1
    return
}

func (m *ConnManager)AddConnCnt(addr string, tcpCnt, rdmaCnt uint8) {
    m.mutex.Lock()
    defer m.mutex.Unlock()
    conn , _ := m.connMap[addr]
    if conn == nil {
        return
    }
    conn.TcpCnt   += tcpCnt
    conn.RdmaCnt  += rdmaCnt
    m.calcRdmaCnt += uint64(rdmaCnt)
    m.calcTcpCnt  += uint64(tcpCnt)
}

func (m *ConnManager)GetConnNode(addr string) *ConnNode {
    m.mutex.RLock()
    defer m.mutex.RUnlock()
    conn, _ := m.connMap[addr]
    return conn
}

func (m *ConnManager)GetAllConnNodes() []*ConnNode {
    m.mutex.RLock()
    defer m.mutex.RUnlock()
    nodeArray := make([]*ConnNode, 0)
    for _, conn := range m.connMap {
        nodeArray = append(nodeArray, conn)
    }
    return nodeArray
}

func (m *ConnManager) GetAllKey()  *list.List{
    keyList := list.New()
    _ = m.Range(func(addr string, conn *ConnNode) error {
        keyList.PushBack(addr)
        return nil
    })
    return keyList
}

func (m *ConnManager)Range(visit func(addr string, conn *ConnNode)(error)) (err error){
    m.mutex.RLock()
    defer m.mutex.RUnlock()
    for addr, conn := range m.connMap {
        if err = visit(addr, conn); err != nil {
            return
        }
    }
    return nil
}

func (m *ConnManager)FinishTerm() {
    m.rdmaCnt     = m.calcRdmaCnt
    m.tcpCnt      = m.calcTcpCnt
    m.calcTcpCnt  = 0
    m.calcRdmaCnt = 0
    return
}

func (m *ConnManager) CleanZombiePoint(keyList *list.List) {
    oldestActive := time.Now().Unix() - ConnPointTimeOutSec
    m.mutex.Lock()
    defer m.mutex.Unlock()
    for iter := keyList.Front(); iter != nil; {
        next := iter.Next()
        addr := iter.Value.(string)
        conn, _ := m.connMap[addr]
        if conn != nil && conn.CheckTerm < oldestActive{
            //timeout
            delete(m.connMap, addr)
            keyList.Remove(iter)
        }
        iter = next
    }
}

func (m *ConnManager) RefreshConfInfo(keyList *list.List) {
    for iter := keyList.Front(); iter != nil; iter = iter.Next(){
        conn := m.GetConnNode(iter.Value.(string))
        if conn == nil || conn.CheckTerm - conn.LastUpdate < GetPeerConfDurSec{
            if conn == nil {
                errorF("get conn node(%s) failed, already removed?", iter.Value.(string))
            }
            continue
        }
        if err := conn.getPeerConfInfo(iter.Value.(string)); err != nil {
            errorF("get peer(%s) conf info failed:%s", iter.Value.(string), err.Error())
            continue
        }

        if conn.CalcRdmaInfo(m.localConf) {
            warnF("peer(%s) rdma enable", iter.Value.(string))
        }
    }
}


func (m *ConnManager) MainSchedule() {
    ticker := time.NewTicker(time.Minute * 5)
    defer ticker.Stop()
    for ;; {
        select {
        case <-m.stopC:
            return
        case <-ticker.C:
            keyList := m.GetAllKey()
            infoF("conn manager schedule, need update %d conn nodes", keyList.Len())
            m.CleanZombiePoint(keyList)
            m.RefreshConfInfo(keyList)
        }
    }

}
