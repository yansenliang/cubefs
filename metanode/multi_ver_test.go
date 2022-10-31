package metanode

import (
	"fmt"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/config"
	"github.com/cubefs/cubefs/util/log"
	"github.com/stretchr/testify/assert"
	"math"
	"os"
	"sync"
	"testing"
	"time"
)

var partitionId uint64 = 10
var ModeDirType uint32 = 2147484141
var ModFileType uint32 = 420
var manager = &metadataManager{}
var mp *metaPartition

//PartitionId   uint64              `json:"partition_id"`
//VolName       string              `json:"vol_name"`
//PartitionType int                 `json:"partition_type"`
var metaConf = &MetaPartitionConfig{
	PartitionId:   10001,
	VolName:       "testVol",
	PartitionType: proto.VolumeTypeHot,
}

const (
	ConfigKeyLogDir   = "logDir"
	ConfigKeyLogLevel = "logLevel"
)

var cfgJSON = `{
		"role": "meta",
		"logDir": "/tmp/chubaofs/Logs",
		"logLevel":"debug",
		"walDir":"/tmp/chubaofs/raft",
		"clusterName":"chubaofs"
	}`
var tlog *testing.T

func tLogf(format string, args ...interface{}) {
	tlog.Log(fmt.Sprintf(format, args...))
}

func newPartition(conf *MetaPartitionConfig, manager *metadataManager) (mp *metaPartition) {
	mp = &metaPartition{
		config:        conf,
		dentryTree:    NewBtree(),
		inodeTree:     NewBtree(),
		extendTree:    NewBtree(),
		multipartTree: NewBtree(),
		stopC:         make(chan bool),
		storeChan:     make(chan *storeMsg, 100),
		freeList:      newFreeList(),
		extDelCh:      make(chan []proto.ExtentKey, defaultDelExtentsCnt),
		extReset:      make(chan struct{}),
		vol:           NewVol(),
		manager:       manager,
		verSeq:        conf.VerSeq,
	}
	mp.config.Cursor = 0
	mp.config.End = 100000
	return mp
}

func init() {
	cfg := config.LoadConfigString(cfgJSON)

	logDir := cfg.GetString(ConfigKeyLogDir)
	os.RemoveAll(logDir)

	if _, err := log.InitLog(logDir, "metanode", log.DebugLevel, nil); err != nil {
		fmt.Println("Fatal: failed to start the chubaofs daemon - ", err)
		return
	}
	log.LogDebugf("action start")
	return
}

func initMp(t *testing.T) {
	tlog = t
	mp = newPartition(metaConf, manager)
	mp.multiVersionList = &proto.VolVersionInfoList{}
	ino := testCreateInode(nil, DirModeType)
	t.Logf("cursor %v create ino %v", mp.config.Cursor, ino)
	mp.config.Cursor = 1000
}

func buildExtentKey(seq uint64, foffset uint64, extid uint64, exteoffset uint64, size uint32) proto.ExtentKey {
	return proto.ExtentKey{
		FileOffset:   foffset,
		PartitionId:  partitionId,
		ExtentId:     extid,
		ExtentOffset: exteoffset, // offset in extent like tiny extent offset large than 0,normal is 0
		Size:         size,       // extent real size?
		VerSeq:       seq,
	}
}

func buildExtents(verSeq uint64, startFileOff uint64, extid uint64) (exts []proto.ExtentKey) {
	var (
		i      uint64
		extOff uint64 = 0
	)
	for ; i < 1; i++ {
		ext1 := buildExtentKey(verSeq, startFileOff+i*1000, extid, extOff+i*1000, 1000)
		exts = append(exts, ext1)
	}

	return
}

func isExtEqual(ek1 proto.ExtentKey, ek2 proto.ExtentKey) bool {
	return ek1.ExtentId == ek2.ExtentId &&
		ek1.FileOffset == ek2.FileOffset &&
		ek1.Size == ek2.Size &&
		ek1.ExtentOffset == ek2.ExtentOffset &&
		ek1.PartitionId == ek2.PartitionId
}

func isDentryEqual(den1 *proto.Dentry, den2 *Dentry) bool {
	return den1.Inode == den2.Inode &&
		den1.Name == den2.Name &&
		den1.Type == den2.Type
}

func checkOffSetInSequnce(t *testing.T, eks []proto.ExtentKey) bool {
	if len(eks) < 2 {
		return true
	}

	var (
		lastFileOff uint64 = eks[0].FileOffset
		lastSize    uint32 = eks[0].Size
	)

	for idx, ext := range eks[1:] {
		//t.Logf("idx:%v ext:%v, lastFileOff %v, lastSize %v", idx, ext, lastFileOff, lastSize)
		if ext.FileOffset != lastFileOff+uint64(lastSize) {
			t.Errorf("checkOffSetInSequnce not equal idx %v %v:(%v+%v) eks{%v}", idx, ext.FileOffset, lastFileOff, lastSize, eks)
			return false
		}
		lastFileOff = ext.FileOffset
		lastSize = ext.Size
	}
	return true
}

func testGetExtList(t *testing.T, ino *Inode, verRead uint64) (resp *proto.GetExtentsResponse) {
	reqExtList := &proto.GetExtentsRequest{
		VolName:     metaConf.VolName,
		PartitionID: partitionId,
		Inode:       ino.Inode,
	}
	packet := &Packet{}
	reqExtList.VerSeq = verRead
	assert.True(t, nil == mp.ExtentsList(reqExtList, packet))
	resp = &proto.GetExtentsResponse{}

	assert.True(t, nil == packet.UnmarshalData(resp))
	t.Logf("testGetExtList.resp %v", resp)
	assert.True(t, packet.ResultCode == proto.OpOk)
	assert.True(t, checkOffSetInSequnce(t, resp.Extents))
	return
}

func testCheckExtList(t *testing.T, ino *Inode, seqArr []uint64) bool {
	reqExtList := &proto.GetExtentsRequest{
		VolName:     metaConf.VolName,
		PartitionID: partitionId,
		Inode:       ino.Inode,
	}

	for idx, verRead := range seqArr {
		t.Logf("check extlist index %v ver %v", idx, verRead)
		reqExtList.VerSeq = verRead
		getExtRsp := testGetExtList(t, ino, verRead)
		t.Logf("check extlist rsp %v size %v,%v", getExtRsp, getExtRsp.Size, ino.Size)
		assert.True(t, getExtRsp.Size == uint64(1000*(idx+1)))
		if getExtRsp.Size != uint64(1000*(idx+1)) {
			panic(nil)
		}
	}
	return true
}

func testCreateInode(t *testing.T, mode uint32) *Inode {
	inoID, _ := mp.nextInodeID()
	if t != nil {
		t.Logf("inode id:%v", inoID)
	}

	ino := NewInode(inoID, mode)
	ino.verSeq = mp.verSeq
	if t != nil {
		t.Logf("testCreateInode ino %v", ino)
	}

	mp.fsmCreateInode(ino)
	return ino
}

func testCreateDentry(t *testing.T, parentId uint64, inodeId uint64, name string, mod uint32) *Dentry {

	dentry := &Dentry{
		ParentId: parentId,
		Name:     name,
		Inode:    inodeId,
		Type:     mod,
		VerSeq:   mp.verSeq,
	}
	t.Logf("createDentry dentry %v", dentry)
	ret := mp.fsmCreateDentry(dentry, false)
	assert.True(t, proto.OpOk == ret)
	if ret != proto.OpOk {
		panic(nil)
	}
	return dentry
}

func TestEkMarshal(t *testing.T) {
	log.LogDebugf("TestEkMarshal")
	initMp(t)
	// inodeID uint64, ekRef *sync.Map, ek *proto.ExtentKey
	ino := testCreateInode(t, FileModeType)
	ino.ekRefMap = new(sync.Map)
	ek := &proto.ExtentKey{
		PartitionId: 10,
		ExtentId:    20,
		VerSeq:      123444,
	}
	id := storeEkSplit(0, ino.ekRefMap, ek)
	dpID, extID := proto.ParseFromId(id)
	assert.True(t, dpID == ek.PartitionId)
	assert.True(t, extID == ek.ExtentId)

	ok, _ := ino.DecSplitEk(ek)
	assert.True(t, ok == true)
	log.LogDebugf("TestEkMarshal close")
}

func initVer() {
	verInfo := &proto.VolVersionInfo{
		Ver:    0,
		Ctime:  time.Unix(0, 0),
		Status: proto.VersionNormal,
	}
	mp.multiVersionList.VerList = append(mp.multiVersionList.VerList, verInfo)
}

func testGetSplitSize(t *testing.T, ino *Inode) (cnt int32) {
	if nil == mp.inodeTree.Get(ino) {
		return
	}
	ino.ekRefMap.Range(func(key, value interface{}) bool {
		dpID, extID := proto.ParseFromId(key.(uint64))
		log.LogDebugf("id:[%v],key %v (dpId-%v|extId-%v) refCnt %v", cnt, key, dpID, extID, value.(uint32))
		cnt++
		return true
	})
	return
}

func testGetEkRefCnt(t *testing.T, ino *Inode, ek *proto.ExtentKey) (cnt uint32) {
	id := ek.GenerateId()
	var (
		val interface{}
		ok  bool
	)
	if nil == mp.inodeTree.Get(ino) {
		t.Logf("testGetEkRefCnt inode %v ek %v not found", ino, ek)
		return
	}
	if val, ok = ino.ekRefMap.Load(id); !ok {
		t.Logf("inode %v not ek %v", ino.Inode, ek)
		return
	}
	t.Logf("testGetEkRefCnt ek %v get refCnt %v", ek, val.(uint32))
	return val.(uint32)
}

func testDelDiscardEK(t *testing.T, fileIno *Inode) (cnt uint32) {
	delCnt := len(mp.extDelCh)
	t.Logf("enter testDelDiscardEK extDelCh size %v", delCnt)
	if len(mp.extDelCh) == 0 {
		t.Logf("testDelDiscardEK discard ek cnt %v", cnt)
		return
	}
	for i := 0; i < delCnt; i++ {
		eks := <-mp.extDelCh
		for _, ek := range eks {
			t.Logf("the delete ek is %v", ek)
			cnt++
		}
		t.Logf("pop %v", i)
	}
	t.Logf("testDelDiscardEK discard ek cnt %v", cnt)
	return
}

// create
func TestSplitKeyDeletion(t *testing.T) {
	log.LogDebugf("action[TestSplitKeyDeletion] start!!!!!!!!!!!")
	initMp(t)
	initVer()
	mp.config.Cursor = 1100

	fileIno := testCreateInode(t, FileModeType)

	fileName := "fileTest"
	dirDen := testCreateDentry(t, 1, fileIno.Inode, fileName, FileModeType)
	assert.True(t, dirDen != nil)

	initExt := buildExtentKey(0, 0, 1024, 0, 1000)
	fileIno.Extents.eks = append(fileIno.Extents.eks, initExt)

	splitSeq := testCreateVer()
	splitKey := buildExtentKey(splitSeq, 500, 1024, 128100, 100)
	extents := &SortedExtents{}
	extents.eks = append(extents.eks, splitKey)

	iTmp := &Inode{
		Inode:   fileIno.Inode,
		Extents: extents,
		verSeq:  splitSeq,
	}

	mp.fsmAppendExtentsWithCheck(iTmp, true)
	assert.True(t, testGetSplitSize(t, fileIno) == 1)
	assert.True(t, testGetEkRefCnt(t, fileIno, &initExt) == 4)

	testCleanSnapshot(t, 0)
	delCnt := testDelDiscardEK(t, fileIno)
	assert.True(t, 1 == delCnt)

	assert.True(t, testGetSplitSize(t, fileIno) == 1)
	assert.True(t, testGetEkRefCnt(t, fileIno, &initExt) == 3)

	log.LogDebugf("try to deletion current")
	testDeleteDirTree(t, 1, 0)

	fileIno.GetAllExtsOfflineInode(mp.config.PartitionId)

	splitCnt := uint32(testGetSplitSize(t, fileIno))
	assert.True(t, 0 == splitCnt)

	assert.True(t, testGetSplitSize(t, fileIno) == 0)
	assert.True(t, testGetEkRefCnt(t, fileIno, &initExt) == 0)

}

func testGetlastVer() (verSeq uint64) {
	vlen := len(mp.multiVersionList.VerList)
	return mp.multiVersionList.VerList[vlen-1].Ver
}

var tm = time.Now().Unix()

func testCreateVer() (verSeq uint64) {
	mp.multiVersionList.Lock()
	defer mp.multiVersionList.Unlock()

	tm = tm + 1
	verInfo := &proto.VolVersionInfo{
		Ver:    uint64(tm),
		Ctime:  time.Now(),
		Status: proto.VersionNormal,
	}
	mp.multiVersionList.VerList = append(mp.multiVersionList.VerList, verInfo)
	mp.verSeq = verInfo.Ver
	return verInfo.Ver
}

func testReadDirAll(t *testing.T, verSeq uint64, parentId uint64) (resp *ReadDirLimitResp) {
	//testPrintAllDentry(t)
	t.Logf("[testReadDirAll] with seq %v parentId %v", verSeq, parentId)
	req := &ReadDirLimitReq{
		PartitionID: partitionId,
		VolName:     mp.GetVolName(),
		ParentID:    parentId,
		Limit:       math.MaxUint64,
		VerSeq:      verSeq,
	}
	return mp.readDirLimit(req)
}

func testVerListRemoveVer(t *testing.T, verSeq uint64) bool {
	testPrintAllSysVerList(t)
	for i, ver := range mp.multiVersionList.VerList {
		if ver.Ver == verSeq {
			// mp.multiVersionList = append(mp.multiVersionList[:i], mp.multiVersionList[i+1:]...)
			if i == len(mp.multiVersionList.VerList)-1 {
				mp.multiVersionList.VerList = mp.multiVersionList.VerList[:i]
				return true
			}
			mp.multiVersionList.VerList = append(mp.multiVersionList.VerList[:i], mp.multiVersionList.VerList[i+1:]...)
			return true
		}
	}
	return false
}

var ct = uint64(time.Now().Unix())
var seqAllArr = []uint64{0, ct, ct + 2111, ct + 10333, ct + 53456, ct + 60000, ct + 72344, ct + 234424, ct + 334424}

func TestAppendList(t *testing.T) {
	initMp(t)
	for _, verSeq := range seqAllArr {
		verInfo := &proto.VolVersionInfo{
			Ver:    verSeq,
			Ctime:  time.Unix(int64(verSeq), 0),
			Status: proto.VersionNormal,
		}
		mp.multiVersionList.VerList = append(mp.multiVersionList.VerList, verInfo)
	}

	var ino = testCreateInode(t, 0)
	t.Logf("enter TestAppendList")
	index := 5
	seqArr := seqAllArr[1:index]
	t.Logf("layer len %v, arr size %v, seqarr(%v)", len(ino.multiVersions), len(seqArr), seqArr)
	for idx, seq := range seqArr {
		exts := buildExtents(seq, uint64(idx*1000), uint64(idx))
		t.Logf("buildExtents exts[%v]", exts)
		iTmp := &Inode{
			Inode: ino.Inode,
			Extents: &SortedExtents{
				eks: exts,
			},
			ObjExtents: NewSortedObjExtents(),
			verSeq:     seq,
		}
		mp.verSeq = seq

		if status := mp.fsmAppendExtentsWithCheck(iTmp, false); status != proto.OpOk {
			t.Errorf("status %v", status)
		}
	}
	t.Logf("layer len %v, arr size %v, seqarr(%v)", len(ino.multiVersions), len(seqArr), seqArr)
	assert.True(t, len(ino.multiVersions) == len(seqArr))
	assert.True(t, ino.verSeq == mp.verSeq)

	for i := 0; i < len(seqArr)-1; i++ {
		assert.True(t, ino.multiVersions[i].verSeq == seqArr[len(seqArr)-i-2])
		t.Logf("layer %v len %v content %v,seq %v, %v", i, len(ino.multiVersions[i].Extents.eks), ino.multiVersions[i].Extents.eks,
			ino.multiVersions[i].verSeq, seqArr[len(seqArr)-i-2])
		assert.True(t, len(ino.multiVersions[i].Extents.eks) == 0)
	}

	//-------------   split at begin -----------------------------------------
	t.Logf("start split at begin")
	var splitSeq = seqAllArr[index]
	splitKey := buildExtentKey(splitSeq, 0, 0, 128000, 10)
	extents := &SortedExtents{}
	extents.eks = append(extents.eks, splitKey)

	iTmp := &Inode{
		Inode:   ino.Inode,
		Extents: extents,
		verSeq:  splitSeq,
	}
	mp.fsmAppendExtentsWithCheck(iTmp, true)
	t.Logf("in split at begin")
	assert.True(t, ino.multiVersions[0].Extents.eks[0].VerSeq == ino.multiVersions[3].verSeq)
	assert.True(t, ino.multiVersions[0].Extents.eks[0].FileOffset == 0)
	assert.True(t, ino.multiVersions[0].Extents.eks[0].ExtentId == 0)
	assert.True(t, ino.multiVersions[0].Extents.eks[0].ExtentOffset == 0)
	assert.True(t, ino.multiVersions[0].Extents.eks[0].Size == splitKey.Size)

	t.Logf("in split at begin")

	assert.True(t, isExtEqual(ino.Extents.eks[0], splitKey))
	assert.True(t, checkOffSetInSequnce(t, ino.Extents.eks))

	t.Logf("top layer len %v, layer 1 len %v arr size %v", len(ino.Extents.eks), len(ino.multiVersions[0].Extents.eks), len(seqArr))
	assert.True(t, len(ino.multiVersions[0].Extents.eks) == 1)
	assert.True(t, len(ino.Extents.eks) == len(seqArr)+1)

	testCheckExtList(t, ino, seqArr)

	//--------  split at middle  -----------------------------------------------
	t.Logf("start split at middle")

	lastTopEksLen := len(ino.Extents.eks)
	t.Logf("split at middle lastTopEksLen %v", lastTopEksLen)

	index++
	splitSeq = seqAllArr[index]
	splitKey = buildExtentKey(splitSeq, 500, 0, 128100, 100)
	extents = &SortedExtents{}
	extents.eks = append(extents.eks, splitKey)

	iTmp = &Inode{
		Inode:   ino.Inode,
		Extents: extents,
		verSeq:  splitSeq,
	}
	t.Logf("split at middle multiVersions %v", len(ino.multiVersions))
	mp.fsmAppendExtentsWithCheck(iTmp, true)
	t.Logf("split at middle multiVersions %v", len(ino.multiVersions))

	getExtRsp := testGetExtList(t, ino, ino.multiVersions[0].verSeq)
	t.Logf("split at middle getExtRsp len %v seq(%v), toplayer len:%v seq(%v)",
		len(getExtRsp.Extents), ino.multiVersions[0].verSeq, len(ino.Extents.eks), ino.verSeq)

	assert.True(t, len(getExtRsp.Extents) == lastTopEksLen+2)
	assert.True(t, len(ino.Extents.eks) == lastTopEksLen+2)
	assert.True(t, checkOffSetInSequnce(t, ino.Extents.eks))

	t.Logf("ino exts{%v}", ino.Extents.eks)

	//--------  split at end  -----------------------------------------------
	t.Logf("start split at end")
	// split at end
	lastTopEksLen = len(ino.Extents.eks)
	index++
	splitSeq = seqAllArr[index]
	splitKey = buildExtentKey(splitSeq, 3900, 3, 129000, 100)
	extents = &SortedExtents{}
	extents.eks = append(extents.eks, splitKey)

	iTmp = &Inode{
		Inode:   ino.Inode,
		Extents: extents,
		verSeq:  splitSeq,
	}
	t.Logf("split key:%v", splitKey)
	getExtRsp = testGetExtList(t, ino, ino.multiVersions[0].verSeq)
	t.Logf("split at middle multiVersions %v, extent %v, level 1 %v", len(ino.multiVersions), getExtRsp.Extents, ino.multiVersions[0].Extents.eks)

	mp.fsmAppendExtentsWithCheck(iTmp, true)
	t.Logf("split at middle multiVersions %v", len(ino.multiVersions))
	getExtRsp = testGetExtList(t, ino, ino.multiVersions[0].verSeq)
	t.Logf("split at middle multiVersions %v, extent %v, level 1 %v", len(ino.multiVersions), getExtRsp.Extents, ino.multiVersions[0].Extents.eks)

	t.Logf("split at middle getExtRsp len %v seq(%v), toplayer len:%v seq(%v)",
		len(getExtRsp.Extents), ino.multiVersions[0].verSeq, len(ino.Extents.eks), ino.verSeq)

	assert.True(t, len(getExtRsp.Extents) == lastTopEksLen+1)
	assert.True(t, len(ino.Extents.eks) == lastTopEksLen+1)
	assert.True(t, isExtEqual(ino.Extents.eks[lastTopEksLen], splitKey))
	//assert.True(t, false)

	//--------  split at the splited one  -----------------------------------------------
	t.Logf("start split at end")
	// split at end
	lastTopEksLen = len(ino.Extents.eks)
	index++
	splitSeq = seqAllArr[index]
	splitKey = buildExtentKey(splitSeq, 3950, 3, 129000, 20)
	extents = &SortedExtents{}
	extents.eks = append(extents.eks, splitKey)

	iTmp = &Inode{
		Inode:   ino.Inode,
		Extents: extents,
		verSeq:  splitSeq,
	}
	t.Logf("split key:%v", splitKey)
	mp.fsmAppendExtentsWithCheck(iTmp, true)

	getExtRsp = testGetExtList(t, ino, ino.multiVersions[0].verSeq)

	assert.True(t, len(ino.Extents.eks) == lastTopEksLen+2)
	assert.True(t, checkOffSetInSequnce(t, ino.Extents.eks))
}

//func MockSubmitTrue(mp *metaPartition, inode uint64, offset int, data []byte,
//	flags int) (write int, err error) {
//	return len(data), nil
//}

func testPrintAllSysVerList(t *testing.T) {
	for idx, info := range mp.multiVersionList.VerList {
		t.Logf("testPrintAllSysVerList idx %v, info %v", idx, info)
	}
}

func testPrintAllDentry(t *testing.T) uint64 {
	var cnt uint64
	mp.dentryTree.Ascend(func(i BtreeItem) bool {
		den := i.(*Dentry)
		t.Logf("testPrintAllDentry name %v top layer dentry:%v", den.Name, den)
		for id, info := range den.dentryList {
			t.Logf("testPrintAllDentry name %v layer %v, denSeq %v den %v", den.Name, id, info.VerSeq, info)
		}
		cnt++
		return true
	})
	return cnt
}

func testPrintAllInodeInfo(t *testing.T) {
	mp.inodeTree.Ascend(func(item BtreeItem) bool {
		i := item.(*Inode)
		t.Logf("action[PrintAllVersionInfo] toplayer inode [%v] verSeq [%v] hist len [%v]", i, i.verSeq, len(i.multiVersions))
		for id, info := range i.multiVersions {
			t.Logf("action[PrintAllVersionInfo] layer [%v]  verSeq [%v] inode [%v]", id, info.verSeq, info)
		}
		return true
	})
}

func testPrintInodeInfo(t *testing.T, ino *Inode) {

	i := mp.inodeTree.Get(ino).(*Inode)
	t.Logf("action[PrintAllVersionInfo] toplayer inode [%v] verSeq [%v] hist len [%v]", i, i.verSeq, len(i.multiVersions))
	for id, info := range i.multiVersions {
		t.Logf("action[PrintAllVersionInfo] layer [%v]  verSeq [%v] inode [%v]", id, info.verSeq, info)
	}

}

func testDelDirSnapshotVersion(t *testing.T, verSeq uint64, dirIno *Inode, dirDentry *Dentry) {
	if verSeq != 0 {
		assert.True(t, testVerListRemoveVer(t, verSeq))
	}

	rspReadDir := testReadDirAll(t, verSeq, dirIno.Inode)
	//testPrintAllDentry(t)

	rDirIno := dirIno.Copy().(*Inode)
	rDirIno.verSeq = verSeq

	rspDelIno := mp.fsmUnlinkInode(rDirIno)

	t.Logf("rspDelinfo ret %v content %v", rspDelIno.Status, rspDelIno)
	assert.True(t, rspDelIno.Status == proto.OpOk)

	rDirDentry := dirDentry.Copy().(*Dentry)
	rDirDentry.VerSeq = verSeq
	rspDelDen := mp.fsmDeleteDentry(rDirDentry, false)
	assert.True(t, rspDelDen.Status == proto.OpOk)

	for idx, info := range rspReadDir.Children {
		t.Logf("testDelDirSnapshotVersion: delSeq %v  to del idx %v infof %v", verSeq, idx, info)
		rino := &Inode{
			Inode:  info.Inode,
			Type:   FileModeType,
			verSeq: verSeq,
		}
		testPrintInodeInfo(t, rino)
		log.LogDebugf("testDelDirSnapshotVersion get rino %v start", rino)
		t.Logf("testDelDirSnapshotVersion get rino %v start", rino)
		ino := mp.getInode(rino, false)
		log.LogDebugf("testDelDirSnapshotVersion get rino %v end", ino)
		t.Logf("testDelDirSnapshotVersion get rino %v end", rino)
		assert.True(t, ino.Status == proto.OpOk)
		if ino.Status != proto.OpOk {
			panic(nil)
		}
		rino.verSeq = verSeq
		rspDelIno = mp.fsmUnlinkInode(rino)

		assert.True(t, rspDelIno.Status == proto.OpOk || rspDelIno.Status == proto.OpNotExistErr)
		if rspDelIno.Status != proto.OpOk && rspDelIno.Status != proto.OpNotExistErr {
			t.Logf("testDelDirSnapshotVersion: rspDelIno %v return st %v", rspDelIno, proto.ParseErrorCode(int32(rspDelIno.Status)))
			panic(nil)
		}
		dentry := &Dentry{
			ParentId: rDirIno.Inode,
			Name:     info.Name,
			Type:     FileModeType,
			VerSeq:   verSeq,
			Inode:    rino.Inode,
		}
		log.LogDebugf("test.testDelDirSnapshotVersion: dentry param %v ", dentry)
		//testPrintAllDentry(t)
		iden, st := mp.getDentry(dentry)
		if st != proto.OpOk {
			t.Logf("testDelDirSnapshotVersion: dentry %v return st %v", dentry, proto.ParseErrorCode(int32(st)))
		}
		log.LogDebugf("test.testDelDirSnapshotVersion: get dentry %v ", iden)
		assert.True(t, st == proto.OpOk)

		rDen := iden.Copy().(*Dentry)
		rDen.VerSeq = verSeq
		rspDelDen = mp.fsmDeleteDentry(rDen, false)
		assert.True(t, rspDelDen.Status == proto.OpOk)
	}
}

func TestDentry(t *testing.T) {
	initMp(t)

	var denArry []*Dentry
	//err := gohook.HookMethod(mp, "submit", MockSubmitTrue, nil)
	mp.config.Cursor = 1100
	//--------------------build dir and it's child on different version ------------------
	seq0 := testCreateVer()
	dirIno := testCreateInode(t, DirModeType)
	assert.True(t, dirIno != nil)
	dirDen := testCreateDentry(t, 1, dirIno.Inode, "testDir", DirModeType)
	assert.True(t, dirDen != nil)

	fIno := testCreateInode(t, FileModeType)
	assert.True(t, fIno != nil)
	fDen := testCreateDentry(t, dirIno.Inode, fIno.Inode, "testfile", FileModeType)
	denArry = append(denArry, fDen)

	//--------------------------------------
	seq1 := testCreateVer()
	fIno1 := testCreateInode(t, FileModeType)
	fDen1 := testCreateDentry(t, dirIno.Inode, fIno1.Inode, "testfile2", FileModeType)
	denArry = append(denArry, fDen1)

	//--------------------------------------
	seq2 := testCreateVer()
	fIno2 := testCreateInode(t, FileModeType)
	fDen2 := testCreateDentry(t, dirIno.Inode, fIno2.Inode, "testfile3", FileModeType)
	denArry = append(denArry, fDen2)

	//--------------------------------------
	seq3 := testCreateVer()
	//--------------------read dir and it's child on different version ------------------

	t.Logf("TestDentry seq %v,%v,uncommit %v,dir:%v, dentry {%v],inode[%v,%v,%v]", seq1, seq2, seq3, dirDen, denArry, fIno, fIno1, fIno2)
	//-----------read curr version --
	rspReadDir := testReadDirAll(t, 0, 1)
	t.Logf("len child %v, len arry %v", len(rspReadDir.Children), len(denArry))
	assert.True(t, len(rspReadDir.Children) == 1)
	assert.True(t, isDentryEqual(&rspReadDir.Children[0], dirDen))

	rspReadDir = testReadDirAll(t, 0, dirIno.Inode)

	assert.True(t, len(rspReadDir.Children) == len(denArry))
	for idx, info := range rspReadDir.Children {
		t.Logf("getinfo:%v, expect:%v", info, denArry[idx])
		assert.True(t, isDentryEqual(&info, denArry[idx]))
	}

	//-----------read 0 version --
	rspReadDir = testReadDirAll(t, math.MaxUint64, dirIno.Inode)
	assert.True(t, len(rspReadDir.Children) == 0)

	//-----------read layer 1 version --   seq2 is the last layer, seq1 is the second layer
	rspReadDir = testReadDirAll(t, seq1, dirIno.Inode)
	assert.True(t, len(rspReadDir.Children) == 2)
	for idx, info := range rspReadDir.Children {
		t.Logf("getinfo:%v, expect:%v", info, denArry[idx])
		assert.True(t, isDentryEqual(&info, denArry[idx]))
	}
	//-----------read layer 2 version --
	rspReadDir = testReadDirAll(t, seq0, dirIno.Inode)
	assert.True(t, len(rspReadDir.Children) == 1)
	assert.True(t, isDentryEqual(&rspReadDir.Children[0], fDen))

	testPrintAllDentry(t)
	//--------------------del snapshot and read dir and it's child on different version(cann't be work on interfrace) ------------------
	t.Logf("try testDelDirSnapshotVersion %v", seq0)
	log.LogDebugf("try testDelDirSnapshotVersion %v", seq0)

	testDelDirSnapshotVersion(t, seq0, dirIno, dirDen)
	rspReadDir = testReadDirAll(t, seq0, dirIno.Inode)
	assert.True(t, len(rspReadDir.Children) == 1)

	testPrintAllDentry(t)
	//---------------------------------------------
	t.Logf("try testDelDirSnapshotVersion 0 top layer")
	log.LogDebugf("try testDelDirSnapshotVersion 0")
	testDelDirSnapshotVersion(t, 0, dirIno, dirDen)
	rspReadDir = testReadDirAll(t, 0, dirIno.Inode)
	t.Logf("after  testDelDirSnapshotVersion 0 can see file %v %v", len(rspReadDir.Children), rspReadDir.Children)
	assert.True(t, len(rspReadDir.Children) == 0)
	rspReadDir = testReadDirAll(t, seq1, dirIno.Inode)
	t.Logf("after  testDelDirSnapshotVersion 0 can see file %v %v", len(rspReadDir.Children), rspReadDir.Children)
	assert.True(t, len(rspReadDir.Children) == 2)

	//---------------------------------------------
	t.Logf("try testDelDirSnapshotVersion %v", seq1)
	log.LogDebugf("try testDelDirSnapshotVersion %v", seq1)
	testDelDirSnapshotVersion(t, seq1, dirIno, dirDen)

	rspReadDir = testReadDirAll(t, seq1, dirIno.Inode)
	t.Logf("after  testDelDirSnapshotVersion %v can see file %v %v", seq1, len(rspReadDir.Children), rspReadDir.Children)
	assert.True(t, len(rspReadDir.Children) == 2)
	testPrintAllSysVerList(t)

	//---------------------------------------------
	t.Logf("try testDelDirSnapshotVersion %v", seq2)
	log.LogDebugf("try testDelDirSnapshotVersion %v", seq2)
	testDelDirSnapshotVersion(t, seq2, dirIno, dirDen)

	rspReadDir = testReadDirAll(t, seq2, dirIno.Inode)
	t.Logf("after  testDelDirSnapshotVersion %v can see file %v %v", seq1, len(rspReadDir.Children), rspReadDir.Children)
	assert.True(t, len(rspReadDir.Children) == 0)

	t.Logf("testPrintAllSysVerList")
	testPrintAllSysVerList(t)
	t.Logf("testPrintAllInodeInfo")
	testPrintAllInodeInfo(t)
}

func testPrintDirTree(t *testing.T, parentId uint64, path string, verSeq uint64) (dirCnt int, fCnt int) {
	if verSeq == 0 {
		verSeq = math.MaxUint64
	}
	rspReadDir := testReadDirAll(t, verSeq, parentId)
	for _, child := range rspReadDir.Children {
		pathInner := fmt.Sprintf("%v/%v", path, child.Name)
		if proto.IsDir(child.Type) {
			dirCnt++
			dc, fc := testPrintDirTree(t, child.Inode, pathInner, verSeq)
			dirCnt += dc
			fCnt += fc
			t.Logf("dir:%v", pathInner)
		} else {
			fCnt++
			t.Logf("file:%v", pathInner)
		}
	}
	return
}
func testAppendExt(t *testing.T, seq uint64, idx int, inode uint64) {
	exts := buildExtents(seq, uint64(idx*1000), uint64(idx))
	t.Logf("buildExtents exts[%v]", exts)
	iTmp := &Inode{
		Inode: inode,
		Extents: &SortedExtents{
			eks: exts,
		},
		ObjExtents: NewSortedObjExtents(),
		verSeq:     seq,
	}
	mp.verSeq = seq
	if status := mp.fsmAppendExtentsWithCheck(iTmp, false); status != proto.OpOk {
		t.Errorf("status %v", status)
	}
}

func TestTruncateAndDel(t *testing.T) {
	log.LogDebugf("TestTruncate start")
	initMp(t)
	mp.config.Cursor = 1100
	//--------------------build dir and it's child on different version ------------------
	initVer()
	fileIno := testCreateInode(t, FileModeType)
	assert.True(t, fileIno != nil)
	dirDen := testCreateDentry(t, 1, fileIno.Inode, "testDir", FileModeType)
	assert.True(t, dirDen != nil)

	testAppendExt(t, 0, 0, fileIno.Inode)
	seq1 := testCreateVer() // seq1 is NOT commited

	seq2 := testCreateVer() // seq1 is commited,seq2 not commited

	t.Logf("TestTruncate. create new snapshot seq %v,%v,file verlist [%v]", seq1, seq2, fileIno.multiVersions)

	ino := &Inode{
		Inode:      fileIno.Inode,
		Size:       500,
		ModifyTime: time.Now().Unix(),
	}
	mp.fsmExtentsTruncate(ino)

	t.Logf("TestTruncate. create new snapshot seq %v,%v,file verlist size %v [%v]", seq1, seq2, len(fileIno.multiVersions), fileIno.multiVersions)

	assert.True(t, 2 == len(fileIno.multiVersions))
	rsp := testGetExtList(t, fileIno, 0)
	assert.True(t, rsp.Size == 500)

	rsp = testGetExtList(t, fileIno, seq2)
	assert.True(t, rsp.Size == 500)

	rsp = testGetExtList(t, fileIno, seq1)
	assert.True(t, rsp.Size == 1000)

	rsp = testGetExtList(t, fileIno, math.MaxUint64)
	assert.True(t, rsp.Size == 1000)

	// -------------------------------------------------------

	testCreateVer() // seq2 IS commited, seq3 not
	mp.fsmUnlinkInode(ino)

	assert.True(t, 3 == len(fileIno.multiVersions))
	rsp = testGetExtList(t, fileIno, 0)
	assert.True(t, len(rsp.Extents) == 0)

	rsp = testGetExtList(t, fileIno, seq2)
	assert.True(t, rsp.Size == 500)

	rsp = testGetExtList(t, fileIno, seq1)
	assert.True(t, rsp.Size == 1000)

	rsp = testGetExtList(t, fileIno, math.MaxUint64)
	assert.True(t, rsp.Size == 1000)
}

func testDeleteFile(t *testing.T, verSeq uint64, parentId uint64, child *proto.Dentry) {
	t.Logf("testDeleteFile seq %v", verSeq)
	fsmDentry := &Dentry{
		ParentId:   parentId,
		Name:       child.Name,
		Inode:      child.Inode,
		Type:       child.Type,
		VerSeq:     verSeq,
		dentryList: nil,
	}
	t.Logf("testDeleteFile seq %v %v dentry %v", verSeq, fsmDentry.VerSeq, fsmDentry)
	assert.True(t, nil != mp.fsmDeleteDentry(fsmDentry, false))

	var rino = &Inode{
		Inode:  child.Inode,
		Type:   child.Type,
		verSeq: verSeq,
	}
	rino.verSeq = verSeq
	rspDelIno := mp.fsmUnlinkInode(rino)

	assert.True(t, rspDelIno.Status == proto.OpOk || rspDelIno.Status == proto.OpNotExistErr)
	if rspDelIno.Status != proto.OpOk && rspDelIno.Status != proto.OpNotExistErr {
		t.Logf("testDelDirSnapshotVersion: rspDelIno %v return st %v", rspDelIno, proto.ParseErrorCode(int32(rspDelIno.Status)))
		panic(nil)
	}
}

func testDeleteDirTree(t *testing.T, parentId uint64, verSeq uint64) {
	t.Logf("testDeleteDirTree parentId %v seq %v", parentId, verSeq)
	rspReadDir := testReadDirAll(t, verSeq, parentId)
	for _, child := range rspReadDir.Children {
		if proto.IsDir(child.Type) {
			testDeleteDirTree(t, child.Inode, verSeq)
		}
		t.Logf("action[testDeleteDirTree] delete children %v", child)
		log.LogDebugf("action[testDeleteDirTree] seq %v delete children %v", verSeq, child)
		testDeleteFile(t, verSeq, parentId, &child)
	}
	return
}

func testCleanSnapshot(t *testing.T, verSeq uint64) {
	t.Logf("action[testCleanSnapshot] verSeq %v", verSeq)
	log.LogDebugf("action[testCleanSnapshot] verSeq %v", verSeq)
	assert.True(t, testVerListRemoveVer(t, verSeq))
	if verSeq == 0 {
		verSeq = math.MaxUint64
	}
	testDeleteDirTree(t, 1, verSeq)
	return
}

// create
func testSnapshotDeletion(t *testing.T, topFirst bool) {
	log.LogDebugf("action[TestSnapshotDeletion] start!!!!!!!!!!!")
	initMp(t)
	initVer()
	//err := gohook.HookMethod(mp, "submit", MockSubmitTrue, nil)
	mp.config.Cursor = 1100
	//--------------------build dir and it's child on different version ------------------

	dirLayCnt := 4
	var (
		dirName      string
		dirInoId     uint64 = 1
		verArr       []uint64
		renameDen    *Dentry
		renameDstIno uint64
		dirCnt       int
		fileCnt      int
	)

	for layIdx := 0; layIdx < dirLayCnt; layIdx++ {
		t.Logf("build tree:layer %v,last dir name %v inodeid %v", layIdx, dirName, dirInoId)
		dirIno := testCreateInode(t, DirModeType)
		assert.True(t, dirIno != nil)
		dirName = fmt.Sprintf("dir_layer_%v_1", layIdx+1)
		dirDen := testCreateDentry(t, dirInoId, dirIno.Inode, dirName, DirModeType)
		assert.True(t, dirDen != nil)
		if dirDen == nil {
			panic(nil)
		}
		dirIno1 := testCreateInode(t, DirModeType)
		assert.True(t, dirIno1 != nil)
		dirName1 := fmt.Sprintf("dir_layer_%v_2", layIdx+1)
		dirDen1 := testCreateDentry(t, dirInoId, dirIno1.Inode, dirName1, DirModeType)
		assert.True(t, dirDen1 != nil)

		if layIdx == 2 {
			renameDen = dirDen.Copy().(*Dentry)
		}
		if layIdx == 1 {
			renameDstIno = dirIno1.Inode
		}
		for fileIdx := 0; fileIdx < (layIdx+1)*2; fileIdx++ {
			fileIno := testCreateInode(t, FileModeType)
			assert.True(t, dirIno != nil)

			fileName := fmt.Sprintf("layer_%v_file_%v", layIdx+1, fileIdx+1)
			dirDen = testCreateDentry(t, dirIno.Inode, fileIno.Inode, fileName, FileModeType)
			assert.True(t, dirDen != nil)
		}
		dirInoId = dirIno.Inode
		ver := testGetlastVer()
		verArr = append(verArr, ver)

		dCnt, fCnt := testPrintDirTree(t, 1, "root", ver)
		if layIdx+1 < dirLayCnt {
			log.LogDebugf("testCreateVer")
			testCreateVer()
		}

		log.LogDebugf("PrintALl verSeq %v get dirCnt %v, fCnt %v mp verlist size %v", ver, dCnt, fCnt, len(mp.multiVersionList.VerList))
	}

	t.Logf("---------------------------------------------------------------------")
	t.Logf("--------testPrintDirTree by ver -------------------------------------")
	t.Logf("---------------------------------------------------------------------")
	for idx, ver := range verArr {
		dCnt, fCnt := testPrintDirTree(t, 1, "root", ver)
		t.Logf("---------------------------------------------------------------------")
		t.Logf("PrintALl verSeq %v get dirCnt %v, fCnt %v", ver, dCnt, fCnt)
		assert.True(t, dCnt == dirCnt+2)
		assert.True(t, fCnt == fileCnt+(idx+1)*2)
		dirCnt = dCnt
		fileCnt = fCnt
	}
	t.Logf("------------rename dir ----------------------")
	if renameDen != nil {
		t.Logf("try to move dir %v", renameDen)
		renameDen.VerSeq = 0
		assert.True(t, nil != mp.fsmDeleteDentry(renameDen, false))
		renameDen.Name = fmt.Sprintf("rename_from_%v", renameDen.Name)
		renameDen.ParentId = renameDstIno

		t.Logf("try to move to dir %v", renameDen)
		assert.True(t, mp.fsmCreateDentry(renameDen, false) == proto.OpOk)
		testPrintDirTree(t, 1, "root", 0)
	}
	delSnapshotList := func() {
		t.Logf("---------------------------------------------------------------------")
		t.Logf("--------testCleanSnapshot by ver-------------------------------------")
		t.Logf("---------------------------------------------------------------------")
		for idx, ver := range verArr {
			t.Logf("---------------------------------------------------------------------")
			t.Logf("index %v ver %v try to deletion", idx, ver)
			log.LogDebugf("index %v ver %v try to deletion", idx, ver)
			t.Logf("---------------------------------------------------------------------")
			testCleanSnapshot(t, ver)
			t.Logf("---------------------------------------------------------------------")
			t.Logf("index %v ver %v after deletion mp inode freeList len %v", idx, ver, mp.freeList.Len())
			log.LogDebugf("index %v ver %v after deletion mp inode freeList len %v", idx, ver, mp.freeList.Len())
			t.Logf("---------------------------------------------------------------------")
			if idx == len(verArr)-2 {
				break
			}
		}
	}
	delCurrent := func() {
		t.Logf("---------------------------------------------------------------------")
		t.Logf("--------testDeleteAll current -------------------------------------")
		t.Logf("---------------------------------------------------------------------")
		log.LogDebugf("try to deletion current")
		testDeleteDirTree(t, 1, 0)
		log.LogDebugf("try to deletion current finish")
	}

	if topFirst {
		delCurrent()
		delSnapshotList()
	} else {
		delSnapshotList()
		delCurrent()
	}

	t.Logf("---------------------------------------------------------------------")
	t.Logf("after deletion current layerr mp inode freeList len %v fileCnt %v dircnt %v", mp.freeList.Len(), fileCnt, dirCnt)
	assert.True(t, mp.freeList.Len() == fileCnt+dirCnt)
	assert.True(t, 0 == testPrintAllDentry(t))
	t.Logf("---------------------------------------------------------------------")

	t.Logf("---------------------------------------------------------------------")
	t.Logf("--------testPrintAllInodeInfo should have no inode -------------------------------------")
	t.Logf("---------------------------------------------------------------------")
	testPrintAllInodeInfo(t)
	t.Logf("---------------------------------------------")
	//assert.True(t, false)
}

// create
func TestSnapshotDeletion(t *testing.T) {
	testSnapshotDeletion(t, true)
	testSnapshotDeletion(t, false)
}