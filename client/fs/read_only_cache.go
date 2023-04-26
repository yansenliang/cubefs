package fs

import (
	"bytes"
	"container/list"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/log"
)

var (
	DENTRY_NOT_CACHE = errors.New("DENTRY_NOT_CACHE")
	DENTRY_NOT_EXIST = errors.New("DENTRY_NOT_EXIST")
)

const (
	MinDentryBufferEvictNum = 10
	// MaxDentryCacheEvictNum is used in the back ground. We can evict 2000 entries at max.
	MaxDentryBufferEvictNum        = 1000
	MaxDEntryBufferElement         = 1000000
	DentryBufferBgEvictionInterval = 5 * time.Minute
	DefaultDentryBufferExpiredTime = 5 * time.Hour
)

type addressPointer struct {
	Offset int64
	Size   uint64
}

type persistentAttr struct {
	Addr addressPointer //  attr address
}

type dentryData struct {
	Type uint32
	Ino  uint64
}

type persistentDentry struct {
	DentryHead  addressPointer        // dentry address
	EntryBuffer map[string]dentryData // buffer entry until all of the dir's the entry are cached
	Expiration  time.Duration         // max time of persisted EntryBuffer stays in memory
	IsPersist   bool                  // flag used to identify whether it is persisted to the file
}

type persistentFileHandler struct {
	DataFile    *os.File
	EndPosition int64
}

type ReadOnlyMetaCache struct {
	sync.RWMutex
	AttrBinaryFile        *persistentFileHandler       // AttrBinary File's Handle
	DentryBinaryFile      *persistentFileHandler       // DentryBinary File's Handle
	Inode2PersistAttr     map[uint64]*persistentAttr   // transfer inode to persisent attr
	Inode2PersistDentry   map[uint64]*persistentDentry // transfer inode to persisent dentry
	FullCachedEntryBuffer map[uint64]*list.Element     // key is ino, element is address pointer of persistentDentry
	LruList               *list.List                   // store full cached Entrybuffer
	PersistAttrMtx        sync.RWMutex
	PersistDentryMtx      sync.RWMutex
}

func NewReadOnlyMetaCache(sub_dir string) (*ReadOnlyMetaCache, error) {
	meta_cache := &ReadOnlyMetaCache{
		AttrBinaryFile:        &persistentFileHandler{},
		DentryBinaryFile:      &persistentFileHandler{},
		Inode2PersistAttr:     make(map[uint64]*persistentAttr),
		Inode2PersistDentry:   make(map[uint64]*persistentDentry),
		LruList:               list.New(),
		FullCachedEntryBuffer: make(map[uint64]*list.Element),
	}
	attr_file_path := sub_dir + "read_only_attr_cache"
	dentry_file_path := sub_dir + "read_only_dentry_cache"
	if err := meta_cache.ParseAllPersistentAttr(attr_file_path); err != nil {
		log.LogDebugf("[ReadOnlyMetaCache][NewReadOnlyMetaCache] parse attr file fail,err(%s)", err.Error())
		return meta_cache, err
	}
	if err := meta_cache.ParseAllPersistentDentry(dentry_file_path); err != nil {
		log.LogDebugf("[ReadOnlyMetaCache][NewReadOnlyMetaCache] parse dentry file fail,err(%s)", err.Error())
		return meta_cache, err
	}
	go meta_cache.BackgroundEvictionEntryBuffer()
	return meta_cache, nil
}

// open and read the Attr file to build Inode2PersistAttr, it will also set AttrBinaryFile correct
func (persistent_meta_cache *ReadOnlyMetaCache) ParseAllPersistentAttr(attr_file_path string) error {
	var err error
	persistent_meta_cache.AttrBinaryFile.DataFile, err = os.OpenFile(attr_file_path, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0666)
	if err != nil {
		log.LogDebugf("[ReadOnlyCache][ParseAllPersistentAttr] open persisent attr file fail")
		return err
	}
	// stat the attr file and set file size as EndPosisiton
	info, _ := persistent_meta_cache.AttrBinaryFile.DataFile.Stat()
	persistent_meta_cache.AttrBinaryFile.EndPosition = info.Size()

	buf := make([]byte, 16+8) // 16 bytes for address, 8 bytes for Ino
	bytes_buf := &bytes.Buffer{}
	bytes_buf.Grow(16 + 8)
	for i := int64(0); i < persistent_meta_cache.AttrBinaryFile.EndPosition; {
		address := &addressPointer{}
		if _, err = persistent_meta_cache.AttrBinaryFile.DataFile.ReadAt(buf, i); err != nil {
			log.LogDebugf("[ReadOnlyCache][ParseAllPersistentAttr] read persisent attr file fail")
			return err
		}
		bytes_buf.Read(buf)
		if err = binary.Read(bytes_buf, binary.BigEndian, &address.Offset); err != nil {
			log.LogDebugf("[ReadOnlyCache][ParseAllPersistentAttr] parse byte buffer into address offset fail")
			return err
		}
		if err = binary.Read(bytes_buf, binary.BigEndian, &address.Size); err != nil {
			log.LogDebugf("[ReadOnlyCache][ParseAllPersistentAttr] parse byte buffer into address size fail")
			return err
		}
		var ino uint64
		if err = binary.Read(bytes_buf, binary.BigEndian, &ino); err != nil {
			log.LogDebugf("[ReadOnlyCache][ParseAllPersistentAttr] parse byte buffer into ino fail")
			return err
		}
		// skip the real attr , just read the next address
		i = address.Offset + int64(address.Size)
		persistent_meta_cache.Inode2PersistAttr[ino] = &persistentAttr{Addr: *address}
	}
	return nil
}

// open and read the Dentry file to build Inode2PersistDentry, it will also set DentryBinaryFile correct
func (persistent_meta_cache *ReadOnlyMetaCache) ParseAllPersistentDentry(dentry_file string) error {
	var err error
	persistent_meta_cache.DentryBinaryFile.DataFile, err = os.OpenFile(dentry_file, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0666)
	if err != nil {
		log.LogDebugf("[ReadOnlyCache][ParseAllPersistentAttr] open persisent dentry file fail")
		return err
	}
	// stat the attr file and set file size as EndPosisiton
	info, _ := persistent_meta_cache.DentryBinaryFile.DataFile.Stat()
	persistent_meta_cache.DentryBinaryFile.EndPosition = info.Size()

	buf := make([]byte, 16+8) // 16 bytes for address, 8 bytes for Ino
	bytes_buf := &bytes.Buffer{}
	bytes_buf.Grow(16 + 8)
	for i := int64(0); i < persistent_meta_cache.DentryBinaryFile.EndPosition; {
		address := &addressPointer{}
		if _, err = persistent_meta_cache.DentryBinaryFile.DataFile.ReadAt(buf, i); err != nil {
			log.LogDebugf("[ReadOnlyCache][ParseAllPersistentAttr] read persisent dentry file fail")
			return err
		}
		bytes_buf.Read(buf)
		if err = binary.Read(bytes_buf, binary.BigEndian, &address.Offset); err != nil {
			log.LogDebugf("[ReadOnlyCache][ParseAllPersistentAttr] parse byte buffer into address offset fail")
			return err
		}
		if err = binary.Read(bytes_buf, binary.BigEndian, &address.Size); err != nil {
			log.LogDebugf("[ReadOnlyCache][ParseAllPersistentAttr] parse byte buffer into address size fail")
			return err
		}
		var ino uint64
		if err = binary.Read(bytes_buf, binary.BigEndian, &ino); err != nil {
			log.LogDebugf("[ReadOnlyCache][ParseAllPersistentAttr] parse byte buffer into ino fail")
			return err
		}
		// skip the real attr , just read the next address
		i = address.Offset + int64(address.Size)
		persistent_meta_cache.Inode2PersistDentry[ino] = &persistentDentry{
			DentryHead: *address,
			IsPersist:  true,
		}
	}
	return nil
}

func (persistent_meta_cache *ReadOnlyMetaCache) PutAttr(attr *proto.InodeInfo) error {
	persistent_meta_cache.PersistAttrMtx.Lock()
	defer persistent_meta_cache.PersistAttrMtx.Unlock()

	_, ok := persistent_meta_cache.Inode2PersistAttr[attr.Inode]
	if !ok {
		persistent_attr := &persistentAttr{
			Addr: addressPointer{},
		}
		err := persistent_meta_cache.WriteAttrToFile(attr, persistent_attr)
		if err != nil {
			log.LogDebugf("[ReadOnlyCache][PutAttr] : persist attr to file fail, err: %s, ino: %d", err.Error(), attr.Inode)
			return err
		}

		persistent_meta_cache.Inode2PersistAttr[attr.Inode] = persistent_attr
	}
	return nil
}

func (persistent_meta_cache *ReadOnlyMetaCache) GetAttr(ino uint64, inode_info *proto.InodeInfo) error {
	persistent_meta_cache.PersistAttrMtx.RLock()
	defer persistent_meta_cache.PersistAttrMtx.RUnlock()

	persistent_attr, ok := persistent_meta_cache.Inode2PersistAttr[ino]
	if !ok {
		return errors.New(fmt.Sprintf("inode %d is not exist in read only cache", ino))
	}
	err := persistent_meta_cache.ReadAttrFromFile(&persistent_attr.Addr, inode_info)
	if err != nil {
		log.LogDebugf("[ReadOnlyCache][GetAttr] : get attr from file fail, err : %s, ino: %d", err.Error(), ino)
		return err
	}
	return nil
}

func (persistent_meta_cache *ReadOnlyMetaCache) PutDentry(parentInode uint64, dentries []proto.Dentry, is_end bool) error {
	persistent_meta_cache.PersistDentryMtx.Lock()
	defer persistent_meta_cache.PersistDentryMtx.Unlock()

	var (
		persistent_dentry *persistentDentry
		ok                bool
	)

	persistent_dentry, ok = persistent_meta_cache.Inode2PersistDentry[parentInode]
	if !ok {
		persistent_dentry = &persistentDentry{
			IsPersist:   false,
			EntryBuffer: map[string]dentryData{},
		}
		persistent_meta_cache.Inode2PersistDentry[parentInode] = persistent_dentry
	}

	// add new dentry to entry buffer
	for _, dentry := range dentries {
		if _, ok := persistent_dentry.EntryBuffer[dentry.Name]; !ok {
			persistent_dentry.EntryBuffer[dentry.Name] = dentryData{
				Type: dentry.Type,
				Ino:  dentry.Inode,
			}
		}
	}

	if is_end && !persistent_dentry.IsPersist {
		err := persistent_meta_cache.WriteDentryToFile(parentInode, persistent_dentry)
		if err != nil {
			log.LogDebugf("[ReadOnlyCache][PutDentry] : persist dentry to file fail, err: %s, ino: %d", err.Error(), parentInode)
			return err
		}

		log.LogErrorf("[ReadOnlyCache][PutDentry] : persist dentry success ino: %d", parentInode)
		if persistent_meta_cache.LruList.Len() >= MaxDEntryBufferElement {
			persistent_meta_cache.Evict(true)
		}

		persistent_dentry.IsPersist = true
		persistent_dentry.Expiration = time.Duration(time.Now().Add(DefaultDentryBufferExpiredTime).UnixNano())
		persistent_meta_cache.FullCachedEntryBuffer[parentInode] = persistent_meta_cache.LruList.PushFront(persistent_dentry)
	}

	return nil
}

func (persistent_meta_cache *ReadOnlyMetaCache) Lookup(ino uint64, name string) (uint64, error) {
	persistent_meta_cache.PersistDentryMtx.RLock()
	defer persistent_meta_cache.PersistDentryMtx.RUnlock()

	var (
		persistent_dentry *persistentDentry
		dentry            dentryData
		ok                bool
	)

	persistent_dentry, ok = persistent_meta_cache.Inode2PersistDentry[ino]
	if !ok {
		log.LogDebugf("dentry cache of inode %d is not exist in read only cache", ino)
		return 0, DENTRY_NOT_CACHE
	}

	// entry has been persisted, if EntryBuffer is not cached in memory, loading it into the memory
	if persistent_dentry.IsPersist {
		element, is_full_cached := persistent_meta_cache.FullCachedEntryBuffer[ino]
		if is_full_cached {
			persistent_meta_cache.LruList.MoveToFront(element)
			element.Value.(*persistentDentry).Expiration = time.Duration(time.Now().Add(DefaultDentryBufferExpiredTime).UnixNano())
		} else {
			err := persistent_meta_cache.ReadDentryFromFile(&persistent_dentry.DentryHead, &persistent_dentry.EntryBuffer)
			if err != nil {
				log.LogDebugf("[ReadOnlyCache][Lookup] : get dentry from file fail, err : %s, ino: %d", err.Error(), ino)
				return 0, err
			}
			if persistent_meta_cache.LruList.Len() >= MaxDEntryBufferElement {
				persistent_meta_cache.Evict(true)
			}
			persistent_dentry.Expiration = time.Duration(time.Now().Add(DefaultDentryBufferExpiredTime).UnixNano())
			persistent_meta_cache.FullCachedEntryBuffer[ino] = persistent_meta_cache.LruList.PushFront(persistent_dentry)
		}
	}

	// try to find in EntryBuffer
	if dentry, ok = persistent_dentry.EntryBuffer[name]; ok {
		return dentry.Ino, nil
	}

	if persistent_dentry.IsPersist {
		log.LogDebugf("%s is not found in persistent dentry in read only cache, it is not existed in node %d", name, ino)
		return 0, DENTRY_NOT_EXIST
	} else {
		log.LogDebugf("%s doesn't cache hit in dentry buffer of read only cache, it may be not cached in node %d", name, ino)
		return 0, DENTRY_NOT_CACHE
	}
}

func (persistent_meta_cache *ReadOnlyMetaCache) GetDentry(ino uint64) ([]proto.Dentry, error) {
	persistent_meta_cache.PersistDentryMtx.RLock()
	defer persistent_meta_cache.PersistDentryMtx.RUnlock()

	res := []proto.Dentry{}
	persistent_dentry, ok := persistent_meta_cache.Inode2PersistDentry[ino]
	// don'try to find in EntryBuffer if it has not been persisted, because it may not return complete entries in ino
	if !ok || !persistent_dentry.IsPersist {
		log.LogDebugf("dentry cache of inode %d is not exist completely in read only cache", ino)
		return res, DENTRY_NOT_CACHE
	}

	element, is_full_cached := persistent_meta_cache.FullCachedEntryBuffer[ino]
	if is_full_cached {
		persistent_meta_cache.LruList.MoveToFront(element)
		element.Value.(*persistentDentry).Expiration = time.Duration(time.Now().Add(DefaultDentryBufferExpiredTime).UnixNano())
	} else {
		err := persistent_meta_cache.ReadDentryFromFile(&persistent_dentry.DentryHead, &persistent_dentry.EntryBuffer)
		log.LogDebugf("[ReadOnlyCache][GetDentry] : all_entries size  : %d, ino: %d", len(persistent_dentry.EntryBuffer), ino)
		if err != nil {
			log.LogDebugf("[ReadOnlyCache][GetDentry] : get dentry from file fail, err : %s, ino: %d", err.Error(), ino)
			return res, err
		}
		if persistent_meta_cache.LruList.Len() >= MaxDEntryBufferElement {
			persistent_meta_cache.Evict(true)
		}
		persistent_dentry.Expiration = time.Duration(time.Now().Add(DefaultDentryBufferExpiredTime).UnixNano())
		persistent_meta_cache.FullCachedEntryBuffer[ino] = persistent_meta_cache.LruList.PushFront(persistent_dentry)
	}

	for name, dentry := range persistent_dentry.EntryBuffer {
		res = append(res, proto.Dentry{
			Name:  name,
			Type:  dentry.Type,
			Inode: dentry.Ino,
		})
	}

	log.LogDebugf("[ReadOnlyCache][GetDentry] : num of entry in %d is %d", ino, len(res))
	return res, nil
}

func (persistent_meta_cache *ReadOnlyMetaCache) Evict(foreground bool) {
	for i := 0; i < MinDentryBufferEvictNum; i++ {
		element := persistent_meta_cache.LruList.Back()
		if element == nil {
			break
		}
		persist_dentry := element.Value.(*persistentDentry)
		if !foreground && time.Now().UnixNano() <= int64(persist_dentry.Expiration) {
			return
		}
		persistent_meta_cache.LruList.Remove(element)
		for k := range persist_dentry.EntryBuffer {
			delete(persist_dentry.EntryBuffer, k)
		}
	}

	if foreground {
		return
	}

	for i := 0; i < MaxDentryBufferEvictNum; i++ {
		element := persistent_meta_cache.LruList.Back()
		if element == nil {
			break
		}
		persist_dentry := element.Value.(*persistentDentry)
		// elements after it are not expired, dosen't evict them
		if time.Now().UnixNano() <= int64(persist_dentry.Expiration) {
			break
		}
		persistent_meta_cache.LruList.Remove(element)
		for k := range persist_dentry.EntryBuffer {
			delete(persist_dentry.EntryBuffer, k)
		}
	}
}

func (persistent_meta_cache *ReadOnlyMetaCache) BackgroundEvictionEntryBuffer() {
	t := time.NewTicker(DentryBufferBgEvictionInterval)
	defer t.Stop()

	for range t.C {
		log.LogInfof("[ReadOnlyCache][BackgroundEvictionEntryBuffer]: Start BG evict")
		start := time.Now()
		persistent_meta_cache.PersistDentryMtx.Lock()
		persistent_meta_cache.Evict(false)
		persistent_meta_cache.PersistDentryMtx.Unlock()
		elapsed := time.Since(start)
		log.LogInfof("[ReadOnlyCache][BackgroundEvictionEntryBuffer]: Total dentry cache(%d), cost(%d)ns", persistent_meta_cache.LruList.Len(), elapsed.Nanoseconds())
	}
}

func (persistent_meta_cache *ReadOnlyMetaCache) ReadAttrFromFile(address *addressPointer, attr *proto.InodeInfo) error {
	// log.LogErrorf("[ReadOnlyMetaCache][ReadAttrFromFile] -------")
	buf := make([]byte, address.Size)
	_, err := persistent_meta_cache.AttrBinaryFile.DataFile.ReadAt(buf, address.Offset)
	if err != nil && err != io.EOF {
		return err
	}
	// unmarshal the data
	if err := AttrUnmarshal(buf, attr); err != nil {
		log.LogDebugf("[ReadOnlyMetaCache][ReadAttrFromFile] unmarshal Attr fail")
		return err
	}
	return nil
}

func (persistent_meta_cache *ReadOnlyMetaCache) WriteAttrToFile(attr *proto.InodeInfo, address *persistentAttr) error {
	// log.LogErrorf("[ReadOnlyMetaCache][WriteAttrToFile] -------")
	bytes_buf := &bytes.Buffer{}
	bs, err := AttrMarshal(attr)
	if err != nil {
		return err
	}
	address.Addr.Size = uint64(len(bs))
	address.Addr.Offset = persistent_meta_cache.AttrBinaryFile.EndPosition + 16 // 16 bytes for address
	if err := binary.Write(bytes_buf, binary.BigEndian, &address.Addr.Offset); err != nil {
		log.LogDebugf("[ReadOnlyMetaCache][WriteAttrToFile] writing offset %d to bytes buffer fail", address.Addr.Offset)
		return err
	}
	if err := binary.Write(bytes_buf, binary.BigEndian, &address.Addr.Size); err != nil {
		log.LogDebugf("[ReadOnlyMetaCache][WriteAttrToFile] writing size %d to bytes buffer fail", address.Addr.Size)
		return err
	}
	bytes_buf.Write(bs)
	var length int
	length, err = persistent_meta_cache.AttrBinaryFile.DataFile.WriteAt(bytes_buf.Bytes(), int64(persistent_meta_cache.AttrBinaryFile.EndPosition))
	if err != nil {
		log.LogDebugf("[ReadOnlyMetaCache][WriteAttrToFile] writing inode %d to binary file fail", attr.Inode)
		return err
	}
	persistent_meta_cache.AttrBinaryFile.EndPosition += int64(length)
	return nil
}

func (persistent_meta_cache *ReadOnlyMetaCache) ReadDentryFromFile(address *addressPointer, entries *map[string]dentryData) error {
	// log.LogErrorf("[ReadOnlyMetaCache][ReadAttrFromFile] -------")
	buf := make([]byte, address.Size)
	_, err := persistent_meta_cache.DentryBinaryFile.DataFile.ReadAt(buf, address.Offset)
	if err != nil {
		if err == io.EOF {
			log.LogDebugf("[ReadOnlyMetaCache][ReadDentryFromFile] buf read data from file fail .err(%s) ", err.Error())
		}
		return err
	}
	if err := DentryBatchUnMarshal(buf, entries); err != nil {
		log.LogDebugf("[ReadOnlyMetaCache][ReadDentryFromFile] unmarshal all entries fail")
		return err
	}

	return nil
}

// write all dentry of one directory to the DentryFile
func (persistent_meta_cache *ReadOnlyMetaCache) WriteDentryToFile(parentIno uint64, persistent_dentry *persistentDentry) error {
	// log.LogErrorf("[ReadOnlyMetaCache][WriteDentryToFile] -------")
	bytes_buf := &bytes.Buffer{}
	bs, err := DentryBatchMarshal(parentIno, &(persistent_dentry.EntryBuffer))
	if err != nil {
		return err
	}
	persistent_dentry.DentryHead.Size = uint64(len(bs))
	persistent_dentry.DentryHead.Offset = persistent_meta_cache.DentryBinaryFile.EndPosition + 16 // 16 bytes for address
	if err := binary.Write(bytes_buf, binary.BigEndian, &persistent_dentry.DentryHead.Offset); err != nil {
		log.LogDebugf("[ReadOnlyMetaCache][WriteDentryToFile] writing offset %d to bytes buffer fail", persistent_dentry.DentryHead.Offset)
		return err
	}
	if err := binary.Write(bytes_buf, binary.BigEndian, &persistent_dentry.DentryHead.Size); err != nil {
		log.LogDebugf("[ReadOnlyMetaCache][WriteDentryToFile] writing size %d to bytes buffer fail", persistent_dentry.DentryHead.Size)
		return err
	}
	bytes_buf.Write(bs)
	var length int
	length, err = persistent_meta_cache.DentryBinaryFile.DataFile.WriteAt(bytes_buf.Bytes(), int64(persistent_meta_cache.DentryBinaryFile.EndPosition))
	if err != nil {
		log.LogDebugf("[ReadOnlyMetaCache][WriteDentryToFile] writing dentry of inode %d to binary file fail", parentIno)
		return err
	}
	persistent_meta_cache.DentryBinaryFile.EndPosition += int64(length)
	return nil
}

func DentryBatchMarshal(parentIno uint64, entries *map[string]dentryData) ([]byte, error) {
	bytes_buf := bytes.NewBuffer(make([]byte, 0))
	if err := binary.Write(bytes_buf, binary.BigEndian, &parentIno); err != nil {
		log.LogDebugf("[ReadOnlyMetaCache][DentryBatchMarshal] writing parent ino %d to bytes buffer fail", parentIno)
		return nil, err
	}
	if err := binary.Write(bytes_buf, binary.BigEndian, uint32(len(*entries))); err != nil {
		log.LogDebugf("[ReadOnlyMetaCache][DentryBatchMarshal] writing len of entries fail")
		return nil, err
	}
	for k, v := range *entries {
		bs, err := DentryMarshal(k, v)
		if err != nil {
			log.LogDebugf("[ReadOnlyMetaCache][DentryBatchMarshal] marshal entry[%s, %d, %d] fail", k, v.Ino, v.Type)
			return nil, err
		}
		if err = binary.Write(bytes_buf, binary.BigEndian, uint32(len(bs))); err != nil {
			log.LogDebugf("[ReadOnlyMetaCache][DentryBatchMarshal] write len of entry to byte buffer fail")
			return nil, err
		}
		if _, err := bytes_buf.Write(bs); err != nil {
			return nil, err
		}
	}
	return bytes_buf.Bytes(), nil
}

func DentryBatchUnMarshal(raw []byte, entries *map[string]dentryData) error {
	bytes_buf := bytes.NewBuffer(raw)
	var parentIno uint64
	if err := binary.Read(bytes_buf, binary.BigEndian, &parentIno); err != nil {
		log.LogDebugf("[ReadOnlyMetaCache][DentryBatchUnMarshal] parse bytes buffer data to parent Ino fail")
		return err
	}

	var batchLen uint32
	if err := binary.Read(bytes_buf, binary.BigEndian, &batchLen); err != nil {
		log.LogDebugf("[ReadOnlyMetaCache][DentryBatchUnMarshal] parse bytes buffer data to the count  of entries fail")
		return err
	}
	var dataLen uint32
	var err error
	for i := 0; i < int(batchLen); i++ {
		if err = binary.Read(bytes_buf, binary.BigEndian, &dataLen); err != nil {
			return err
		}
		data := make([]byte, int(dataLen))
		if _, err = bytes_buf.Read(data); err != nil {
			return err
		}
		var (
			name        string
			dentry_data dentryData
		)
		if name, dentry_data, err = DentryUnmarshal(data); err != nil {
			log.LogDebugf("[ReadOnlyMetaCache][DentryBatchUnMarshal] unmarsal %d entry fail ", i)
			return err
		}
		(*entries)[name] = dentry_data
	}
	return nil
}

// DentryMarshal marshals the dentry into a byte array
func DentryMarshal(name string, data dentryData) ([]byte, error) {
	bytes_buf := bytes.NewBuffer(make([]byte, 0))
	if err := binary.Write(bytes_buf, binary.BigEndian, uint32(len(name))); err != nil {
		log.LogDebugf("[ReadOnlyMetaCache][DentryMarshal] write len of entry %s to byte buffer fail", name)
		return nil, err
	}
	bytes_buf.Write([]byte(name))
	if err := binary.Write(bytes_buf, binary.BigEndian, &data.Ino); err != nil {
		log.LogDebugf("[ReadOnlyMetaCache][DentryMarshal] write the ino of entry[%s, %d, %d] to byte buffer fail", name, data.Ino, data.Type)
		return nil, err
	}
	if err := binary.Write(bytes_buf, binary.BigEndian, &data.Type); err != nil {
		log.LogDebugf("[ReadOnlyMetaCache][DentryMarshal] write the type of entry[%s, %d, %d] to byte buffer fail", name, data.Ino, data.Type)
		return nil, err
	}
	return bytes_buf.Bytes(), nil
}

// DentryUnmarshal unmarshals one byte array into the dentry
func DentryUnmarshal(raw []byte) (string, dentryData, error) {
	bytes_buf := bytes.NewBuffer(raw)
	var nameLen uint32
	var dentry_data dentryData
	if err := binary.Read(bytes_buf, binary.BigEndian, &nameLen); err != nil {
		log.LogDebugf("[ReadOnlyMetaCache][DentryMarshal] parse byte buffer to len of entry name fail")
		return "", dentry_data, err
	}
	data := make([]byte, int(nameLen))
	bytes_buf.Read(data)
	name := string(data)
	if err := binary.Read(bytes_buf, binary.BigEndian, &dentry_data.Ino); err != nil {
		log.LogDebugf("[ReadOnlyMetaCache][DentryMarshal] parse byte buffer to entry ino fail")
		return name, dentry_data, err
	}
	if err := binary.Read(bytes_buf, binary.BigEndian, &dentry_data.Type); err != nil {
		log.LogDebugf("[ReadOnlyMetaCache][DentryMarshal] parse byte buffer to entry Type fail")
		return name, dentry_data, err
	}
	return name, dentry_data, nil
}

func AttrMarshal(a *proto.InodeInfo) ([]byte, error) {
	var err error
	buff := bytes.NewBuffer(make([]byte, 0, 128))
	buff.Grow(64)
	if err = binary.Write(buff, binary.BigEndian, &a.Inode); err != nil {
		panic(err)
	}
	if err = binary.Write(buff, binary.BigEndian, &a.Mode); err != nil {
		panic(err)
	}
	if err = binary.Write(buff, binary.BigEndian, &a.Nlink); err != nil {
		panic(err)
	}
	if err = binary.Write(buff, binary.BigEndian, &a.Size); err != nil {
		panic(err)
	}
	if err = binary.Write(buff, binary.BigEndian, &a.Uid); err != nil {
		panic(err)
	}
	if err = binary.Write(buff, binary.BigEndian, &a.Gid); err != nil {
		panic(err)
	}
	if err = binary.Write(buff, binary.BigEndian, &a.Generation); err != nil {
		panic(err)
	}
	if err = binary.Write(buff, binary.BigEndian, a.CreateTime.Unix()); err != nil {
		panic(err)
	}
	if err = binary.Write(buff, binary.BigEndian, a.AccessTime.Unix()); err != nil {
		panic(err)
	}
	if err = binary.Write(buff, binary.BigEndian, a.ModifyTime.Unix()); err != nil {
		panic(err)
	}
	// write Target
	targetSize := uint32(len(a.Target))
	if err = binary.Write(buff, binary.BigEndian, &targetSize); err != nil {
		panic(err)
	}
	if _, err = buff.Write(a.Target); err != nil {
		panic(err)
	}
	return buff.Bytes(), nil
}

func AttrUnmarshal(raw []byte, a *proto.InodeInfo) error {
	buff := bytes.NewBuffer(raw)
	var err error
	if err = binary.Read(buff, binary.BigEndian, &a.Inode); err != nil {
		log.LogDebugf("[ReadOnlyMetaCache][AttrUnmarshal] parse byte buffer to a.Inode fail")
		return err
	}
	if err = binary.Read(buff, binary.BigEndian, &a.Mode); err != nil {
		log.LogDebugf("[ReadOnlyMetaCache][AttrUnmarshal] parse byte buffer to a.Mode fail")
		return err
	}
	if err = binary.Read(buff, binary.BigEndian, &a.Nlink); err != nil {
		log.LogDebugf("[ReadOnlyMetaCache][AttrUnmarshal] parse byte buffer to a.Nlink fail")
		return err
	}
	if err = binary.Read(buff, binary.BigEndian, &a.Size); err != nil {
		log.LogDebugf("[ReadOnlyMetaCache][AttrUnmarshal] parse byte buffer to a.Size fail")
		return err
	}
	if err = binary.Read(buff, binary.BigEndian, &a.Uid); err != nil {
		log.LogDebugf("[ReadOnlyMetaCache][AttrUnmarshal] parse byte buffer to a.Uid fail")
		return err
	}
	if err = binary.Read(buff, binary.BigEndian, &a.Gid); err != nil {
		log.LogDebugf("[ReadOnlyMetaCache][AttrUnmarshal] parse byte buffer to a.Gid fail")
		return err
	}
	if err = binary.Read(buff, binary.BigEndian, &a.Generation); err != nil {
		log.LogDebugf("[ReadOnlyMetaCache][AttrUnmarshal] parse byte buffer to a.Generation fail")
		return err
	}

	var time_unix int64
	err = binary.Read(buff, binary.BigEndian, &time_unix)
	if err != nil {
		log.LogDebugf("[ReadOnlyMetaCache][AttrUnmarshal] parse byte buffer to time_unix fail")
		return err
	}
	a.CreateTime = time.Unix(time_unix, 0)

	err = binary.Read(buff, binary.BigEndian, &time_unix)
	if err != nil {
		log.LogDebugf("[ReadOnlyMetaCache][AttrUnmarshal] parse byte buffer to time_unix fail")
		return err
	}
	a.AccessTime = time.Unix(time_unix, 0)

	err = binary.Read(buff, binary.BigEndian, &time_unix)
	if err != nil {
		log.LogDebugf("[ReadOnlyMetaCache][AttrUnmarshal] parse byte buffer to time_unix fail")
		return err
	}
	a.ModifyTime = time.Unix(time_unix, 0)

	// read Target
	targetSize := uint32(0)
	if err = binary.Read(buff, binary.BigEndian, &targetSize); err != nil {
		log.LogDebugf("[ReadOnlyMetaCache][AttrUnmarshal] parse byte buffer to targetSize fail")
		return err
	}
	if targetSize > 0 {
		a.Target = make([]byte, targetSize)
		if _, err = io.ReadFull(buff, a.Target); err != nil {
			log.LogDebugf("[ReadOnlyMetaCache][AttrUnmarshal] read target of Inode %d fail", a.Inode)
		}
	}
	return nil
}
