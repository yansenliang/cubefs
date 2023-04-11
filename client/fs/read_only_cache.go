package fs

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/log"
)

type ReadOnlyCacheDataType int8

const (
	DATA_ATTR   ReadOnlyCacheDataType = 1 // data is ATTR
	DATA_DENTRY ReadOnlyCacheDataType = 2 // data is dentry
)

type addressPointer struct {
	Offset int64
	Size   uint64
}

type persistentAttr struct {
	Attr addressPointer //  attr address
}

type persistentDentry struct {
	DentryHead addressPointer // dentry address
	DentryTail addressPointer // dentry address
}

type Dentry struct {
	Name string
	Ino  uint64
}
type Attr struct {
	Inode      uint64
	Mode       uint32
	Nlink      uint32
	Size       uint64
	Uid        uint32
	Gid        uint32
	Generation uint64
	ModifyTime int64
	CreateTime int64
	AccessTime int64
	Target     []byte
}
type persistentFileHandler struct {
	DataFile    *os.File
	EndPosition int64
}

type ReadOnlyMetaCache struct {
	sync.RWMutex
	AttrBinaryFile      *persistentFileHandler       // AttrBinary File's Handle
	DentryBinaryFile    *persistentFileHandler       // DentryBinary File's Handle
	Inode2PersistAttr   map[uint64]*persistentAttr   // transfer inode to persisent attr
	Inode2PersistDentry map[uint64]*persistentDentry // transfer inode to persisent dentry
}

func NewReadOnlyMetaCache(sub_dir string) (*ReadOnlyMetaCache, error) {
	meta_cache := &ReadOnlyMetaCache{
		AttrBinaryFile:      nil,
		DentryBinaryFile:    nil,
		Inode2PersistAttr:   make(map[uint64]*persistentAttr),
		Inode2PersistDentry: make(map[uint64]*persistentDentry),
	}
	attr_file_path := sub_dir + "read_only_attr_cache"
	dentry_file_path := sub_dir + "read_only_dentry_cache"
	if err := meta_cache.ParseAllPersistentAttr(attr_file_path); err != nil {
		log.LogErrorf("ReadOnlyMetaCache][NewReadOnlyMetaCache] parse attr file fail")
		return meta_cache, err
	}
	if err := meta_cache.ParseAllPersistentDentry(dentry_file_path); err != nil {
		log.LogErrorf("ReadOnlyMetaCache][NewReadOnlyMetaCache] parse dentry file fail")
		return meta_cache, err
	}
	return meta_cache, nil
}

// open and read the Attr file to build Inode2PersistAttr, it will also set AttrBinaryFile correct
func (persistent_meta_cache *ReadOnlyMetaCache) ParseAllPersistentAttr(attr_file_path string) error {
	var err error
	persistent_meta_cache.AttrBinaryFile.DataFile, err = os.OpenFile(attr_file_path, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0666)
	// stat the attr file and set file size as EndPosisiton
	info, _ := persistent_meta_cache.AttrBinaryFile.DataFile.Stat()
	persistent_meta_cache.AttrBinaryFile.EndPosition = info.Size()

	buf := make([]byte, 16+8) // 16 bytes for address, 8 bytes for Ino
	bytes_buf := &bytes.Buffer{}
	bytes_buf.Grow(16 + 8)
	for i := int64(0); i < persistent_meta_cache.AttrBinaryFile.EndPosition; {
		address := &addressPointer{}
		persistent_meta_cache.AttrBinaryFile.DataFile.ReadAt(buf, i)
		bytes_buf.Read(buf)
		if err = binary.Read(bytes_buf, binary.BigEndian, &address.Offset); err != nil {
			log.LogErrorf("[ReadOnlyCache][ParseAllPersistentAttr] parse byte buffer into address offset fail")
		}
		if err = binary.Read(bytes_buf, binary.BigEndian, &address.Size); err != nil {
			log.LogErrorf("[ReadOnlyCache][ParseAllPersistentAttr] parse byte buffer into address size fail")
		}
		var ino uint64
		if err = binary.Read(bytes_buf, binary.BigEndian, &ino); err != nil {
			log.LogErrorf("[ReadOnlyCache][ParseAllPersistentAttr] parse byte buffer into ino fail")
		}
		// skip the real attr , just read the next address
		i = address.Offset + int64(address.Size)
		persistent_meta_cache.Inode2PersistAttr[ino] = &persistentAttr{Attr: *address}
	}
	return nil
}

// open and read the Dentry file to build Inode2PersistDentry, it will also set DentryBinaryFile correct
func (persistent_meta_cache *ReadOnlyMetaCache) ParseAllPersistentDentry(dentry_file string) error {
	var err error
	persistent_meta_cache.DentryBinaryFile.DataFile, err = os.OpenFile(dentry_file, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0666)
	// stat the attr file and set file size as EndPosisiton
	info, _ := persistent_meta_cache.DentryBinaryFile.DataFile.Stat()
	persistent_meta_cache.DentryBinaryFile.EndPosition = info.Size()

	buf := make([]byte, 16+8) // 16 bytes for address, 8 bytes for Ino
	bytes_buf := &bytes.Buffer{}
	bytes_buf.Grow(16 + 8)
	for i := int64(0); i < persistent_meta_cache.DentryBinaryFile.EndPosition; {
		address := &addressPointer{}
		persistent_meta_cache.DentryBinaryFile.DataFile.ReadAt(buf, i)
		bytes_buf.Read(buf)
		if err = binary.Read(bytes_buf, binary.BigEndian, &address.Offset); err != nil {
			log.LogErrorf("[ReadOnlyCache][ParseAllPersistentAttr] parse byte buffer into address offset fail")
		}
		if err = binary.Read(bytes_buf, binary.BigEndian, &address.Size); err != nil {
			log.LogErrorf("[ReadOnlyCache][ParseAllPersistentAttr] parse byte buffer into address size fail")
		}
		var ino uint64
		if err = binary.Read(bytes_buf, binary.BigEndian, &ino); err != nil {
			log.LogErrorf("[ReadOnlyCache][ParseAllPersistentAttr] parse byte buffer into ino fail")
		}
		// skip the real attr , just read the next address
		i = address.Offset + int64(address.Size)
		persistent_meta_cache.Inode2PersistDentry[ino] = &persistentDentry{DentryHead: *address}
	}
	return nil
}

func (persistent_meta_cache *ReadOnlyMetaCache) PutAttr(attr *proto.InodeInfo) error {
	persistent_attr, ok := persistent_meta_cache.Inode2PersistAttr[attr.Inode]
	if !ok {
		bytes_buf := &bytes.Buffer{}
		err := binary.Write(bytes_buf, binary.BigEndian, *persistent_attr)
		if err != nil {
			log.LogErrorf("[ReadOnlyCache][PutAttr] : transfer attr to bytes fail, err: %s, ino: %d", err.Error(), attr.Inode)
			return err
		}
		persistent_attr_addr, err := persistent_meta_cache.WriteFile(DATA_ATTR, bytes_buf.Bytes())
		if err != nil {
			log.LogErrorf("[ReadOnlyCache][PutAttr] : persist attr to file fail, err: %s, ino: %d", err.Error(), attr.Inode)
			return err
		}
		persistent_meta_cache.Inode2PersistAttr[attr.Inode] = &persistentAttr{
			Attr: *persistent_attr_addr[0],
		}
	}
	return nil
}

func (persistent_meta_cache *ReadOnlyMetaCache) PutDentry(parentInode uint64, dentry map[string]uint64) error {
	bytes_buf := &bytes.Buffer{}
	err := binary.Write(bytes_buf, binary.BigEndian, dentry)
	if err != nil {
		log.LogErrorf("[ReadOnlyCache][PutDentry] : transfer dentry to bytes fail, err: %s, ino: %d", err.Error(), parentInode)
		return err
	}
	persistent_dentry_addrs, err := persistent_meta_cache.WriteFile(DATA_DENTRY, bytes_buf.Bytes())
	if err != nil {
		log.LogErrorf("[ReadOnlyCache][PutDentry] : persist dentry to file fail, err: %s, ino: %d", err.Error(), parentInode)
		return err
	}
	persistent_meta_cache.Inode2PersistDentry[parentInode] = &persistentDentry{
		DentryHead: *persistent_dentry_addrs[0],
		DentryTail: *persistent_dentry_addrs[1],
	}
	return nil
}

func (persistent_meta_cache *ReadOnlyMetaCache) GetAttr(ino uint64) (*proto.InodeInfo, error) {
	persistent_attr, ok := persistent_meta_cache.Inode2PersistAttr[ino]
	if !ok {
		return nil, errors.New(fmt.Sprintf("inode %d is not exist in read only cache", ino))
	}
	attr_bytes, err := persistent_meta_cache.ReadFile(DATA_ATTR, &persistent_attr.Attr)
	if err != nil {
		log.LogErrorf("[ReadOnlyCache][GetAttr] : get attr from file fail, err : %s, ino: %d", err.Error(), ino)
		return nil, err
	}
	bytes_buf := &bytes.Buffer{}
	_, err = bytes_buf.Read(attr_bytes)
	if err != nil {
		log.LogErrorf("[ReadOnlyCache][GetAttr] : bytes buffer read data from buf fail, err : %s, ino: %d", err.Error(), ino)
		return nil, err
	}
	inode_info := &proto.InodeInfo{}
	err = binary.Read(bytes_buf, binary.BigEndian, inode_info)
	if err != nil {
		log.LogErrorf("[ReadOnlyCache][GetAttr] : parse bytes buffe data to attr fail, err : %s, ino: %d", err.Error(), ino)
		return nil, err
	}
	return inode_info, nil
}

func (persistent_meta_cache *ReadOnlyMetaCache) GetDentry(ino uint64, names []string) (map[string]uint64, error) {
	persistent_dentry, ok := persistent_meta_cache.Inode2PersistDentry[ino]
	if !ok {
		return nil, errors.New(fmt.Sprintf("dentry cache of inode %d is not exist in read only cache", ino))
	}
	dentry_bytes, err := persistent_meta_cache.ReadFile(DATA_DENTRY, &persistent_dentry.DentryHead)
	if err != nil {
		log.LogErrorf("[ReadOnlyCache][GetDentry] : get dentry from file fail, err : %s, ino: %d", err.Error(), ino)
		return nil, err
	}
	bytes_buf := &bytes.Buffer{}
	_, err = bytes_buf.Read(dentry_bytes)
	if err != nil {
		log.LogErrorf("[ReadOnlyCache][GetDentry] : bytes buffer read data from buf fail, err : %s, ino: %d", err.Error(), ino)
		return nil, err
	}
	dentry := &map[string]uint64{}
	err = binary.Read(bytes_buf, binary.BigEndian, dentry)
	if err != nil {
		log.LogErrorf("[ReadOnlyCache][GetDentry] : parse bytes buffe data to dentry fail, err : %s, ino: %d", err.Error(), ino)
		return nil, err
	}
	res := map[string]uint64{}
	for _, name := range names {
		if inode, ok := (*dentry)[name]; ok {
			res[name] = inode
		} else {
			log.LogInfof("[ReadOnlyCache][GetDentry] : inode is not cache in dentry. parentIno: %d, name: %d", ino, name)
		}
	}
	return res, nil
}

func (persistent_meta_cache *ReadOnlyMetaCache) ReadAttrFromFile(address *addressPointer, attr *Attr) error {
	buf := make([]byte, address.Size)
	_, err := persistent_meta_cache.AttrBinaryFile.DataFile.ReadAt(buf, address.Offset)
	if err != nil && err != io.EOF {
		return err
	}
	// unmarshal the data
	if err := attr.AttrUnmarshal(buf); err != nil {
		log.LogErrorf("ReadOnlyMetaCache][ReadAttrFromFile] unmarshal Attr fail")
		return err
	}
	return nil
}

func (persistent_meta_cache *ReadOnlyMetaCache) ReadDentryFromFile(address *addressPointer, entries *[]Dentry) error {
	bytes_buf := &bytes.Buffer{}
	buf := make([]byte, address.Size)
	_, err := persistent_meta_cache.DentryBinaryFile.DataFile.ReadAt(buf, address.Offset)
	if err != nil && err != io.EOF {
		return err
	}
	_, err = bytes_buf.Read(buf)
	if err != nil {
		log.LogErrorf("ReadOnlyMetaCache][ReadDentryFromFile] bytes buffer read data from buf fail ")
		return err
	}
	var parentIno uint64
	if err = binary.Read(bytes_buf, binary.BigEndian, &parentIno); err != nil {
		log.LogErrorf("ReadOnlyMetaCache][ReadDentryFromFile] parse bytes buffer data to parent Ino fail")
		return err
	}
	if err := DentryBatchUnMarshal(bytes_buf.Bytes(), entries); err != nil {
		log.LogErrorf("ReadOnlyMetaCache][ReadDentryFromFile] unmarshal all entries fail")
		return err
	}

	return nil
}

func (persistent_meta_cache *ReadOnlyMetaCache) WriteAttrToFile(attr *Attr, address *persistentAttr) error {
	bytes_buf := &bytes.Buffer{}
	bs, err := attr.AttrMarshal()
	if err != nil {
		return err
	}
	address.Attr.Size = uint64(len(bs))
	address.Attr.Offset = persistent_meta_cache.AttrBinaryFile.EndPosition + 16 // 16 bytes for address
	if err := binary.Write(bytes_buf, binary.BigEndian, &address.Attr.Offset); err != nil {
		log.LogErrorf("ReadOnlyMetaCache][WriteAttrToFile] writing offset %d to bytes buffer fail", address.Attr.Offset)
		return err
	}
	if err := binary.Write(bytes_buf, binary.BigEndian, &address.Attr.Size); err != nil {
		log.LogErrorf("ReadOnlyMetaCache][WriteAttrToFile] writing size %d to bytes buffer fail", address.Attr.Size)
		return err
	}
	bytes_buf.Write(bs)
	var length int
	length, err = persistent_meta_cache.AttrBinaryFile.DataFile.WriteAt(bytes_buf.Bytes(), int64(persistent_meta_cache.AttrBinaryFile.EndPosition))
	if err != nil {
		log.LogErrorf("ReadOnlyMetaCache][WriteAttrToFile] writing inode %d to binary file fail", attr.Inode)
		return err
	}
	persistent_meta_cache.AttrBinaryFile.EndPosition += int64(length)
	return nil
}

// write all dentry of one directory to the DentryFile
func (persistent_meta_cache *ReadOnlyMetaCache) WriteDentryToFile(parentIno uint64, entries *[]Dentry, address *persistentDentry) error {
	bytes_buf := &bytes.Buffer{}
	bs, err := DentryBatchMarshal(entries)
	if err != nil {
		return err
	}
	address.DentryHead.Size = uint64(len(bs) + 8)                                       // 8 bytes for parentIno
	address.DentryHead.Offset = persistent_meta_cache.DentryBinaryFile.EndPosition + 16 // 16 bytes for address
	if err := binary.Write(bytes_buf, binary.BigEndian, &address.DentryHead.Offset); err != nil {
		log.LogErrorf("ReadOnlyMetaCache][WriteDentryToFile] writing offset %d to bytes buffer fail", address.DentryHead.Offset)
		return err
	}
	if err := binary.Write(bytes_buf, binary.BigEndian, &address.DentryHead.Size); err != nil {
		log.LogErrorf("ReadOnlyMetaCache][WriteDentryToFile] writing size %d to bytes buffer fail", address.DentryHead.Size)
		return err
	}
	if err := binary.Write(bytes_buf, binary.BigEndian, &parentIno); err != nil {
		log.LogErrorf("ReadOnlyMetaCache][WriteDentryToFile] writing parent ino %d to bytes buffer fail", parentIno)
		return err
	}
	bytes_buf.Write(bs)
	var length int
	length, err = persistent_meta_cache.AttrBinaryFile.DataFile.WriteAt(bytes_buf.Bytes(), int64(persistent_meta_cache.DentryBinaryFile.EndPosition))
	if err != nil {
		log.LogErrorf("ReadOnlyMetaCache][WriteDentryToFile] writing dentry of inode %d to binary file fail", parentIno)
		return err
	}
	persistent_meta_cache.DentryBinaryFile.EndPosition += int64(length)
	return nil
}

func DentryBatchMarshal(entries *[]Dentry) ([]byte, error) {
	bytes_buf := bytes.NewBuffer(make([]byte, 0))
	if err := binary.Write(bytes_buf, binary.BigEndian, uint32(len(*entries))); err != nil {
		return nil, err
	}
	for _, v := range *entries {
		bs, err := v.DentryMarshal()
		if err != nil {
			log.LogErrorf("ReadOnlyMetaCache][DentryBatchMarshal] marshal entry[%s, %d] fail", v.Name, v.Ino)
			return nil, err
		}
		if err = binary.Write(bytes_buf, binary.BigEndian, uint32(len(bs))); err != nil {
			log.LogErrorf("ReadOnlyMetaCache][DentryBatchMarshal] write len of entry to byte buffer fail")
			return nil, err
		}
		if _, err := bytes_buf.Write(bs); err != nil {
			return nil, err
		}
	}
	return bytes_buf.Bytes(), nil
}
func DentryBatchUnMarshal(raw []byte, entries *[]Dentry) error {
	bytes_buf := bytes.NewBuffer(raw)
	var batchLen uint32
	if err := binary.Read(bytes_buf, binary.BigEndian, &batchLen); err != nil {
		log.LogErrorf("ReadOnlyMetaCache][DentryBatchUnMarshal] parse bytes buffer data to the count  of entries fail")
		return err
	}
	var dataLen uint32
	for i := 0; i < int(batchLen); i++ {
		if err := binary.Read(bytes_buf, binary.BigEndian, &dataLen); err != nil {
			return err
		}
		data := make([]byte, int(dataLen))
		if _, err := bytes_buf.Read(data); err != nil {
			return err
		}
		den := &Dentry{}
		if err := den.DentryUnmarshal(data); err != nil {
			log.LogErrorf("ReadOnlyMetaCache][DentryBatchUnMarshal] unmarsal %d entry fail ", i)
			return err
		}
		*entries = append(*entries, *den)
	}
	return nil
}

// DentryMarshal marshals the dentry into a byte array
func (d *Dentry) DentryMarshal() ([]byte, error) {
	bytes_buf := bytes.NewBuffer(make([]byte, 0))
	if err := binary.Write(bytes_buf, binary.BigEndian, uint32(len(d.Name))); err != nil {
		log.LogErrorf("ReadOnlyMetaCache][DentryMarshal] write len of entry %s to byte buffer fail", d.Name)
		return nil, err
	}
	bytes_buf.Write([]byte(d.Name))
	if err := binary.Write(bytes_buf, binary.BigEndian, &d.Ino); err != nil {
		log.LogErrorf("ReadOnlyMetaCache][DentryMarshal] write entry ino %d to byte buffer fail", d.Ino)
		return nil, err
	}
	return bytes_buf.Bytes(), nil
}

// DentryUnmarshal unmarshals one byte array into the dentry
func (d *Dentry) DentryUnmarshal(raw []byte) error {
	bytes_buf := bytes.NewBuffer(raw)
	var nameLen uint32
	if err := binary.Read(bytes_buf, binary.BigEndian, &nameLen); err != nil {
		log.LogErrorf("ReadOnlyMetaCache][DentryMarshal] parse byte buffer to len of entry name fail")
		return err
	}
	data := make([]byte, int(nameLen))
	bytes_buf.Read(data)
	d.Name = string(data)
	if err := binary.Read(bytes_buf, binary.BigEndian, &d.Ino); err != nil {
		log.LogErrorf("ReadOnlyMetaCache][DentryMarshal] parse byte buffer to entry ino fail")
		return err
	}
	return nil
}

func (a *Attr) AttrMarshal() ([]byte, error) {
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
	if err = binary.Write(buff, binary.BigEndian, &a.CreateTime); err != nil {
		panic(err)
	}
	if err = binary.Write(buff, binary.BigEndian, &a.AccessTime); err != nil {
		panic(err)
	}
	if err = binary.Write(buff, binary.BigEndian, &a.ModifyTime); err != nil {
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

func (a *Attr) AttrUnmarshal(raw []byte) error {
	buff := bytes.NewBuffer(raw)
	var err error
	if err = binary.Read(buff, binary.BigEndian, &a.Inode); err != nil {
		return err
	}
	if err = binary.Read(buff, binary.BigEndian, &a.Mode); err != nil {
		return err
	}
	if err = binary.Read(buff, binary.BigEndian, &a.Nlink); err != nil {
		return err
	}
	if err = binary.Read(buff, binary.BigEndian, &a.Size); err != nil {
		return err
	}
	if err = binary.Read(buff, binary.BigEndian, &a.Uid); err != nil {
		return err
	}
	if err = binary.Read(buff, binary.BigEndian, &a.Gid); err != nil {
		return err
	}
	if err = binary.Read(buff, binary.BigEndian, &a.Generation); err != nil {
		return err
	}
	if err = binary.Read(buff, binary.BigEndian, &a.CreateTime); err != nil {
		return err
	}
	if err = binary.Read(buff, binary.BigEndian, &a.AccessTime); err != nil {
		return err
	}
	if err = binary.Read(buff, binary.BigEndian, &a.ModifyTime); err != nil {
		return err
	}
	// read Target
	targetSize := uint32(0)
	if err = binary.Read(buff, binary.BigEndian, &targetSize); err != nil {
		return err
	}
	if targetSize > 0 {
		a.Target = make([]byte, targetSize)
		if _, err = io.ReadFull(buff, a.Target); err != nil {
			log.LogErrorf("ReadOnlyMetaCache][AttrUnmarshal] read target of Inode %d fail", a.Inode)
		}
	}
	return nil
}
