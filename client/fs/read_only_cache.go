package fs

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
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
	Size   int64
}

type persistentAttr struct {
	Attr addressPointer //  attr address
}

type persistentDentry struct {
	DentryHead addressPointer // dentry address
	DentryTail addressPointer // dentry address
}

type Dentry struct {
	Ino     uint64
	Index   int8
	Next    addressPointer // when Next is nil, it
	Size    uint32         // bytes occupied by field Entries
	Entries []byte
}

type persistentFileHandler struct {
	DataFile    *os.File
	EndPosition uint64
}

type ReadOnlyMetaCache struct {
	sync.RWMutex
	AttrBinaryFile      *persistentFileHandler       // AttrBinary File's Handle
	DentryBinaryFile    *persistentFileHandler       // DentryBinary File's Handle
	Inode2PersistAttr   map[uint64]*persistentAttr   // transfer inode to persisent attr
	Inode2PersistDentry map[uint64]*persistentDentry // transfer inode to persisent dentry
}

func NewReadOnlyMetaCache() (*ReadOnlyMetaCache, error) {
	return nil, nil
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

func (persistent_meta_cache *ReadOnlyMetaCache) ReadFile(dataType ReadOnlyCacheDataType, address *addressPointer) ([]byte, error) {
	return nil, nil
}

func (persistent_meta_cache *ReadOnlyMetaCache) WriteFile(dataType ReadOnlyCacheDataType, data []byte) ([]*addressPointer, error) {
	return nil, nil
}
