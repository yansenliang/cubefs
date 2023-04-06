package fs

import (
	"os"
	"sync"

	"github.com/cubefs/cubefs/proto"
)

type addressPointer struct {
	Offset int64
	Size   int64
}

type persistentInode struct {
	FileType   uint8
	Attr       addressPointer //  attr address
	DentryHead addressPointer // dentry address
	DentryTail addressPointer // dentry address
}

type Dentry struct {
	Index   int8
	Next    addressPointer // when Next is nil, it
	Size    uint32
	Entries map[string]uint64
}

type persistentFileHandler struct {
	DataFile    *os.File
	EndPosition uint64
}

type ReadOnlyMetaCache struct {
	sync.RWMutex
	AttrBinaryFile     *persistentFileHandler      // AttrBinary File's Handle
	DentryBinaryFile   *persistentFileHandler      // DentryBinary File's Handle
	Inode2PersistInode map[uint64]*persistentInode // transfer inode to persisent inode
}

func NewReadOnlyMetaCache() (*ReadOnlyMetaCache, error) {
	return nil, nil
}

func (persistent_meta_cache *ReadOnlyMetaCache) PutAttr(attr *proto.InodeInfo) error {
	return nil
}
func (persistent_meta_cache *ReadOnlyMetaCache) PutDentry(parentInode uint64, dentry map[string]uint64) error {
	return nil
}

func (persistent_meta_cache *ReadOnlyMetaCache) GetAttr(ino uint64) (*proto.InodeInfo, error) {
	return nil, nil
}

func (persistent_meta_cache *ReadOnlyMetaCache) GetDentry(ino uint64, names []string) (map[string]uint64, error) {
	return nil, nil
}

func (persistent_meta_cache *ReadOnlyMetaCache) readFile(address *addressPointer, fileType uint8) ([]byte, error) {
	return nil, nil
}

func (persistent_meta_cache *ReadOnlyMetaCache) WriteFile(fileType uint8, data []byte) (*addressPointer, error) {
	return nil, nil
}
