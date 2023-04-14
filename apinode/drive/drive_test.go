package drive

import (
	"context"
	crand "crypto/rand"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"io"
	"io/fs"
	"math/rand"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cubefs/cubefs/blobstore/common/rpc"
	_ "github.com/cubefs/cubefs/blobstore/testing/nolog"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"github.com/cubefs/cubefs/apinode/sdk"
	"github.com/cubefs/cubefs/apinode/testing/mocks"
)

const (
	testUserID = "test-user-1"
)

var (
	A = gomock.Any()
	C = gomock.NewController

	Ctx = context.Background()
)

type mockNode struct {
	DriveNode  *DriveNode
	Volume     *mocks.MockIVolume
	ClusterMgr *mocks.MockClusterManager
	Cluster    *mocks.MockICluster
	GenInode   func() uint64
}

func newMockNode(tb testing.TB) mockNode {
	urm, _ := NewUserRouteMgr()
	volume := mocks.NewMockIVolume(C(tb))
	clusterMgr := mocks.NewMockClusterManager(C(tb))
	inode := uint64(1)
	return mockNode{
		DriveNode: &DriveNode{
			vol:        volume,
			userRouter: urm,
			clusterMgr: clusterMgr,
		},
		Volume:     volume,
		ClusterMgr: clusterMgr,
		Cluster:    mocks.NewMockICluster(C(tb)),
		GenInode: func() uint64 {
			return atomic.AddUint64(&inode, 1)
		},
	}
}

func (node *mockNode) OnceGetUser() {
	path := getUserRouteFile(testUserID)
	LookupN := len(strings.Split(strings.Trim(path, "/"), "/"))
	node.Volume.EXPECT().Lookup(A, A, A).DoAndReturn(
		func(_ context.Context, _ uint64, name string) (*sdk.DirInfo, error) {
			return &sdk.DirInfo{
				Name:  name,
				Inode: node.GenInode(),
			}, nil
		}).Times(LookupN)
	node.Volume.EXPECT().GetXAttr(A, A, A).DoAndReturn(
		func(ctx context.Context, ino uint64, key string) (string, error) {
			uid := UserID(key)
			ur := UserRoute{
				Uid:         uid,
				ClusterType: 1,
				ClusterID:   "cluster01",
				VolumeID:    "volume01",
				DriveID:     string(uid) + "_drive",
				RootPath:    getRootPath(uid),
				RootFileID:  FileID(ino),
				Ctime:       time.Now().Unix(),
			}
			val, _ := ur.Marshal()
			return string(val), nil
		})
	node.ClusterMgr.EXPECT().GetCluster(A).Return(node.Cluster)
	node.Cluster.EXPECT().GetVol(A).Return(node.Volume)
}

func (node *mockNode) OnceLookup(isDir bool) {
	typ := uint32(0)
	if isDir {
		typ = uint32(fs.ModeDir)
	}
	node.Volume.EXPECT().Lookup(A, A, A).DoAndReturn(
		func(_ context.Context, _ uint64, name string) (*sdk.DirInfo, error) {
			return &sdk.DirInfo{
				Name:  name,
				Inode: node.GenInode(),
				Type:  typ,
			}, nil
		})
}

func (node *mockNode) OnceGetInode() {
	node.Volume.EXPECT().GetInode(A, A).DoAndReturn(
		func(_ context.Context, ino uint64) (*sdk.InodeInfo, error) {
			return &sdk.InodeInfo{
				Inode: ino,
			}, nil
		})
}

// httptest server need close by you.
func newTestServer(d *DriveNode) (*httptest.Server, rpc.Client) {
	server := httptest.NewServer(d.RegisterAPIRouters())
	return server, rpc.NewClient(&rpc.Config{})
}

func genURL(host string, uri string, querys ...string) string {
	if len(querys)%2 == 1 {
		querys = append(querys, "")
	}
	q := make(url.Values)
	for i := 0; i < len(querys); i += 2 {
		q.Set(querys[i], querys[i+1])
	}
	if len(q) > 0 {
		return fmt.Sprintf("%s%s?%s", host, uri, q.Encode())
	}
	return fmt.Sprintf("%s%s", host, uri)
}

type mockBody struct {
	remain int
	buff   []byte
}

func (r *mockBody) Read(p []byte) (n int, err error) {
	if r.remain <= 0 {
		return 0, io.EOF
	}
	buff := r.buff[:]
	if r.remain < len(buff) {
		buff = buff[:r.remain]
	}
	n = copy(p, buff)
	return
}

func (r *mockBody) Sum32() uint32 {
	crc := crc32.NewIEEE()
	remain, buff := r.remain, r.buff[:]
	for remain > 0 {
		if remain < len(buff) {
			buff = buff[:remain]
		}
		n, _ := crc.Write(buff)
		remain -= n
	}
	return crc.Sum32()
}

func newMockBody(size int) *mockBody {
	buff := make([]byte, 1<<20)
	crand.Read(buff)
	return &mockBody{
		remain: size,
		buff:   buff,
	}
}

func TestGetUserRouteInfo(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	urm, _ := NewUserRouteMgr()
	mockVol := mocks.NewMockIVolume(ctrl)
	mockClusterMgr := mocks.NewMockClusterManager(ctrl)
	d := &DriveNode{
		vol:        mockVol,
		userRouter: urm,
		clusterMgr: mockClusterMgr,
	}

	mockVol.EXPECT().Lookup(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, fmt.Errorf("look up error"))
	_, err := d.GetUserRouteInfo(context.TODO(), "test")
	require.Equal(t, err.Error(), "look up error")

	_, h2 := hashUid("test")
	mockVol.EXPECT().Lookup(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, ino uint64, name string) (*sdk.DirInfo, error) {
			if name == fmt.Sprintf("%d", h2%hashMask) {
				return nil, sdk.ErrNotFound
			}
			return &sdk.DirInfo{
				Name:  name,
				Inode: uint64(rand.Int63()),
			}, nil
		}).AnyTimes()
	_, err = d.GetUserRouteInfo(context.TODO(), "test")
	require.ErrorIs(t, err, sdk.ErrNotFound)

	// not found xattr
	mockVol.EXPECT().GetXAttr(gomock.Any(), gomock.Any(), gomock.Any()).Return("", sdk.ErrNotFound)
	_, err = d.GetUserRouteInfo(context.TODO(), "test1")
	require.ErrorIs(t, err, sdk.ErrNotFound)

	ur := UserRoute{
		Uid:         "test1",
		ClusterType: 1,
		ClusterID:   "cluster01",
		VolumeID:    "volume01",
		DriveID:     "test1_drive",
		RootPath:    getRootPath("test1"),
		RootFileID:  10,
		Ctime:       time.Now().Unix(),
	}
	v, _ := json.Marshal(ur)
	mockVol.EXPECT().GetXAttr(gomock.Any(), gomock.Any(), gomock.Any()).Return(string(v), nil)
	ur1, err := d.GetUserRouteInfo(context.TODO(), "test1")
	require.NoError(t, err)
	require.Equal(t, *ur1, ur)
}

func TestGetRootInoAndVolume(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	urm, _ := NewUserRouteMgr()
	mockVol := mocks.NewMockIVolume(ctrl)
	mockClusterMgr := mocks.NewMockClusterManager(ctrl)
	d := &DriveNode{
		vol:        mockVol,
		userRouter: urm,
		clusterMgr: mockClusterMgr,
	}

	mockVol.EXPECT().Lookup(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, ino uint64, name string) (*sdk.DirInfo, error) {
			return &sdk.DirInfo{
				Name:  name,
				Inode: uint64(rand.Int63()),
			}, nil
		}).AnyTimes()
	mockVol.EXPECT().GetXAttr(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(func(ctx context.Context, ino uint64, key string) (string, error) {
		ur := UserRoute{
			Uid:         "test1",
			ClusterType: 1,
			ClusterID:   "cluster01",
			VolumeID:    "volume01",
			DriveID:     "test1_drive",
			RootPath:    getRootPath("test1"),
			RootFileID:  10,
			Ctime:       time.Now().Unix(),
		}
		v, _ := json.Marshal(ur)
		return string(v), nil
	}).AnyTimes()
	mockClusterMgr.EXPECT().GetCluster(gomock.Any()).Return(nil)
	_, _, err := d.getRootInoAndVolume(context.TODO(), "test1")
	require.ErrorIs(t, err, sdk.ErrNoCluster)

	mockCluster := mocks.NewMockICluster(ctrl)
	mockClusterMgr.EXPECT().GetCluster(gomock.Any()).Return(mockCluster).AnyTimes()
	mockCluster.EXPECT().GetVol(gomock.Any()).Return(nil)

	_, _, err = d.getRootInoAndVolume(context.TODO(), "test1")
	require.ErrorIs(t, err, sdk.ErrNoVolume)

	mockVol1 := mocks.NewMockIVolume(ctrl)
	mockCluster.EXPECT().GetVol(gomock.Any()).Return(mockVol1)
	rootIno, vol, err := d.getRootInoAndVolume(context.TODO(), "test1")
	require.Nil(t, err)
	require.Equal(t, rootIno, Inode(10))
	require.Equal(t, vol, mockVol1)
}

func TestLookup(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	d := &DriveNode{}
	mockVol := mocks.NewMockIVolume(ctrl)

	_, err := d.lookup(context.TODO(), mockVol, 1, "/")
	require.ErrorIs(t, err, sdk.ErrBadRequest)

	mockVol.EXPECT().Lookup(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, sdk.ErrNotFound)
	_, err = d.lookup(context.TODO(), mockVol, 1, "/a/")
	require.ErrorIs(t, err, sdk.ErrNotFound)

	mockVol.EXPECT().Lookup(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(func(ctx context.Context, parentIno uint64, name string) (*sdk.DirInfo, error) {
		return &sdk.DirInfo{
			Name:  name,
			Inode: parentIno + 1,
		}, nil
	}).Times(3)
	dirInfo, err := d.lookup(context.TODO(), mockVol, 1, "/a/b/c")
	require.ErrorIs(t, err, nil)
	require.Equal(t, dirInfo.Name, "c")
	require.Equal(t, dirInfo.Inode, uint64(4))
}

func TestCreateDir(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockVol := mocks.NewMockIVolume(ctrl)
	d := &DriveNode{}
	_, err := d.createDir(context.TODO(), mockVol, 1, "/", false)
	require.ErrorIs(t, err, sdk.ErrBadRequest)

	mockVol.EXPECT().Lookup(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, sdk.ErrNotFound).Times(2)
	_, err = d.createDir(context.TODO(), mockVol, 1, "/a/b", false)
	require.ErrorIs(t, err, sdk.ErrNotFound)
	mockVol.EXPECT().Mkdir(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, sdk.ErrForbidden)
	_, err = d.createDir(context.TODO(), mockVol, 1, "/a", false)
	require.ErrorIs(t, err, sdk.ErrForbidden)

	mockVol.EXPECT().Lookup(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, parentIno uint64, name string) (*sdk.DirInfo, error) {
			return &sdk.DirInfo{
				Name:  name,
				Inode: parentIno + 1,
				Type:  uint32(os.ModeIrregular),
			}, nil
		})
	_, err = d.createDir(context.TODO(), mockVol, 1, "/a/b", false)
	require.ErrorIs(t, err, sdk.ErrConflict)

	mockVol.EXPECT().Lookup(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, parentIno uint64, name string) (*sdk.DirInfo, error) {
			if name == "c" {
				return nil, sdk.ErrNotFound
			}
			return &sdk.DirInfo{
				Name:  name,
				Inode: parentIno + 1,
				Type:  uint32(os.ModeDir),
			}, nil
		}).Times(3)
	mockVol.EXPECT().Mkdir(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, parentIno uint64, name string) (*sdk.InodeInfo, error) {
			return &sdk.InodeInfo{
				Inode:      parentIno + 1,
				Mode:       uint32(os.ModeDir),
				Nlink:      1,
				Size:       4096,
				Uid:        0,
				Gid:        0,
				ModifyTime: time.Now(),
				CreateTime: time.Now(),
				AccessTime: time.Now(),
			}, nil
		})
	inoInfo, err := d.createDir(context.TODO(), mockVol, 1, "/a/b/c", false)
	require.Nil(t, err)
	require.Equal(t, inoInfo.Inode, uint64(4))
	require.True(t, os.FileMode(inoInfo.Mode).IsDir())

	mockVol.EXPECT().Lookup(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, sdk.ErrNotFound).Times(3)
	mockVol.EXPECT().Mkdir(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, parentIno uint64, name string) (*sdk.InodeInfo, error) {
			return &sdk.InodeInfo{
				Inode:      parentIno + 1,
				Mode:       uint32(os.ModeDir),
				Nlink:      1,
				Size:       4096,
				Uid:        0,
				Gid:        0,
				ModifyTime: time.Now(),
				CreateTime: time.Now(),
				AccessTime: time.Now(),
			}, nil
		}).Times(3)
	inoInfo, err = d.createDir(context.TODO(), mockVol, 1, "/a/b/c", true)
	require.Nil(t, err)
	require.Equal(t, inoInfo.Inode, uint64(4))
	require.True(t, os.FileMode(inoInfo.Mode).IsDir())

	mockVol.EXPECT().Lookup(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, parentIno uint64, name string) (*sdk.DirInfo, error) {
			return &sdk.DirInfo{
				Name:  name,
				Inode: parentIno + 1,
				Type:  uint32(os.ModeDir),
			}, nil
		}).Times(3)
	mockVol.EXPECT().GetInode(gomock.Any(), gomock.Any()).Return(nil, sdk.ErrNotFound)
	_, err = d.createDir(context.TODO(), mockVol, 1, "/a/b/c", true)
	require.ErrorIs(t, err, sdk.ErrNotFound)

	mockVol.EXPECT().Lookup(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, parentIno uint64, name string) (*sdk.DirInfo, error) {
			return &sdk.DirInfo{
				Name:  name,
				Inode: parentIno + 1,
				Type:  uint32(os.ModeDir),
			}, nil
		}).Times(3)
	mockVol.EXPECT().GetInode(gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, inode uint64) (*sdk.InodeInfo, error) {
			return &sdk.InodeInfo{
				Inode: inode,
				Mode:  uint32(os.ModeDir),
			}, nil
		})
	inoInfo, err = d.createDir(context.TODO(), mockVol, 1, "/a/b/c", true)
	require.Nil(t, err)
	require.Equal(t, inoInfo.Inode, uint64(4))
	require.True(t, os.FileMode(inoInfo.Mode).IsDir())
}

func TestCreateFile(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockVol := mocks.NewMockIVolume(ctrl)
	d := &DriveNode{}

	_, err := d.createFile(context.TODO(), mockVol, 1, "/")
	require.ErrorIs(t, err, sdk.ErrBadRequest)

	mockVol.EXPECT().CreateFile(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, sdk.ErrBadFile)
	_, err = d.createFile(context.Background(), mockVol, 1, "a")
	require.ErrorIs(t, err, sdk.ErrBadFile)

	mockVol.EXPECT().CreateFile(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, parentIno uint64, name string) (*sdk.InodeInfo, error) {
			return &sdk.InodeInfo{
				Inode: parentIno + 1,
				Mode:  uint32(os.ModeIrregular),
				Size:  0,
			}, nil
		})
	inoInfo, err := d.createFile(context.Background(), mockVol, 1, "a")
	require.Nil(t, err)
	require.Equal(t, inoInfo.Inode, uint64(2))

	mockVol.EXPECT().Lookup(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, parentIno uint64, name string) (*sdk.DirInfo, error) {
			return &sdk.DirInfo{
				Name:  name,
				Inode: parentIno + 1,
				Type:  uint32(os.ModeDir),
			}, nil
		}).Times(2)
	mockVol.EXPECT().GetInode(gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, inode uint64) (*sdk.InodeInfo, error) {
			return &sdk.InodeInfo{
				Inode: inode,
				Mode:  uint32(os.ModeDir),
			}, nil
		})
	mockVol.EXPECT().CreateFile(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, parentIno uint64, name string) (*sdk.InodeInfo, error) {
			return &sdk.InodeInfo{
				Inode: parentIno + 1,
				Mode:  uint32(os.ModeIrregular),
				Size:  0,
			}, nil
		})
	inoInfo, err = d.createFile(context.TODO(), mockVol, 1, "/a/b/c")
	require.Nil(t, err)
	require.Equal(t, inoInfo.Inode, uint64(4))

	mockVol.EXPECT().Lookup(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, parentIno uint64, name string) (*sdk.DirInfo, error) {
			mode := uint32(os.ModeDir)
			if name == "c" {
				mode = uint32(os.ModeIrregular)
			}
			return &sdk.DirInfo{
				Name:  name,
				Inode: parentIno + 1,
				Type:  mode,
			}, nil
		}).Times(3)
	mockVol.EXPECT().GetInode(gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, inode uint64) (*sdk.InodeInfo, error) {
			mode := uint32(os.ModeDir)
			if inode == 4 {
				mode = uint32(os.ModeIrregular)
			}
			return &sdk.InodeInfo{
				Inode: inode,
				Mode:  mode,
			}, nil
		}).Times(2)

	mockVol.EXPECT().CreateFile(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, parentIno uint64, name string) (*sdk.InodeInfo, error) {
			return nil, sdk.ErrExist
		})
	inoInfo, err = d.createFile(context.TODO(), mockVol, 1, "/a/b/c")
	require.Nil(t, err)
	require.Equal(t, inoInfo.Inode, uint64(4))
}