package fs

import (
	"io/fs"
	"math/rand"
	"testing"
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/stretchr/testify/require"
)

var (
	read_only_meta_cache, _        = NewReadOnlyMetaCache("/test/rdOnlyCache/")
	next_inode              uint64 = 1
)

func GenerateAttrCase(test_case_num int32) []proto.InodeInfo {
	test_attr_case := []proto.InodeInfo{}
	rand.Seed(time.Now().UnixNano())
	mode_set := []fs.FileMode{fs.ModeDir, fs.ModeSymlink, fs.ModeNamedPipe, fs.ModeSocket, fs.ModeDevice, fs.ModeCharDevice, fs.ModeIrregular}
	for index := 0; index < int(test_case_num); index++ {
		test_attr_case = append(test_attr_case,
			proto.InodeInfo{
				Inode:      next_inode,
				Mode:       uint32(mode_set[rand.Intn(len(mode_set))]),
				Nlink:      rand.Uint32(),
				Size:       rand.Uint64(),
				Uid:        rand.Uint32(),
				Gid:        rand.Uint32(),
				Generation: rand.Uint64(),
				ModifyTime: time.Now(),
				CreateTime: time.Now(),
				AccessTime: time.Now(),
				Target:     []byte{},
			})
		next_inode++
	}
	return test_attr_case
}

func GenerateDentries(test_attr_case []proto.InodeInfo) []proto.Dentry {
	test_dentry_case := []proto.Dentry{}
	for _, attr_case := range test_attr_case {
		test_dentry_case = append(test_dentry_case,
			proto.Dentry{
				Name:  "test_name_" + string(attr_case.Inode),
				Inode: attr_case.Inode,
				Type:  attr_case.Mode,
			})
	}
	return test_dentry_case
}

func TestPutAttr(t *testing.T) {
	var err error
	var test_case_num int32 = 100
	test_attr_case := GenerateAttrCase(test_case_num)
	for _, attr_case := range test_attr_case {
		err = read_only_meta_cache.PutAttr(&attr_case)
		if err != nil {
			t.Fatalf("[TestPutAttr] Cache attt: %d fail", attr_case.Inode)
		}
		err = read_only_meta_cache.PutAttr(&attr_case)
		if err != nil {
			t.Fatalf("[TestPutAttr] Duplicate Cache attt: %d fail", attr_case.Inode)
		}
	}
}

func TestGetAttr(t *testing.T) {
	var err error
	var test_case_num int32 = 100
	test_attr_case := GenerateAttrCase(test_case_num)
	// PutAttr
	for _, attr_case := range test_attr_case {
		err = read_only_meta_cache.PutAttr(&attr_case)
		if err != nil {
			t.Fatalf("[TestPutAttr] Cache attt: %d fail", attr_case.Inode)
		}
	}

	// positive find
	for _, attr_case := range test_attr_case {
		fetch_attr := proto.InodeInfo{}
		err = read_only_meta_cache.GetAttr(attr_case.Inode, &fetch_attr)
		if err != nil {
			t.Fatalf("[TestGetAttr]Read attr from read only metadata cache fail, ino: %d", attr_case.Inode)
		}
		require.Equal(t, attr_case, fetch_attr)
	}

	// negative find
	var index int32
	for index = 0; index < 10; index++ {
		fetch_attr := proto.InodeInfo{}
		var ino uint64 = uint64(next_inode + uint64(index))
		err = read_only_meta_cache.GetAttr(ino, &fetch_attr)
		if err != nil {
			t.Fatalf("[TestGetAttr]Read attr from read only metadata cache fail, ino: %d", ino)
		}
		require.Equal(t, 0, fetch_attr.Inode)
	}
}

func TestPutDentry(t *testing.T) {
	var err error
	var test_case_num int32 = 100
	parent_attr := GenerateAttrCase(1)[0]
	sub_dir_attr_case_1 := GenerateAttrCase(test_case_num)
	subdir_dentries_1 := GenerateDentries(sub_dir_attr_case_1)
	// put entry in the first time, create persistent dentry
	err = read_only_meta_cache.PutDentry(parent_attr.Inode, subdir_dentries_1, false)
	if err != nil {
		t.Fatalf("[TestPutDentry]Cache Dentry fail")
	}

	sub_dir_attr_case_2 := GenerateAttrCase(test_case_num)
	subdir_dentries_2 := GenerateDentries(sub_dir_attr_case_2)
	// Put Dentry Completely, Persist it
	err = read_only_meta_cache.PutDentry(parent_attr.Inode, subdir_dentries_2, true)
	if err != nil {
		t.Fatalf("[TestPutDentry]Cache dentry and persist entrybuffer fail")
	}
	// Put dentry completely again, don't persist it
	err = read_only_meta_cache.PutDentry(parent_attr.Inode, subdir_dentries_2, true)
	if err != nil {
		t.Fatalf("[TestPutDentry]Duplicate persist entrybuffer fail")
	}
}

func TestLookup(t *testing.T) {
	var (
		test_case_num int32 = 10
		err           error
		ino           uint64
	)
	parent_attr := GenerateAttrCase(1)[0]
	// Lookup in not cached persistent_dentry
	ino, err = read_only_meta_cache.Lookup(parent_attr.Inode, "not_exist")
	require.Equal(t, 0, ino)
	require.Equal(t, DENTRY_NOT_CACHE, err)

	// Put Dentry Partly
	subdir_attr_case_1 := GenerateAttrCase(test_case_num)
	subdir_dentries_1 := GenerateDentries(subdir_attr_case_1)
	err = read_only_meta_cache.PutDentry(parent_attr.Inode, subdir_dentries_1, false)
	if err != nil {
		t.Fatalf("[TestLookup] Put Dentry Fail, parent ino: %d", parent_attr.Inode)
	}

	// Lookup not cached dentry when persistent_dentry is cached partly
	ino, err = read_only_meta_cache.Lookup(parent_attr.Inode, "not_exist")
	require.Equal(t, 0, ino)
	require.Equal(t, DENTRY_NOT_CACHE, err)

	// PutDentry Completely
	subdir_attr_case_2 := GenerateAttrCase(test_case_num)
	subdir_dentries_2 := GenerateDentries(subdir_attr_case_2)
	err = read_only_meta_cache.PutDentry(parent_attr.Inode, subdir_dentries_2, true)
	if err != nil {
		t.Fatalf("[TestLookup] Put Dentry Fail, parent ino: %d", parent_attr.Inode)
	}

	// Lookup cached dentry
	ino, err = read_only_meta_cache.Lookup(parent_attr.Inode, subdir_dentries_1[0].Name)
	require.Equal(t, subdir_dentries_1[0].Inode, ino)

	// Lookup not cached dentry when persistent_dentry is cached completely
	ino, err = read_only_meta_cache.Lookup(parent_attr.Inode, "not_exist")
	require.Equal(t, 0, ino)
	require.Equal(t, DENTRY_NOT_EXIST, err)
}

func TestGetDentry(t *testing.T) {
	var (
		test_case_num    int32 = 100
		err              error
		fetch_dentries_1 []proto.Dentry
		// fetch_dentries_2 []proto.Dentry
		fetch_dentries_2 []proto.Dentry
	)
	parent_attr := GenerateAttrCase(1)[0]
	subdir_attr_case_1 := GenerateAttrCase(test_case_num)
	subdir_dentries_1 := GenerateDentries(subdir_attr_case_1)

	// fetch not cached dentry
	fetch_dentries_1, err = read_only_meta_cache.GetDentry(parent_attr.Inode)
	require.Equal(t, 0, len(fetch_dentries_1))
	require.Equal(t, DENTRY_NOT_CACHE, err)

	read_only_meta_cache.PutDentry(parent_attr.Inode, subdir_dentries_1, false)
	// fetch not complete cached dentry
	fetch_dentries_1, err = read_only_meta_cache.GetDentry(parent_attr.Inode)
	require.Equal(t, 0, len(fetch_dentries_1))
	require.Equal(t, DENTRY_NOT_CACHE, err)

	// // duplicate cache
	// read_only_meta_cache.PutDentry(parent_attr.Inode, subdir_dentries_1, false)
	// fetch_dentries_2, err = read_only_meta_cache.GetDentry(parent_attr.Inode)
	// require.Equal(t, subdir_dentries_1, fetch_dentries_2)

	sub_dir_attr_case_2 := GenerateAttrCase(test_case_num)
	subdir_dentries_2 := GenerateDentries(sub_dir_attr_case_2)
	read_only_meta_cache.PutDentry(parent_attr.Inode, subdir_dentries_2, true)
	// Fetch Dentry From Cache
	total_sub_dir_dentries := append(subdir_dentries_1, subdir_dentries_2...)
	fetch_dentries_2, err = read_only_meta_cache.GetDentry(parent_attr.Inode)
	require.Equal(t, total_sub_dir_dentries, fetch_dentries_2)
}
