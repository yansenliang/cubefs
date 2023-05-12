package fs

import (
	"fmt"
	"io/fs"
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/stretchr/testify/require"
)

var (
	data_path     string = "/test/rdOnlyCache/unit_test/"
	next_inode    uint64 = 2
	test_case_num int32  = 5
	root_inode           = proto.InodeInfo{
		Inode:      1,
		Mode:       uint32(fs.ModeDir),
		Nlink:      rand.Uint32(),
		Size:       rand.Uint64(),
		Uid:        rand.Uint32(),
		Gid:        rand.Uint32(),
		Generation: rand.Uint64(),
		ModifyTime: time.Unix(time.Now().Unix(), 0),
		CreateTime: time.Unix(time.Now().Unix(), 0),
		AccessTime: time.Unix(time.Now().Unix(), 0),
		Target:     []byte{},
	}
	root_subdir_attrs    = GenerateAttrCase(test_case_num)
	root_subdir_dentries = GenerateDentries(root_subdir_attrs)
)

func CompareDentries(expected []proto.Dentry, actual []proto.Dentry) bool {
	if len(expected) != len(actual) {
		return false
	}
	expected_dentry_map := map[string]proto.Dentry{}
	for _, dentry := range expected {
		expected_dentry_map[dentry.Name] = dentry
	}
	for _, dentry := range actual {
		if dentry != expected_dentry_map[dentry.Name] {
			return false
		}
	}
	return true
}

func GenerateAttrCase(case_num int32) []proto.InodeInfo {
	test_attr_case := []proto.InodeInfo{}
	rand.Seed(time.Now().UnixNano())
	mode_set := []fs.FileMode{fs.ModeDir, fs.ModeSymlink, fs.ModeNamedPipe, fs.ModeSocket, fs.ModeDevice, fs.ModeCharDevice, fs.ModeIrregular}
	for index := 0; index < int(case_num); index++ {
		inode_info := proto.InodeInfo{
			Inode:      next_inode,
			Mode:       uint32(mode_set[rand.Intn(len(mode_set))]),
			Nlink:      rand.Uint32(),
			Size:       rand.Uint64(),
			Uid:        rand.Uint32(),
			Gid:        rand.Uint32(),
			Generation: rand.Uint64(),
			ModifyTime: time.Unix(time.Now().Unix(), 0),
			CreateTime: time.Unix(time.Now().Unix(), 0),
			AccessTime: time.Unix(time.Now().Unix(), 0),
			Target:     []byte{},
		}
		if inode_info.Mode == uint32(fs.ModeSymlink) {
			inode_info.Target = []byte("/symbolink")
		}
		test_attr_case = append(test_attr_case, inode_info)
		next_inode++
	}
	return test_attr_case
}

func GenerateDentries(test_attr_case []proto.InodeInfo) []proto.Dentry {
	test_dentry_case := []proto.Dentry{}
	for _, attr_case := range test_attr_case {
		test_dentry_case = append(test_dentry_case,
			proto.Dentry{
				Name:  "test_name_" + fmt.Sprint(attr_case.Inode),
				Inode: attr_case.Inode,
				Type:  attr_case.Mode,
			})
	}
	return test_dentry_case
}

func TestPutAttr(t *testing.T) {
	var (
		err                  error
		read_only_meta_cache *ReadOnlyMetaCache
	)
	read_only_meta_cache, err = NewReadOnlyMetaCache(data_path)
	if err != nil {
		t.Fatalf("New Read Only Meta Cache Fail")
	}
	// test_attr_case := GenerateAttrCase(test_case_num)
	for _, attr_case := range root_subdir_attrs {
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
	var (
		err                  error
		read_only_meta_cache *ReadOnlyMetaCache
	)
	read_only_meta_cache, err = NewReadOnlyMetaCache(data_path)
	if err != nil {
		t.Fatalf("New Read Only Meta Cache Fail")
	}
	// test_attr_case := GenerateAttrCase(test_case_num)
	// PutAttr
	// for _, attr_case := range test_attr_case {
	// 	err = read_only_meta_cache.PutAttr(&attr_case)
	// 	if err != nil {
	// 		t.Fatalf("[TestPutAttr] Cache attt: %d fail", attr_case.Inode)
	// 	}
	// }

	// positive find
	for _, attr_case := range root_subdir_attrs {
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
		require.Equal(t, uint64(0), fetch_attr.Inode)
	}
}

func TestPutDentry(t *testing.T) {
	var (
		err                  error
		read_only_meta_cache *ReadOnlyMetaCache
	)
	read_only_meta_cache, err = NewReadOnlyMetaCache(data_path)
	if err != nil {
		t.Fatalf("New Read Only Meta Cache Fail")
	}
	// put entry in the first time, create persistent dentry
	root_subdir_dentries_num := len(root_subdir_dentries)
	err = read_only_meta_cache.PutDentry(root_inode.Inode, root_subdir_dentries[:root_subdir_dentries_num/2], false)
	if err != nil {
		t.Fatalf("[TestPutDentry]Cache Dentry fail")
	}

	// Put Dentry Completely, Persist it
	err = read_only_meta_cache.PutDentry(root_inode.Inode, root_subdir_dentries[root_subdir_dentries_num/2:], true)
	if err != nil {
		t.Fatalf("[TestPutDentry]Cache dentry and persist entrybuffer fail")
	}
	// Put dentry completely again, don't persist it
	err = read_only_meta_cache.PutDentry(root_inode.Inode, root_subdir_dentries[root_subdir_dentries_num/2+1:], true)
	if err != nil {
		t.Fatalf("[TestPutDentry]Duplicate persist entrybuffer fail")
	}
}

func TestGetDentry(t *testing.T) {
	var (
		err                  error
		read_only_meta_cache *ReadOnlyMetaCache
		fetch_dentries_1     []proto.Dentry
		fetch_dentries_2     []proto.Dentry
		fetch_dentries_3     []proto.Dentry
	)
	read_only_meta_cache, err = NewReadOnlyMetaCache(data_path)
	if err != nil {
		t.Fatalf("New Read Only Meta Cache Fail")
	}

	parent_attr := GenerateAttrCase(1)[0]
	subdir_attr_case_1 := GenerateAttrCase(test_case_num)
	subdir_dentries_1 := GenerateDentries(subdir_attr_case_1)

	// Fetch Dentry From Cache with loading EntryBuffer
	fetch_dentries_3, err = read_only_meta_cache.GetDentry(root_inode.Inode)
	if !CompareDentries(root_subdir_dentries, fetch_dentries_3) {
		require.Equal(t, root_subdir_dentries, fetch_dentries_3)
		t.Fatalf("[TestGetDentry] Get wrong dentry data, error: %s", err.Error())
	}

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
	if !CompareDentries(total_sub_dir_dentries, fetch_dentries_2) {
		t.Fatalf("[TestGetDentry] Get wrong dentry data, error: %s", err.Error())
	}

}

func TestLookup(t *testing.T) {
	var (
		err                  error
		read_only_meta_cache *ReadOnlyMetaCache
		ino                  uint64
	)
	read_only_meta_cache, err = NewReadOnlyMetaCache(data_path)
	if err != nil {
		t.Fatalf("New Read Only Meta Cache Fail")
	}

	// Lookup cached dentry with persistent_dentry loading EntryBuffer
	ino, err = read_only_meta_cache.Lookup(root_inode.Inode, root_subdir_dentries[0].Name)
	if err != nil {
		fmt.Printf("[TestLookup]Fail: err: %s", err.Error())
	}
	require.Equal(t, root_subdir_dentries[0].Inode, ino)

	parent_attr := GenerateAttrCase(1)[0]
	// Lookup in not cached persistent_dentry
	ino, err = read_only_meta_cache.Lookup(parent_attr.Inode, "not_exist")
	require.Equal(t, uint64(0), ino)
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
	require.Equal(t, uint64(0), ino)
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
	require.Equal(t, uint64(0), ino)
	require.Equal(t, DENTRY_NOT_EXIST, err)
}

func TestEvict(t *testing.T) {
	var (
		err                  error
		read_only_meta_cache *ReadOnlyMetaCache
	)
	read_only_meta_cache, err = NewReadOnlyMetaCache(data_path)
	if err != nil {
		t.Fatalf("New Read Only Meta Cache Fail")
	}

	// Test active EntryBuffer eviction until no buffer cached
	read_only_meta_cache.Evict(true)

	// Normal Foreground EntryBuffer Evict When excuting PutDentry
	for i := 0; i < MaxDentryBufferElement*3/2; i++ {
		parent_attr := GenerateAttrCase(1)[0]
		subdir_attr_case := GenerateAttrCase(test_case_num)
		subdir_dentries := GenerateDentries(subdir_attr_case)
		err = read_only_meta_cache.PutDentry(parent_attr.Inode, subdir_dentries, true)
	}

	// Test background execution when no entrybuffer is expired
	read_only_meta_cache.Evict(false)

	// Test active EntryBuffer eviction when num of entry buffer is more than MinDentryBufferEvictNum
	read_only_meta_cache.Evict(true)

	// Wait For Background Eviction Goroutine runing
	time.Sleep(1 * time.Minute)

	// Test background execution when num of expired entrybuffer is more than MinDentryBufferEvictNum
	read_only_meta_cache.Evict(false)
}

func TestMain(m *testing.M) {
	m.Run()
	os.RemoveAll(data_path)
}
