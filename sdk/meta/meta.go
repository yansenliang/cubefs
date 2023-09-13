// Copyright 2018 The CubeFS Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package meta

import (
	"fmt"
	"syscall"
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/auth"
)

const (
	HostsSeparator                = ","
	RefreshMetaPartitionsInterval = time.Minute * 5
)

const (
	statusUnknown int = iota
	statusOK
	statusExist
	statusNoent
	statusFull
	statusAgain
	statusError
	statusInval
	statusNotPerm
	statusConflictExtents
	statusOpDirQuota
	statusNoSpace
	statusTxInodeInfoNotExist
	statusTxConflict
	statusTxTimeout
	statusUploadPartConflict
	statusSnapshotConflict
)

const (
	MaxMountRetryLimit = 6
	MountRetryInterval = time.Second * 5

	/*
	 * Minimum interval of forceUpdateMetaPartitions in seconds,
	 * i.e. only one force update request is allowed every 5 sec.
	 */
	MinForceUpdateMetaPartitionsInterval = 5
)

type AsyncTaskErrorFunc func(err error)

func (f AsyncTaskErrorFunc) OnError(err error) {
	if f != nil {
		f(err)
	}
}

type MetaConfig struct {
	Volume           string
	Owner            string
	Masters          []string
	Authenticate     bool
	TicketMess       auth.TicketMess
	ValidateOwner    bool
	OnAsyncTaskError AsyncTaskErrorFunc
	EnableSummary    bool
	MetaSendTimeout  int64

	//EnableTransaction uint8
	//EnableTransaction bool
	VerReadSeq uint64
}

// Ticket the ticket from authnode
type Ticket struct {
	ID         string `json:"client_id"`
	SessionKey string `json:"session_key"`
	ServiceID  string `json:"service_id"`
	Ticket     string `json:"ticket"`
}

func NewMetaWrapper(config *MetaConfig) (*MetaWrapper, error) {
	sm, err := NewSnapshotMetaWrapper(config)
	if err != nil {
		return nil, err
	}

	mw := &MetaWrapper{
		SnapShotMetaWrapper: sm,
	}

	return mw, nil
}

func (mw *metaWrapper) initMetaWrapper() (err error) {
	if err = mw.updateClusterInfo(); err != nil {
		return err
	}

	if err = mw.updateVolStatInfo(); err != nil {
		return err
	}

	if err = mw.updateMetaPartitions(); err != nil {
		return err
	}

	if err = mw.updateDirChildrenNumLimit(); err != nil {
		return err
	}

	return nil
}

func (mw *metaWrapper) Owner() string {
	return mw.owner
}

func (mw *metaWrapper) OSSSecure() (accessKey, secretKey string) {
	return mw.ossSecure.AccessKey, mw.ossSecure.SecretKey
}

func (mw *metaWrapper) VolCreateTime() int64 {
	return mw.volCreateTime
}

func (mw *metaWrapper) Close() error {
	mw.closeOnce.Do(func() {
		close(mw.closeCh)
		mw.conns.Close()
	})
	return nil
}

func (mw *metaWrapper) Cluster() string {
	return mw.cluster
}

func (mw *metaWrapper) LocalIP() string {
	return mw.localIP
}

func (mw *metaWrapper) exporterKey(act string) string {
	return fmt.Sprintf("%s_sdk_meta_%s", mw.cluster, act)
}

// Proto ResultCode to status
func parseStatus(result uint8) (status int) {
	switch result {
	case proto.OpOk:
		status = statusOK
	case proto.OpExistErr:
		status = statusExist
	case proto.OpNotExistErr:
		status = statusNoent
	case proto.OpInodeFullErr:
		status = statusFull
	case proto.OpAgain:
		status = statusAgain
	case proto.OpArgMismatchErr:
		status = statusInval
	case proto.OpNotPerm:
		status = statusNotPerm
	case proto.OpSnapshotConflict:
		status = statusSnapshotConflict
	case proto.OpConflictExtentsErr:
		status = statusConflictExtents
	case proto.OpDirQuota:
		status = statusOpDirQuota
	case proto.OpNoSpaceErr:
		status = statusNoSpace
	case proto.OpTxInodeInfoNotExistErr:
		status = statusTxInodeInfoNotExist
	case proto.OpTxConflictErr:
		status = statusTxConflict
	case proto.OpTxTimeoutErr:
		status = statusTxTimeout
	case proto.OpUploadPartConflictErr:
		status = statusUploadPartConflict
	default:
		status = statusError
	}
	return
}

func statusToErrno(status int) error {
	switch status {
	case statusOK:
		// return error anyway
		return syscall.EAGAIN
	case statusExist:
		return syscall.EEXIST
	case statusNoent:
		return syscall.ENOENT
	case statusFull:
		return syscall.ENOMEM
	case statusAgain:
		return syscall.EAGAIN
	case statusInval:
		return syscall.EINVAL
	case statusNotPerm:
		return syscall.EPERM
	case statusError:
		return syscall.EAGAIN
	case statusConflictExtents:
		return syscall.ENOTSUP
	case statusOpDirQuota:
		return syscall.EDQUOT
	case statusNoSpace:
		return syscall.ENOSPC
	case statusTxInodeInfoNotExist:
		return syscall.EAGAIN
	case statusTxConflict:
		return syscall.EAGAIN
	case statusTxTimeout:
		return syscall.EAGAIN
	case statusUploadPartConflict:
		return syscall.EAGAIN
	default:
	}
	return syscall.EIO
}
