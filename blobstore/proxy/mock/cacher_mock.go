// Code generated by MockGen. DO NOT EDIT.
// Source: github.com/cubefs/cubefs/blobstore/proxy/cacher (interfaces: Cacher)

// Package mock is a generated GoMock package.
package mock

import (
	context "context"
	reflect "reflect"

	proxy "github.com/cubefs/cubefs/blobstore/api/proxy"
	gomock "github.com/golang/mock/gomock"
)

// MockCacher is a mock of Cacher interface.
type MockCacher struct {
	ctrl     *gomock.Controller
	recorder *MockCacherMockRecorder
}

// MockCacherMockRecorder is the mock recorder for MockCacher.
type MockCacherMockRecorder struct {
	mock *MockCacher
}

// NewMockCacher creates a new mock instance.
func NewMockCacher(ctrl *gomock.Controller) *MockCacher {
	mock := &MockCacher{ctrl: ctrl}
	mock.recorder = &MockCacherMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockCacher) EXPECT() *MockCacherMockRecorder {
	return m.recorder
}

// GetVolume mocks base method.
func (m *MockCacher) GetVolume(arg0 context.Context, arg1 *proxy.CacheVolumeArgs) (*proxy.VersionVolume, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetVolume", arg0, arg1)
	ret0, _ := ret[0].(*proxy.VersionVolume)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetVolume indicates an expected call of GetVolume.
func (mr *MockCacherMockRecorder) GetVolume(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetVolume", reflect.TypeOf((*MockCacher)(nil).GetVolume), arg0, arg1)
}