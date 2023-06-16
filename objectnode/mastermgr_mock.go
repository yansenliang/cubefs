// Code generated by MockGen. DO NOT EDIT.
// Source: github.com/cubefs/cubefs/objectnode (interfaces: MasterMgrAPI)

// Package objectnode is a generated GoMock package.
package objectnode

import (
	reflect "reflect"

	master "github.com/cubefs/cubefs/sdk/master"
	gomock "github.com/golang/mock/gomock"
)

// MockMasterMgrAPI is a mock of MasterMgrAPI interface.
type MockMasterMgrAPI struct {
	ctrl     *gomock.Controller
	recorder *MockMasterMgrAPIMockRecorder
}

// MockMasterMgrAPIMockRecorder is the mock recorder for MockMasterMgrAPI.
type MockMasterMgrAPIMockRecorder struct {
	mock *MockMasterMgrAPI
}

// NewMockMasterMgrAPI creates a new mock instance.
func NewMockMasterMgrAPI(ctrl *gomock.Controller) *MockMasterMgrAPI {
	mock := &MockMasterMgrAPI{ctrl: ctrl}
	mock.recorder = &MockMasterMgrAPIMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockMasterMgrAPI) EXPECT() *MockMasterMgrAPIMockRecorder {
	return m.recorder
}

// AddNode mocks base method.
func (m *MockMasterMgrAPI) AddNode(arg0 string) {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "AddNode", arg0)
}

// AddNode indicates an expected call of AddNode.
func (mr *MockMasterMgrAPIMockRecorder) AddNode(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "AddNode", reflect.TypeOf((*MockMasterMgrAPI)(nil).AddNode), arg0)
}

// AdminAPI mocks base method.
func (m *MockMasterMgrAPI) AdminAPI() *master.AdminAPI {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "AdminAPI")
	ret0, _ := ret[0].(*master.AdminAPI)
	return ret0
}

// AdminAPI indicates an expected call of AdminAPI.
func (mr *MockMasterMgrAPIMockRecorder) AdminAPI() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "AdminAPI", reflect.TypeOf((*MockMasterMgrAPI)(nil).AdminAPI))
}

// ClientAPI mocks base method.
func (m *MockMasterMgrAPI) ClientAPI() *master.ClientAPI {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ClientAPI")
	ret0, _ := ret[0].(*master.ClientAPI)
	return ret0
}

// ClientAPI indicates an expected call of ClientAPI.
func (mr *MockMasterMgrAPIMockRecorder) ClientAPI() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ClientAPI", reflect.TypeOf((*MockMasterMgrAPI)(nil).ClientAPI))
}

// Leader mocks base method.
func (m *MockMasterMgrAPI) Leader() string {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Leader")
	ret0, _ := ret[0].(string)
	return ret0
}

// Leader indicates an expected call of Leader.
func (mr *MockMasterMgrAPIMockRecorder) Leader() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Leader", reflect.TypeOf((*MockMasterMgrAPI)(nil).Leader))
}

// NodeAPI mocks base method.
func (m *MockMasterMgrAPI) NodeAPI() *master.NodeAPI {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "NodeAPI")
	ret0, _ := ret[0].(*master.NodeAPI)
	return ret0
}

// NodeAPI indicates an expected call of NodeAPI.
func (mr *MockMasterMgrAPIMockRecorder) NodeAPI() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "NodeAPI", reflect.TypeOf((*MockMasterMgrAPI)(nil).NodeAPI))
}

// Nodes mocks base method.
func (m *MockMasterMgrAPI) Nodes() []string {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Nodes")
	ret0, _ := ret[0].([]string)
	return ret0
}

// Nodes indicates an expected call of Nodes.
func (mr *MockMasterMgrAPIMockRecorder) Nodes() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Nodes", reflect.TypeOf((*MockMasterMgrAPI)(nil).Nodes))
}

// ReplaceMasterAddresses mocks base method.
func (m *MockMasterMgrAPI) ReplaceMasterAddresses(arg0 []string) {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "ReplaceMasterAddresses", arg0)
}

// ReplaceMasterAddresses indicates an expected call of ReplaceMasterAddresses.
func (mr *MockMasterMgrAPIMockRecorder) ReplaceMasterAddresses(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ReplaceMasterAddresses", reflect.TypeOf((*MockMasterMgrAPI)(nil).ReplaceMasterAddresses), arg0)
}

// SetLeader mocks base method.
func (m *MockMasterMgrAPI) SetLeader(arg0 string) {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "SetLeader", arg0)
}

// SetLeader indicates an expected call of SetLeader.
func (mr *MockMasterMgrAPIMockRecorder) SetLeader(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "SetLeader", reflect.TypeOf((*MockMasterMgrAPI)(nil).SetLeader), arg0)
}

// SetTimeout mocks base method.
func (m *MockMasterMgrAPI) SetTimeout(arg0 uint16) {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "SetTimeout", arg0)
}

// SetTimeout indicates an expected call of SetTimeout.
func (mr *MockMasterMgrAPIMockRecorder) SetTimeout(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "SetTimeout", reflect.TypeOf((*MockMasterMgrAPI)(nil).SetTimeout), arg0)
}

// UserAPI mocks base method.
func (m *MockMasterMgrAPI) UserAPI() *master.UserAPI {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "UserAPI")
	ret0, _ := ret[0].(*master.UserAPI)
	return ret0
}

// UserAPI indicates an expected call of UserAPI.
func (mr *MockMasterMgrAPIMockRecorder) UserAPI() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "UserAPI", reflect.TypeOf((*MockMasterMgrAPI)(nil).UserAPI))
}