// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.
//
// TODO(#147193): restore auto-generated RPC mock tests once gomock has support
// for generics.
package rpc

import (
	context "context"
	reflect "reflect"

	kvpb "github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	roachpb "github.com/cockroachdb/cockroach/pkg/roachpb"
	rpcbase "github.com/cockroachdb/cockroach/pkg/rpc/rpcbase"
	gomock "github.com/golang/mock/gomock"
)

// MockBatchStreamClient is a mock of BatchStreamClient interface.
type MockBatchStreamClient struct {
	ctrl     *gomock.Controller
	recorder *MockBatchStreamClientMockRecorder
}

// MockBatchStreamClientMockRecorder is the mock recorder for MockBatchStreamClient.
type MockBatchStreamClientMockRecorder struct {
	mock *MockBatchStreamClient
}

// NewMockBatchStreamClient creates a new mock instance.
func NewMockBatchStreamClient(ctrl *gomock.Controller) *MockBatchStreamClient {
	mock := &MockBatchStreamClient{ctrl: ctrl}
	mock.recorder = &MockBatchStreamClientMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockBatchStreamClient) EXPECT() *MockBatchStreamClientMockRecorder {
	return m.recorder
}

// Recv mocks base method.
func (m *MockBatchStreamClient) Recv() (*kvpb.BatchResponse, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Recv")
	ret0, _ := ret[0].(*kvpb.BatchResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Recv indicates an expected call of Recv.
func (mr *MockBatchStreamClientMockRecorder) Recv() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Recv", reflect.TypeOf((*MockBatchStreamClient)(nil).Recv))
}

// Send mocks base method.
func (m *MockBatchStreamClient) Send(arg0 *kvpb.BatchRequest) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Send", arg0)
	ret0, _ := ret[0].(error)
	return ret0
}

// Send indicates an expected call of Send.
func (mr *MockBatchStreamClientMockRecorder) Send(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Send", reflect.TypeOf((*MockBatchStreamClient)(nil).Send), arg0)
}

// MockDialbacker is a mock of the generic Dialbacker[Conn] interface.
type MockDialbacker[Conn rpcConn] struct {
	ctrl     *gomock.Controller
	recorder *MockDialbackerMockRecorder[Conn]
}

// MockDialbackerMockRecorder is the mock recorder for MockDialbacker.
type MockDialbackerMockRecorder[Conn rpcConn] struct {
	mock *MockDialbacker[Conn]
}

// NewMockDialbacker creates a new mock instance.
func NewMockDialbacker[Conn rpcConn](ctrl *gomock.Controller) *MockDialbacker[Conn] {
	mock := &MockDialbacker[Conn]{ctrl: ctrl}
	mock.recorder = &MockDialbackerMockRecorder[Conn]{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockDialbacker[Conn]) EXPECT() *MockDialbackerMockRecorder[Conn] {
	return m.recorder
}

// UnvalidatedDial mocks base method.
func (m *MockDialbacker[Conn]) UnvalidatedDial(
	arg0 string, arg1 roachpb.Locality,
) *Connection[Conn] {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "UnvalidatedDial", arg0, arg1)
	ret0, _ := ret[0].(*Connection[Conn])
	return ret0
}

// UnvalidatedDial indicates an expected call of UnvalidatedDial.
func (mr *MockDialbackerMockRecorder[Conn]) UnvalidatedDial(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCall(mr.mock, "UnvalidatedDial", arg0, arg1)
}

// DialNode mocks base method.
func (m *MockDialbacker[Conn]) DialNode(
	arg0 string, arg1 roachpb.NodeID, arg2 roachpb.Locality, arg3 rpcbase.ConnectionClass,
) *Connection[Conn] {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "DialNode", arg0, arg1, arg2, arg3)
	ret0, _ := ret[0].(*Connection[Conn])
	return ret0
}

// DialNode indicates an expected call of DialNode.
func (mr *MockDialbackerMockRecorder[Conn]) DialNode(
	arg0, arg1, arg2, arg3 interface{},
) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCall(mr.mock, "DialNode", arg0, arg1, arg2, arg3)
}

// DialRaw mocks base method.
func (m *MockDialbacker[Conn]) DialRaw(
	arg0 context.Context, arg1 string, arg2 rpcbase.ConnectionClass,
) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "DialRaw", arg0, arg1, arg2)
	ret0, _ := ret[0].(error)
	return ret0
}

// DialRaw indicates an expected call of DialRaw.
func (mr *MockDialbackerMockRecorder[Conn]) DialRaw(arg0, arg1, arg2 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCall(mr.mock, "DialRaw", arg0, arg1, arg2)
}

// WrapCtx mocks base method.
func (m *MockDialbacker[Conn]) WrapCtx(
	arg0 context.Context, arg1 string, arg2 roachpb.NodeID, arg3 rpcbase.ConnectionClass,
) context.Context {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "WrapCtx", arg0, arg1, arg2, arg3)
	ret0, _ := ret[0].(context.Context)
	return ret0
}

// WrapCtx indicates an expected call of WrapCtx.
func (mr *MockDialbackerMockRecorder[Conn]) WrapCtx(
	arg0, arg1, arg2, arg3 interface{},
) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCall(mr.mock, "WrapCtx", arg0, arg1, arg2, arg3)
}
