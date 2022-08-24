// Code generated by MockGen. DO NOT EDIT.
// Source: types.go

// Package mock_frontend is a generated GoMock package.
package mock_frontend

import (
	"context"
	"github.com/google/uuid"
	reflect "reflect"

	gomock "github.com/golang/mock/gomock"
	batch "github.com/matrixorigin/matrixone/pkg/container/batch"
	tree "github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

// MockComputationRunner is a mock of ComputationRunner interface.
type MockComputationRunner struct {
	ctrl     *gomock.Controller
	recorder *MockComputationRunnerMockRecorder
}

// MockComputationRunnerMockRecorder is the mock recorder for MockComputationRunner.
type MockComputationRunnerMockRecorder struct {
	mock *MockComputationRunner
}

// NewMockComputationRunner creates a new mock instance.
func NewMockComputationRunner(ctrl *gomock.Controller) *MockComputationRunner {
	mock := &MockComputationRunner{ctrl: ctrl}
	mock.recorder = &MockComputationRunnerMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockComputationRunner) EXPECT() *MockComputationRunnerMockRecorder {
	return m.recorder
}

// Run mocks base method.
func (m *MockComputationRunner) Run(ts uint64) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Run", ts)
	ret0, _ := ret[0].(error)
	return ret0
}

// Run indicates an expected call of Run.
func (mr *MockComputationRunnerMockRecorder) Run(ts interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Run", reflect.TypeOf((*MockComputationRunner)(nil).Run), ts)
}

// MockComputationWrapper is a mock of ComputationWrapper interface.
type MockComputationWrapper struct {
	ctrl     *gomock.Controller
	recorder *MockComputationWrapperMockRecorder
}

// MockComputationWrapperMockRecorder is the mock recorder for MockComputationWrapper.
type MockComputationWrapperMockRecorder struct {
	mock *MockComputationWrapper
}

// NewMockComputationWrapper creates a new mock instance.
func NewMockComputationWrapper(ctrl *gomock.Controller) *MockComputationWrapper {
	mock := &MockComputationWrapper{ctrl: ctrl}
	mock.recorder = &MockComputationWrapperMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockComputationWrapper) EXPECT() *MockComputationWrapperMockRecorder {
	return m.recorder
}

func (m *MockComputationWrapper) GetUUID() []byte {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetUUID")
	ret0, _ := ret[0].(uuid.UUID)
	return ret0[:]
}

func (mr *MockComputationWrapperMockRecorder) GetUUID() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetUUID", reflect.TypeOf((*MockComputationWrapper)(nil).GetUUID))
}

// Compile mocks base method.
func (m *MockComputationWrapper) Compile(requestCtx context.Context, u interface{}, fill func(interface{}, *batch.Batch) error) (interface{}, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Compile", u, fill)
	ret0, _ := ret[0].(interface{})
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Compile indicates an expected call of Compile.
func (mr *MockComputationWrapperMockRecorder) Compile(u, fill interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Compile", reflect.TypeOf((*MockComputationWrapper)(nil).Compile), u, fill)
}

// GetAffectedRows mocks base method.
func (m *MockComputationWrapper) GetAffectedRows() uint64 {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetAffectedRows")
	ret0, _ := ret[0].(uint64)
	return ret0
}

// GetAffectedRows indicates an expected call of GetAffectedRows.
func (mr *MockComputationWrapperMockRecorder) GetAffectedRows() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetAffectedRows", reflect.TypeOf((*MockComputationWrapper)(nil).GetAffectedRows))
}

// GetAst mocks base method.
func (m *MockComputationWrapper) GetAst() tree.Statement {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetAst")
	ret0, _ := ret[0].(tree.Statement)
	return ret0
}

// GetAst indicates an expected call of GetAst.
func (mr *MockComputationWrapperMockRecorder) GetAst() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetAst", reflect.TypeOf((*MockComputationWrapper)(nil).GetAst))
}

// GetColumns mocks base method.
func (m *MockComputationWrapper) GetColumns() ([]interface{}, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetColumns")
	ret0, _ := ret[0].([]interface{})
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetColumns indicates an expected call of GetColumns.
func (mr *MockComputationWrapperMockRecorder) GetColumns() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetColumns", reflect.TypeOf((*MockComputationWrapper)(nil).GetColumns))
}

// Run mocks base method.
func (m *MockComputationWrapper) Run(ts uint64) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Run", ts)
	ret0, _ := ret[0].(error)
	return ret0
}

// Run indicates an expected call of Run.
func (mr *MockComputationWrapperMockRecorder) Run(ts interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Run", reflect.TypeOf((*MockComputationWrapper)(nil).Run), ts)
}

// SetDatabaseName mocks base method.
func (m *MockComputationWrapper) SetDatabaseName(db string) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "SetDatabaseName", db)
	ret0, _ := ret[0].(error)
	return ret0
}

// SetDatabaseName indicates an expected call of SetDatabaseName.
func (mr *MockComputationWrapperMockRecorder) SetDatabaseName(db interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "SetDatabaseName", reflect.TypeOf((*MockComputationWrapper)(nil).SetDatabaseName), db)
}
