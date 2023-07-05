// Code generated by mockery v2.30.1. DO NOT EDIT.

package databricks

import (
	context "context"

	catalog "github.com/databricks/databricks-sdk-go/service/catalog"

	mock "github.com/stretchr/testify/mock"
)

// mockDataUsageAccountRepository is an autogenerated mock type for the dataUsageAccountRepository type
type mockDataUsageAccountRepository struct {
	mock.Mock
}

type mockDataUsageAccountRepository_Expecter struct {
	mock *mock.Mock
}

func (_m *mockDataUsageAccountRepository) EXPECT() *mockDataUsageAccountRepository_Expecter {
	return &mockDataUsageAccountRepository_Expecter{mock: &_m.Mock}
}

// GetWorkspaceMap provides a mock function with given fields: ctx, metastores, workspaces
func (_m *mockDataUsageAccountRepository) GetWorkspaceMap(ctx context.Context, metastores []catalog.MetastoreInfo, workspaces []Workspace) (map[string][]string, map[string]string, error) {
	ret := _m.Called(ctx, metastores, workspaces)

	var r0 map[string][]string
	var r1 map[string]string
	var r2 error
	if rf, ok := ret.Get(0).(func(context.Context, []catalog.MetastoreInfo, []Workspace) (map[string][]string, map[string]string, error)); ok {
		return rf(ctx, metastores, workspaces)
	}
	if rf, ok := ret.Get(0).(func(context.Context, []catalog.MetastoreInfo, []Workspace) map[string][]string); ok {
		r0 = rf(ctx, metastores, workspaces)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(map[string][]string)
		}
	}

	if rf, ok := ret.Get(1).(func(context.Context, []catalog.MetastoreInfo, []Workspace) map[string]string); ok {
		r1 = rf(ctx, metastores, workspaces)
	} else {
		if ret.Get(1) != nil {
			r1 = ret.Get(1).(map[string]string)
		}
	}

	if rf, ok := ret.Get(2).(func(context.Context, []catalog.MetastoreInfo, []Workspace) error); ok {
		r2 = rf(ctx, metastores, workspaces)
	} else {
		r2 = ret.Error(2)
	}

	return r0, r1, r2
}

// mockDataUsageAccountRepository_GetWorkspaceMap_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'GetWorkspaceMap'
type mockDataUsageAccountRepository_GetWorkspaceMap_Call struct {
	*mock.Call
}

// GetWorkspaceMap is a helper method to define mock.On call
//   - ctx context.Context
//   - metastores []catalog.MetastoreInfo
//   - workspaces []Workspace
func (_e *mockDataUsageAccountRepository_Expecter) GetWorkspaceMap(ctx interface{}, metastores interface{}, workspaces interface{}) *mockDataUsageAccountRepository_GetWorkspaceMap_Call {
	return &mockDataUsageAccountRepository_GetWorkspaceMap_Call{Call: _e.mock.On("GetWorkspaceMap", ctx, metastores, workspaces)}
}

func (_c *mockDataUsageAccountRepository_GetWorkspaceMap_Call) Run(run func(ctx context.Context, metastores []catalog.MetastoreInfo, workspaces []Workspace)) *mockDataUsageAccountRepository_GetWorkspaceMap_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(context.Context), args[1].([]catalog.MetastoreInfo), args[2].([]Workspace))
	})
	return _c
}

func (_c *mockDataUsageAccountRepository_GetWorkspaceMap_Call) Return(_a0 map[string][]string, _a1 map[string]string, _a2 error) *mockDataUsageAccountRepository_GetWorkspaceMap_Call {
	_c.Call.Return(_a0, _a1, _a2)
	return _c
}

func (_c *mockDataUsageAccountRepository_GetWorkspaceMap_Call) RunAndReturn(run func(context.Context, []catalog.MetastoreInfo, []Workspace) (map[string][]string, map[string]string, error)) *mockDataUsageAccountRepository_GetWorkspaceMap_Call {
	_c.Call.Return(run)
	return _c
}

// GetWorkspaces provides a mock function with given fields: ctx
func (_m *mockDataUsageAccountRepository) GetWorkspaces(ctx context.Context) ([]Workspace, error) {
	ret := _m.Called(ctx)

	var r0 []Workspace
	var r1 error
	if rf, ok := ret.Get(0).(func(context.Context) ([]Workspace, error)); ok {
		return rf(ctx)
	}
	if rf, ok := ret.Get(0).(func(context.Context) []Workspace); ok {
		r0 = rf(ctx)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).([]Workspace)
		}
	}

	if rf, ok := ret.Get(1).(func(context.Context) error); ok {
		r1 = rf(ctx)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// mockDataUsageAccountRepository_GetWorkspaces_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'GetWorkspaces'
type mockDataUsageAccountRepository_GetWorkspaces_Call struct {
	*mock.Call
}

// GetWorkspaces is a helper method to define mock.On call
//   - ctx context.Context
func (_e *mockDataUsageAccountRepository_Expecter) GetWorkspaces(ctx interface{}) *mockDataUsageAccountRepository_GetWorkspaces_Call {
	return &mockDataUsageAccountRepository_GetWorkspaces_Call{Call: _e.mock.On("GetWorkspaces", ctx)}
}

func (_c *mockDataUsageAccountRepository_GetWorkspaces_Call) Run(run func(ctx context.Context)) *mockDataUsageAccountRepository_GetWorkspaces_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(context.Context))
	})
	return _c
}

func (_c *mockDataUsageAccountRepository_GetWorkspaces_Call) Return(_a0 []Workspace, _a1 error) *mockDataUsageAccountRepository_GetWorkspaces_Call {
	_c.Call.Return(_a0, _a1)
	return _c
}

func (_c *mockDataUsageAccountRepository_GetWorkspaces_Call) RunAndReturn(run func(context.Context) ([]Workspace, error)) *mockDataUsageAccountRepository_GetWorkspaces_Call {
	_c.Call.Return(run)
	return _c
}

// ListMetastores provides a mock function with given fields: ctx
func (_m *mockDataUsageAccountRepository) ListMetastores(ctx context.Context) ([]catalog.MetastoreInfo, error) {
	ret := _m.Called(ctx)

	var r0 []catalog.MetastoreInfo
	var r1 error
	if rf, ok := ret.Get(0).(func(context.Context) ([]catalog.MetastoreInfo, error)); ok {
		return rf(ctx)
	}
	if rf, ok := ret.Get(0).(func(context.Context) []catalog.MetastoreInfo); ok {
		r0 = rf(ctx)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).([]catalog.MetastoreInfo)
		}
	}

	if rf, ok := ret.Get(1).(func(context.Context) error); ok {
		r1 = rf(ctx)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// mockDataUsageAccountRepository_ListMetastores_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'ListMetastores'
type mockDataUsageAccountRepository_ListMetastores_Call struct {
	*mock.Call
}

// ListMetastores is a helper method to define mock.On call
//   - ctx context.Context
func (_e *mockDataUsageAccountRepository_Expecter) ListMetastores(ctx interface{}) *mockDataUsageAccountRepository_ListMetastores_Call {
	return &mockDataUsageAccountRepository_ListMetastores_Call{Call: _e.mock.On("ListMetastores", ctx)}
}

func (_c *mockDataUsageAccountRepository_ListMetastores_Call) Run(run func(ctx context.Context)) *mockDataUsageAccountRepository_ListMetastores_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(context.Context))
	})
	return _c
}

func (_c *mockDataUsageAccountRepository_ListMetastores_Call) Return(_a0 []catalog.MetastoreInfo, _a1 error) *mockDataUsageAccountRepository_ListMetastores_Call {
	_c.Call.Return(_a0, _a1)
	return _c
}

func (_c *mockDataUsageAccountRepository_ListMetastores_Call) RunAndReturn(run func(context.Context) ([]catalog.MetastoreInfo, error)) *mockDataUsageAccountRepository_ListMetastores_Call {
	_c.Call.Return(run)
	return _c
}

// newMockDataUsageAccountRepository creates a new instance of mockDataUsageAccountRepository. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
// The first argument is typically a *testing.T value.
func newMockDataUsageAccountRepository(t interface {
	mock.TestingT
	Cleanup(func())
}) *mockDataUsageAccountRepository {
	mock := &mockDataUsageAccountRepository{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}
