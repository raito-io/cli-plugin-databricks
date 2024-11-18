// Code generated by mockery v2.46.3. DO NOT EDIT.

package databricks

import (
	context "context"

	catalog "github.com/databricks/databricks-sdk-go/service/catalog"

	iam "github.com/databricks/databricks-sdk-go/service/iam"

	mock "github.com/stretchr/testify/mock"

	provisioning "github.com/databricks/databricks-sdk-go/service/provisioning"

	repo "cli-plugin-databricks/databricks/repo"

	types "cli-plugin-databricks/databricks/repo/types"
)

// mockDataAccessAccountRepository is an autogenerated mock type for the dataAccessAccountRepository type
type mockDataAccessAccountRepository struct {
	mock.Mock
}

type mockDataAccessAccountRepository_Expecter struct {
	mock *mock.Mock
}

func (_m *mockDataAccessAccountRepository) EXPECT() *mockDataAccessAccountRepository_Expecter {
	return &mockDataAccessAccountRepository_Expecter{mock: &_m.Mock}
}

// GetWorkspaceByName provides a mock function with given fields: ctx, workspaceName
func (_m *mockDataAccessAccountRepository) GetWorkspaceByName(ctx context.Context, workspaceName string) (*provisioning.Workspace, error) {
	ret := _m.Called(ctx, workspaceName)

	if len(ret) == 0 {
		panic("no return value specified for GetWorkspaceByName")
	}

	var r0 *provisioning.Workspace
	var r1 error
	if rf, ok := ret.Get(0).(func(context.Context, string) (*provisioning.Workspace, error)); ok {
		return rf(ctx, workspaceName)
	}
	if rf, ok := ret.Get(0).(func(context.Context, string) *provisioning.Workspace); ok {
		r0 = rf(ctx, workspaceName)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*provisioning.Workspace)
		}
	}

	if rf, ok := ret.Get(1).(func(context.Context, string) error); ok {
		r1 = rf(ctx, workspaceName)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// mockDataAccessAccountRepository_GetWorkspaceByName_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'GetWorkspaceByName'
type mockDataAccessAccountRepository_GetWorkspaceByName_Call struct {
	*mock.Call
}

// GetWorkspaceByName is a helper method to define mock.On call
//   - ctx context.Context
//   - workspaceName string
func (_e *mockDataAccessAccountRepository_Expecter) GetWorkspaceByName(ctx interface{}, workspaceName interface{}) *mockDataAccessAccountRepository_GetWorkspaceByName_Call {
	return &mockDataAccessAccountRepository_GetWorkspaceByName_Call{Call: _e.mock.On("GetWorkspaceByName", ctx, workspaceName)}
}

func (_c *mockDataAccessAccountRepository_GetWorkspaceByName_Call) Run(run func(ctx context.Context, workspaceName string)) *mockDataAccessAccountRepository_GetWorkspaceByName_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(context.Context), args[1].(string))
	})
	return _c
}

func (_c *mockDataAccessAccountRepository_GetWorkspaceByName_Call) Return(_a0 *provisioning.Workspace, _a1 error) *mockDataAccessAccountRepository_GetWorkspaceByName_Call {
	_c.Call.Return(_a0, _a1)
	return _c
}

func (_c *mockDataAccessAccountRepository_GetWorkspaceByName_Call) RunAndReturn(run func(context.Context, string) (*provisioning.Workspace, error)) *mockDataAccessAccountRepository_GetWorkspaceByName_Call {
	_c.Call.Return(run)
	return _c
}

// GetWorkspaceMap provides a mock function with given fields: ctx, metastores, workspaces
func (_m *mockDataAccessAccountRepository) GetWorkspaceMap(ctx context.Context, metastores []catalog.MetastoreInfo, workspaces []provisioning.Workspace) (map[string][]*provisioning.Workspace, map[string]string, error) {
	ret := _m.Called(ctx, metastores, workspaces)

	if len(ret) == 0 {
		panic("no return value specified for GetWorkspaceMap")
	}

	var r0 map[string][]*provisioning.Workspace
	var r1 map[string]string
	var r2 error
	if rf, ok := ret.Get(0).(func(context.Context, []catalog.MetastoreInfo, []provisioning.Workspace) (map[string][]*provisioning.Workspace, map[string]string, error)); ok {
		return rf(ctx, metastores, workspaces)
	}
	if rf, ok := ret.Get(0).(func(context.Context, []catalog.MetastoreInfo, []provisioning.Workspace) map[string][]*provisioning.Workspace); ok {
		r0 = rf(ctx, metastores, workspaces)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(map[string][]*provisioning.Workspace)
		}
	}

	if rf, ok := ret.Get(1).(func(context.Context, []catalog.MetastoreInfo, []provisioning.Workspace) map[string]string); ok {
		r1 = rf(ctx, metastores, workspaces)
	} else {
		if ret.Get(1) != nil {
			r1 = ret.Get(1).(map[string]string)
		}
	}

	if rf, ok := ret.Get(2).(func(context.Context, []catalog.MetastoreInfo, []provisioning.Workspace) error); ok {
		r2 = rf(ctx, metastores, workspaces)
	} else {
		r2 = ret.Error(2)
	}

	return r0, r1, r2
}

// mockDataAccessAccountRepository_GetWorkspaceMap_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'GetWorkspaceMap'
type mockDataAccessAccountRepository_GetWorkspaceMap_Call struct {
	*mock.Call
}

// GetWorkspaceMap is a helper method to define mock.On call
//   - ctx context.Context
//   - metastores []catalog.MetastoreInfo
//   - workspaces []provisioning.Workspace
func (_e *mockDataAccessAccountRepository_Expecter) GetWorkspaceMap(ctx interface{}, metastores interface{}, workspaces interface{}) *mockDataAccessAccountRepository_GetWorkspaceMap_Call {
	return &mockDataAccessAccountRepository_GetWorkspaceMap_Call{Call: _e.mock.On("GetWorkspaceMap", ctx, metastores, workspaces)}
}

func (_c *mockDataAccessAccountRepository_GetWorkspaceMap_Call) Run(run func(ctx context.Context, metastores []catalog.MetastoreInfo, workspaces []provisioning.Workspace)) *mockDataAccessAccountRepository_GetWorkspaceMap_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(context.Context), args[1].([]catalog.MetastoreInfo), args[2].([]provisioning.Workspace))
	})
	return _c
}

func (_c *mockDataAccessAccountRepository_GetWorkspaceMap_Call) Return(_a0 map[string][]*provisioning.Workspace, _a1 map[string]string, _a2 error) *mockDataAccessAccountRepository_GetWorkspaceMap_Call {
	_c.Call.Return(_a0, _a1, _a2)
	return _c
}

func (_c *mockDataAccessAccountRepository_GetWorkspaceMap_Call) RunAndReturn(run func(context.Context, []catalog.MetastoreInfo, []provisioning.Workspace) (map[string][]*provisioning.Workspace, map[string]string, error)) *mockDataAccessAccountRepository_GetWorkspaceMap_Call {
	_c.Call.Return(run)
	return _c
}

// GetWorkspaces provides a mock function with given fields: ctx
func (_m *mockDataAccessAccountRepository) GetWorkspaces(ctx context.Context) ([]provisioning.Workspace, error) {
	ret := _m.Called(ctx)

	if len(ret) == 0 {
		panic("no return value specified for GetWorkspaces")
	}

	var r0 []provisioning.Workspace
	var r1 error
	if rf, ok := ret.Get(0).(func(context.Context) ([]provisioning.Workspace, error)); ok {
		return rf(ctx)
	}
	if rf, ok := ret.Get(0).(func(context.Context) []provisioning.Workspace); ok {
		r0 = rf(ctx)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).([]provisioning.Workspace)
		}
	}

	if rf, ok := ret.Get(1).(func(context.Context) error); ok {
		r1 = rf(ctx)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// mockDataAccessAccountRepository_GetWorkspaces_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'GetWorkspaces'
type mockDataAccessAccountRepository_GetWorkspaces_Call struct {
	*mock.Call
}

// GetWorkspaces is a helper method to define mock.On call
//   - ctx context.Context
func (_e *mockDataAccessAccountRepository_Expecter) GetWorkspaces(ctx interface{}) *mockDataAccessAccountRepository_GetWorkspaces_Call {
	return &mockDataAccessAccountRepository_GetWorkspaces_Call{Call: _e.mock.On("GetWorkspaces", ctx)}
}

func (_c *mockDataAccessAccountRepository_GetWorkspaces_Call) Run(run func(ctx context.Context)) *mockDataAccessAccountRepository_GetWorkspaces_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(context.Context))
	})
	return _c
}

func (_c *mockDataAccessAccountRepository_GetWorkspaces_Call) Return(_a0 []provisioning.Workspace, _a1 error) *mockDataAccessAccountRepository_GetWorkspaces_Call {
	_c.Call.Return(_a0, _a1)
	return _c
}

func (_c *mockDataAccessAccountRepository_GetWorkspaces_Call) RunAndReturn(run func(context.Context) ([]provisioning.Workspace, error)) *mockDataAccessAccountRepository_GetWorkspaces_Call {
	_c.Call.Return(run)
	return _c
}

// ListGroups provides a mock function with given fields: ctx, optFn
func (_m *mockDataAccessAccountRepository) ListGroups(ctx context.Context, optFn ...func(*types.DatabricksGroupsFilter)) <-chan repo.ChannelItem[iam.Group] {
	_va := make([]interface{}, len(optFn))
	for _i := range optFn {
		_va[_i] = optFn[_i]
	}
	var _ca []interface{}
	_ca = append(_ca, ctx)
	_ca = append(_ca, _va...)
	ret := _m.Called(_ca...)

	if len(ret) == 0 {
		panic("no return value specified for ListGroups")
	}

	var r0 <-chan repo.ChannelItem[iam.Group]
	if rf, ok := ret.Get(0).(func(context.Context, ...func(*types.DatabricksGroupsFilter)) <-chan repo.ChannelItem[iam.Group]); ok {
		r0 = rf(ctx, optFn...)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(<-chan repo.ChannelItem[iam.Group])
		}
	}

	return r0
}

// mockDataAccessAccountRepository_ListGroups_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'ListGroups'
type mockDataAccessAccountRepository_ListGroups_Call struct {
	*mock.Call
}

// ListGroups is a helper method to define mock.On call
//   - ctx context.Context
//   - optFn ...func(*types.DatabricksGroupsFilter)
func (_e *mockDataAccessAccountRepository_Expecter) ListGroups(ctx interface{}, optFn ...interface{}) *mockDataAccessAccountRepository_ListGroups_Call {
	return &mockDataAccessAccountRepository_ListGroups_Call{Call: _e.mock.On("ListGroups",
		append([]interface{}{ctx}, optFn...)...)}
}

func (_c *mockDataAccessAccountRepository_ListGroups_Call) Run(run func(ctx context.Context, optFn ...func(*types.DatabricksGroupsFilter))) *mockDataAccessAccountRepository_ListGroups_Call {
	_c.Call.Run(func(args mock.Arguments) {
		variadicArgs := make([]func(*types.DatabricksGroupsFilter), len(args)-1)
		for i, a := range args[1:] {
			if a != nil {
				variadicArgs[i] = a.(func(*types.DatabricksGroupsFilter))
			}
		}
		run(args[0].(context.Context), variadicArgs...)
	})
	return _c
}

func (_c *mockDataAccessAccountRepository_ListGroups_Call) Return(_a0 <-chan repo.ChannelItem[iam.Group]) *mockDataAccessAccountRepository_ListGroups_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *mockDataAccessAccountRepository_ListGroups_Call) RunAndReturn(run func(context.Context, ...func(*types.DatabricksGroupsFilter)) <-chan repo.ChannelItem[iam.Group]) *mockDataAccessAccountRepository_ListGroups_Call {
	_c.Call.Return(run)
	return _c
}

// ListMetastores provides a mock function with given fields: ctx
func (_m *mockDataAccessAccountRepository) ListMetastores(ctx context.Context) ([]catalog.MetastoreInfo, error) {
	ret := _m.Called(ctx)

	if len(ret) == 0 {
		panic("no return value specified for ListMetastores")
	}

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

// mockDataAccessAccountRepository_ListMetastores_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'ListMetastores'
type mockDataAccessAccountRepository_ListMetastores_Call struct {
	*mock.Call
}

// ListMetastores is a helper method to define mock.On call
//   - ctx context.Context
func (_e *mockDataAccessAccountRepository_Expecter) ListMetastores(ctx interface{}) *mockDataAccessAccountRepository_ListMetastores_Call {
	return &mockDataAccessAccountRepository_ListMetastores_Call{Call: _e.mock.On("ListMetastores", ctx)}
}

func (_c *mockDataAccessAccountRepository_ListMetastores_Call) Run(run func(ctx context.Context)) *mockDataAccessAccountRepository_ListMetastores_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(context.Context))
	})
	return _c
}

func (_c *mockDataAccessAccountRepository_ListMetastores_Call) Return(_a0 []catalog.MetastoreInfo, _a1 error) *mockDataAccessAccountRepository_ListMetastores_Call {
	_c.Call.Return(_a0, _a1)
	return _c
}

func (_c *mockDataAccessAccountRepository_ListMetastores_Call) RunAndReturn(run func(context.Context) ([]catalog.MetastoreInfo, error)) *mockDataAccessAccountRepository_ListMetastores_Call {
	_c.Call.Return(run)
	return _c
}

// ListServicePrincipals provides a mock function with given fields: ctx, optFn
func (_m *mockDataAccessAccountRepository) ListServicePrincipals(ctx context.Context, optFn ...func(*types.DatabricksServicePrincipalFilter)) <-chan repo.ChannelItem[iam.ServicePrincipal] {
	_va := make([]interface{}, len(optFn))
	for _i := range optFn {
		_va[_i] = optFn[_i]
	}
	var _ca []interface{}
	_ca = append(_ca, ctx)
	_ca = append(_ca, _va...)
	ret := _m.Called(_ca...)

	if len(ret) == 0 {
		panic("no return value specified for ListServicePrincipals")
	}

	var r0 <-chan repo.ChannelItem[iam.ServicePrincipal]
	if rf, ok := ret.Get(0).(func(context.Context, ...func(*types.DatabricksServicePrincipalFilter)) <-chan repo.ChannelItem[iam.ServicePrincipal]); ok {
		r0 = rf(ctx, optFn...)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(<-chan repo.ChannelItem[iam.ServicePrincipal])
		}
	}

	return r0
}

// mockDataAccessAccountRepository_ListServicePrincipals_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'ListServicePrincipals'
type mockDataAccessAccountRepository_ListServicePrincipals_Call struct {
	*mock.Call
}

// ListServicePrincipals is a helper method to define mock.On call
//   - ctx context.Context
//   - optFn ...func(*types.DatabricksServicePrincipalFilter)
func (_e *mockDataAccessAccountRepository_Expecter) ListServicePrincipals(ctx interface{}, optFn ...interface{}) *mockDataAccessAccountRepository_ListServicePrincipals_Call {
	return &mockDataAccessAccountRepository_ListServicePrincipals_Call{Call: _e.mock.On("ListServicePrincipals",
		append([]interface{}{ctx}, optFn...)...)}
}

func (_c *mockDataAccessAccountRepository_ListServicePrincipals_Call) Run(run func(ctx context.Context, optFn ...func(*types.DatabricksServicePrincipalFilter))) *mockDataAccessAccountRepository_ListServicePrincipals_Call {
	_c.Call.Run(func(args mock.Arguments) {
		variadicArgs := make([]func(*types.DatabricksServicePrincipalFilter), len(args)-1)
		for i, a := range args[1:] {
			if a != nil {
				variadicArgs[i] = a.(func(*types.DatabricksServicePrincipalFilter))
			}
		}
		run(args[0].(context.Context), variadicArgs...)
	})
	return _c
}

func (_c *mockDataAccessAccountRepository_ListServicePrincipals_Call) Return(_a0 <-chan repo.ChannelItem[iam.ServicePrincipal]) *mockDataAccessAccountRepository_ListServicePrincipals_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *mockDataAccessAccountRepository_ListServicePrincipals_Call) RunAndReturn(run func(context.Context, ...func(*types.DatabricksServicePrincipalFilter)) <-chan repo.ChannelItem[iam.ServicePrincipal]) *mockDataAccessAccountRepository_ListServicePrincipals_Call {
	_c.Call.Return(run)
	return _c
}

// ListUsers provides a mock function with given fields: ctx, optFn
func (_m *mockDataAccessAccountRepository) ListUsers(ctx context.Context, optFn ...func(*types.DatabricksUsersFilter)) <-chan repo.ChannelItem[iam.User] {
	_va := make([]interface{}, len(optFn))
	for _i := range optFn {
		_va[_i] = optFn[_i]
	}
	var _ca []interface{}
	_ca = append(_ca, ctx)
	_ca = append(_ca, _va...)
	ret := _m.Called(_ca...)

	if len(ret) == 0 {
		panic("no return value specified for ListUsers")
	}

	var r0 <-chan repo.ChannelItem[iam.User]
	if rf, ok := ret.Get(0).(func(context.Context, ...func(*types.DatabricksUsersFilter)) <-chan repo.ChannelItem[iam.User]); ok {
		r0 = rf(ctx, optFn...)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(<-chan repo.ChannelItem[iam.User])
		}
	}

	return r0
}

// mockDataAccessAccountRepository_ListUsers_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'ListUsers'
type mockDataAccessAccountRepository_ListUsers_Call struct {
	*mock.Call
}

// ListUsers is a helper method to define mock.On call
//   - ctx context.Context
//   - optFn ...func(*types.DatabricksUsersFilter)
func (_e *mockDataAccessAccountRepository_Expecter) ListUsers(ctx interface{}, optFn ...interface{}) *mockDataAccessAccountRepository_ListUsers_Call {
	return &mockDataAccessAccountRepository_ListUsers_Call{Call: _e.mock.On("ListUsers",
		append([]interface{}{ctx}, optFn...)...)}
}

func (_c *mockDataAccessAccountRepository_ListUsers_Call) Run(run func(ctx context.Context, optFn ...func(*types.DatabricksUsersFilter))) *mockDataAccessAccountRepository_ListUsers_Call {
	_c.Call.Run(func(args mock.Arguments) {
		variadicArgs := make([]func(*types.DatabricksUsersFilter), len(args)-1)
		for i, a := range args[1:] {
			if a != nil {
				variadicArgs[i] = a.(func(*types.DatabricksUsersFilter))
			}
		}
		run(args[0].(context.Context), variadicArgs...)
	})
	return _c
}

func (_c *mockDataAccessAccountRepository_ListUsers_Call) Return(_a0 <-chan repo.ChannelItem[iam.User]) *mockDataAccessAccountRepository_ListUsers_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *mockDataAccessAccountRepository_ListUsers_Call) RunAndReturn(run func(context.Context, ...func(*types.DatabricksUsersFilter)) <-chan repo.ChannelItem[iam.User]) *mockDataAccessAccountRepository_ListUsers_Call {
	_c.Call.Return(run)
	return _c
}

// ListWorkspaceAssignments provides a mock function with given fields: ctx, workspaceId
func (_m *mockDataAccessAccountRepository) ListWorkspaceAssignments(ctx context.Context, workspaceId int64) ([]iam.PermissionAssignment, error) {
	ret := _m.Called(ctx, workspaceId)

	if len(ret) == 0 {
		panic("no return value specified for ListWorkspaceAssignments")
	}

	var r0 []iam.PermissionAssignment
	var r1 error
	if rf, ok := ret.Get(0).(func(context.Context, int64) ([]iam.PermissionAssignment, error)); ok {
		return rf(ctx, workspaceId)
	}
	if rf, ok := ret.Get(0).(func(context.Context, int64) []iam.PermissionAssignment); ok {
		r0 = rf(ctx, workspaceId)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).([]iam.PermissionAssignment)
		}
	}

	if rf, ok := ret.Get(1).(func(context.Context, int64) error); ok {
		r1 = rf(ctx, workspaceId)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// mockDataAccessAccountRepository_ListWorkspaceAssignments_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'ListWorkspaceAssignments'
type mockDataAccessAccountRepository_ListWorkspaceAssignments_Call struct {
	*mock.Call
}

// ListWorkspaceAssignments is a helper method to define mock.On call
//   - ctx context.Context
//   - workspaceId int64
func (_e *mockDataAccessAccountRepository_Expecter) ListWorkspaceAssignments(ctx interface{}, workspaceId interface{}) *mockDataAccessAccountRepository_ListWorkspaceAssignments_Call {
	return &mockDataAccessAccountRepository_ListWorkspaceAssignments_Call{Call: _e.mock.On("ListWorkspaceAssignments", ctx, workspaceId)}
}

func (_c *mockDataAccessAccountRepository_ListWorkspaceAssignments_Call) Run(run func(ctx context.Context, workspaceId int64)) *mockDataAccessAccountRepository_ListWorkspaceAssignments_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(context.Context), args[1].(int64))
	})
	return _c
}

func (_c *mockDataAccessAccountRepository_ListWorkspaceAssignments_Call) Return(_a0 []iam.PermissionAssignment, _a1 error) *mockDataAccessAccountRepository_ListWorkspaceAssignments_Call {
	_c.Call.Return(_a0, _a1)
	return _c
}

func (_c *mockDataAccessAccountRepository_ListWorkspaceAssignments_Call) RunAndReturn(run func(context.Context, int64) ([]iam.PermissionAssignment, error)) *mockDataAccessAccountRepository_ListWorkspaceAssignments_Call {
	_c.Call.Return(run)
	return _c
}

// UpdateWorkspaceAssignment provides a mock function with given fields: ctx, workspaceId, principalId, permission
func (_m *mockDataAccessAccountRepository) UpdateWorkspaceAssignment(ctx context.Context, workspaceId int64, principalId int64, permission []iam.WorkspacePermission) error {
	ret := _m.Called(ctx, workspaceId, principalId, permission)

	if len(ret) == 0 {
		panic("no return value specified for UpdateWorkspaceAssignment")
	}

	var r0 error
	if rf, ok := ret.Get(0).(func(context.Context, int64, int64, []iam.WorkspacePermission) error); ok {
		r0 = rf(ctx, workspaceId, principalId, permission)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// mockDataAccessAccountRepository_UpdateWorkspaceAssignment_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'UpdateWorkspaceAssignment'
type mockDataAccessAccountRepository_UpdateWorkspaceAssignment_Call struct {
	*mock.Call
}

// UpdateWorkspaceAssignment is a helper method to define mock.On call
//   - ctx context.Context
//   - workspaceId int64
//   - principalId int64
//   - permission []iam.WorkspacePermission
func (_e *mockDataAccessAccountRepository_Expecter) UpdateWorkspaceAssignment(ctx interface{}, workspaceId interface{}, principalId interface{}, permission interface{}) *mockDataAccessAccountRepository_UpdateWorkspaceAssignment_Call {
	return &mockDataAccessAccountRepository_UpdateWorkspaceAssignment_Call{Call: _e.mock.On("UpdateWorkspaceAssignment", ctx, workspaceId, principalId, permission)}
}

func (_c *mockDataAccessAccountRepository_UpdateWorkspaceAssignment_Call) Run(run func(ctx context.Context, workspaceId int64, principalId int64, permission []iam.WorkspacePermission)) *mockDataAccessAccountRepository_UpdateWorkspaceAssignment_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(context.Context), args[1].(int64), args[2].(int64), args[3].([]iam.WorkspacePermission))
	})
	return _c
}

func (_c *mockDataAccessAccountRepository_UpdateWorkspaceAssignment_Call) Return(_a0 error) *mockDataAccessAccountRepository_UpdateWorkspaceAssignment_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *mockDataAccessAccountRepository_UpdateWorkspaceAssignment_Call) RunAndReturn(run func(context.Context, int64, int64, []iam.WorkspacePermission) error) *mockDataAccessAccountRepository_UpdateWorkspaceAssignment_Call {
	_c.Call.Return(run)
	return _c
}

// newMockDataAccessAccountRepository creates a new instance of mockDataAccessAccountRepository. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
// The first argument is typically a *testing.T value.
func newMockDataAccessAccountRepository(t interface {
	mock.TestingT
	Cleanup(func())
}) *mockDataAccessAccountRepository {
	mock := &mockDataAccessAccountRepository{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}
