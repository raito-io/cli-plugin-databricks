// Code generated by mockery v2.40.1. DO NOT EDIT.

package databricks

import (
	context "context"

	catalog "github.com/databricks/databricks-sdk-go/service/catalog"

	mock "github.com/stretchr/testify/mock"

	repo "cli-plugin-databricks/databricks/repo"
)

// mockDataAccessWorkspaceRepository is an autogenerated mock type for the dataAccessWorkspaceRepository type
type mockDataAccessWorkspaceRepository struct {
	mock.Mock
}

type mockDataAccessWorkspaceRepository_Expecter struct {
	mock *mock.Mock
}

func (_m *mockDataAccessWorkspaceRepository) EXPECT() *mockDataAccessWorkspaceRepository_Expecter {
	return &mockDataAccessWorkspaceRepository_Expecter{mock: &_m.Mock}
}

// GetOwner provides a mock function with given fields: ctx, securableType, fullName
func (_m *mockDataAccessWorkspaceRepository) GetOwner(ctx context.Context, securableType catalog.SecurableType, fullName string) (string, error) {
	ret := _m.Called(ctx, securableType, fullName)

	if len(ret) == 0 {
		panic("no return value specified for GetOwner")
	}

	var r0 string
	var r1 error
	if rf, ok := ret.Get(0).(func(context.Context, catalog.SecurableType, string) (string, error)); ok {
		return rf(ctx, securableType, fullName)
	}
	if rf, ok := ret.Get(0).(func(context.Context, catalog.SecurableType, string) string); ok {
		r0 = rf(ctx, securableType, fullName)
	} else {
		r0 = ret.Get(0).(string)
	}

	if rf, ok := ret.Get(1).(func(context.Context, catalog.SecurableType, string) error); ok {
		r1 = rf(ctx, securableType, fullName)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// mockDataAccessWorkspaceRepository_GetOwner_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'GetOwner'
type mockDataAccessWorkspaceRepository_GetOwner_Call struct {
	*mock.Call
}

// GetOwner is a helper method to define mock.On call
//   - ctx context.Context
//   - securableType catalog.SecurableType
//   - fullName string
func (_e *mockDataAccessWorkspaceRepository_Expecter) GetOwner(ctx interface{}, securableType interface{}, fullName interface{}) *mockDataAccessWorkspaceRepository_GetOwner_Call {
	return &mockDataAccessWorkspaceRepository_GetOwner_Call{Call: _e.mock.On("GetOwner", ctx, securableType, fullName)}
}

func (_c *mockDataAccessWorkspaceRepository_GetOwner_Call) Run(run func(ctx context.Context, securableType catalog.SecurableType, fullName string)) *mockDataAccessWorkspaceRepository_GetOwner_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(context.Context), args[1].(catalog.SecurableType), args[2].(string))
	})
	return _c
}

func (_c *mockDataAccessWorkspaceRepository_GetOwner_Call) Return(_a0 string, _a1 error) *mockDataAccessWorkspaceRepository_GetOwner_Call {
	_c.Call.Return(_a0, _a1)
	return _c
}

func (_c *mockDataAccessWorkspaceRepository_GetOwner_Call) RunAndReturn(run func(context.Context, catalog.SecurableType, string) (string, error)) *mockDataAccessWorkspaceRepository_GetOwner_Call {
	_c.Call.Return(run)
	return _c
}

// GetPermissionsOnResource provides a mock function with given fields: ctx, securableType, fullName
func (_m *mockDataAccessWorkspaceRepository) GetPermissionsOnResource(ctx context.Context, securableType catalog.SecurableType, fullName string) (*catalog.PermissionsList, error) {
	ret := _m.Called(ctx, securableType, fullName)

	if len(ret) == 0 {
		panic("no return value specified for GetPermissionsOnResource")
	}

	var r0 *catalog.PermissionsList
	var r1 error
	if rf, ok := ret.Get(0).(func(context.Context, catalog.SecurableType, string) (*catalog.PermissionsList, error)); ok {
		return rf(ctx, securableType, fullName)
	}
	if rf, ok := ret.Get(0).(func(context.Context, catalog.SecurableType, string) *catalog.PermissionsList); ok {
		r0 = rf(ctx, securableType, fullName)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*catalog.PermissionsList)
		}
	}

	if rf, ok := ret.Get(1).(func(context.Context, catalog.SecurableType, string) error); ok {
		r1 = rf(ctx, securableType, fullName)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// mockDataAccessWorkspaceRepository_GetPermissionsOnResource_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'GetPermissionsOnResource'
type mockDataAccessWorkspaceRepository_GetPermissionsOnResource_Call struct {
	*mock.Call
}

// GetPermissionsOnResource is a helper method to define mock.On call
//   - ctx context.Context
//   - securableType catalog.SecurableType
//   - fullName string
func (_e *mockDataAccessWorkspaceRepository_Expecter) GetPermissionsOnResource(ctx interface{}, securableType interface{}, fullName interface{}) *mockDataAccessWorkspaceRepository_GetPermissionsOnResource_Call {
	return &mockDataAccessWorkspaceRepository_GetPermissionsOnResource_Call{Call: _e.mock.On("GetPermissionsOnResource", ctx, securableType, fullName)}
}

func (_c *mockDataAccessWorkspaceRepository_GetPermissionsOnResource_Call) Run(run func(ctx context.Context, securableType catalog.SecurableType, fullName string)) *mockDataAccessWorkspaceRepository_GetPermissionsOnResource_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(context.Context), args[1].(catalog.SecurableType), args[2].(string))
	})
	return _c
}

func (_c *mockDataAccessWorkspaceRepository_GetPermissionsOnResource_Call) Return(_a0 *catalog.PermissionsList, _a1 error) *mockDataAccessWorkspaceRepository_GetPermissionsOnResource_Call {
	_c.Call.Return(_a0, _a1)
	return _c
}

func (_c *mockDataAccessWorkspaceRepository_GetPermissionsOnResource_Call) RunAndReturn(run func(context.Context, catalog.SecurableType, string) (*catalog.PermissionsList, error)) *mockDataAccessWorkspaceRepository_GetPermissionsOnResource_Call {
	_c.Call.Return(run)
	return _c
}

// ListCatalogs provides a mock function with given fields: ctx
func (_m *mockDataAccessWorkspaceRepository) ListCatalogs(ctx context.Context) ([]catalog.CatalogInfo, error) {
	ret := _m.Called(ctx)

	if len(ret) == 0 {
		panic("no return value specified for ListCatalogs")
	}

	var r0 []catalog.CatalogInfo
	var r1 error
	if rf, ok := ret.Get(0).(func(context.Context) ([]catalog.CatalogInfo, error)); ok {
		return rf(ctx)
	}
	if rf, ok := ret.Get(0).(func(context.Context) []catalog.CatalogInfo); ok {
		r0 = rf(ctx)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).([]catalog.CatalogInfo)
		}
	}

	if rf, ok := ret.Get(1).(func(context.Context) error); ok {
		r1 = rf(ctx)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// mockDataAccessWorkspaceRepository_ListCatalogs_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'ListCatalogs'
type mockDataAccessWorkspaceRepository_ListCatalogs_Call struct {
	*mock.Call
}

// ListCatalogs is a helper method to define mock.On call
//   - ctx context.Context
func (_e *mockDataAccessWorkspaceRepository_Expecter) ListCatalogs(ctx interface{}) *mockDataAccessWorkspaceRepository_ListCatalogs_Call {
	return &mockDataAccessWorkspaceRepository_ListCatalogs_Call{Call: _e.mock.On("ListCatalogs", ctx)}
}

func (_c *mockDataAccessWorkspaceRepository_ListCatalogs_Call) Run(run func(ctx context.Context)) *mockDataAccessWorkspaceRepository_ListCatalogs_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(context.Context))
	})
	return _c
}

func (_c *mockDataAccessWorkspaceRepository_ListCatalogs_Call) Return(_a0 []catalog.CatalogInfo, _a1 error) *mockDataAccessWorkspaceRepository_ListCatalogs_Call {
	_c.Call.Return(_a0, _a1)
	return _c
}

func (_c *mockDataAccessWorkspaceRepository_ListCatalogs_Call) RunAndReturn(run func(context.Context) ([]catalog.CatalogInfo, error)) *mockDataAccessWorkspaceRepository_ListCatalogs_Call {
	_c.Call.Return(run)
	return _c
}

// ListFunctions provides a mock function with given fields: ctx, catalogName, schemaName
func (_m *mockDataAccessWorkspaceRepository) ListFunctions(ctx context.Context, catalogName string, schemaName string) ([]repo.FunctionInfo, error) {
	ret := _m.Called(ctx, catalogName, schemaName)

	if len(ret) == 0 {
		panic("no return value specified for ListFunctions")
	}

	var r0 []repo.FunctionInfo
	var r1 error
	if rf, ok := ret.Get(0).(func(context.Context, string, string) ([]repo.FunctionInfo, error)); ok {
		return rf(ctx, catalogName, schemaName)
	}
	if rf, ok := ret.Get(0).(func(context.Context, string, string) []repo.FunctionInfo); ok {
		r0 = rf(ctx, catalogName, schemaName)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).([]repo.FunctionInfo)
		}
	}

	if rf, ok := ret.Get(1).(func(context.Context, string, string) error); ok {
		r1 = rf(ctx, catalogName, schemaName)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// mockDataAccessWorkspaceRepository_ListFunctions_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'ListFunctions'
type mockDataAccessWorkspaceRepository_ListFunctions_Call struct {
	*mock.Call
}

// ListFunctions is a helper method to define mock.On call
//   - ctx context.Context
//   - catalogName string
//   - schemaName string
func (_e *mockDataAccessWorkspaceRepository_Expecter) ListFunctions(ctx interface{}, catalogName interface{}, schemaName interface{}) *mockDataAccessWorkspaceRepository_ListFunctions_Call {
	return &mockDataAccessWorkspaceRepository_ListFunctions_Call{Call: _e.mock.On("ListFunctions", ctx, catalogName, schemaName)}
}

func (_c *mockDataAccessWorkspaceRepository_ListFunctions_Call) Run(run func(ctx context.Context, catalogName string, schemaName string)) *mockDataAccessWorkspaceRepository_ListFunctions_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(context.Context), args[1].(string), args[2].(string))
	})
	return _c
}

func (_c *mockDataAccessWorkspaceRepository_ListFunctions_Call) Return(_a0 []repo.FunctionInfo, _a1 error) *mockDataAccessWorkspaceRepository_ListFunctions_Call {
	_c.Call.Return(_a0, _a1)
	return _c
}

func (_c *mockDataAccessWorkspaceRepository_ListFunctions_Call) RunAndReturn(run func(context.Context, string, string) ([]repo.FunctionInfo, error)) *mockDataAccessWorkspaceRepository_ListFunctions_Call {
	_c.Call.Return(run)
	return _c
}

// ListSchemas provides a mock function with given fields: ctx, catalogName
func (_m *mockDataAccessWorkspaceRepository) ListSchemas(ctx context.Context, catalogName string) ([]catalog.SchemaInfo, error) {
	ret := _m.Called(ctx, catalogName)

	if len(ret) == 0 {
		panic("no return value specified for ListSchemas")
	}

	var r0 []catalog.SchemaInfo
	var r1 error
	if rf, ok := ret.Get(0).(func(context.Context, string) ([]catalog.SchemaInfo, error)); ok {
		return rf(ctx, catalogName)
	}
	if rf, ok := ret.Get(0).(func(context.Context, string) []catalog.SchemaInfo); ok {
		r0 = rf(ctx, catalogName)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).([]catalog.SchemaInfo)
		}
	}

	if rf, ok := ret.Get(1).(func(context.Context, string) error); ok {
		r1 = rf(ctx, catalogName)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// mockDataAccessWorkspaceRepository_ListSchemas_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'ListSchemas'
type mockDataAccessWorkspaceRepository_ListSchemas_Call struct {
	*mock.Call
}

// ListSchemas is a helper method to define mock.On call
//   - ctx context.Context
//   - catalogName string
func (_e *mockDataAccessWorkspaceRepository_Expecter) ListSchemas(ctx interface{}, catalogName interface{}) *mockDataAccessWorkspaceRepository_ListSchemas_Call {
	return &mockDataAccessWorkspaceRepository_ListSchemas_Call{Call: _e.mock.On("ListSchemas", ctx, catalogName)}
}

func (_c *mockDataAccessWorkspaceRepository_ListSchemas_Call) Run(run func(ctx context.Context, catalogName string)) *mockDataAccessWorkspaceRepository_ListSchemas_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(context.Context), args[1].(string))
	})
	return _c
}

func (_c *mockDataAccessWorkspaceRepository_ListSchemas_Call) Return(_a0 []catalog.SchemaInfo, _a1 error) *mockDataAccessWorkspaceRepository_ListSchemas_Call {
	_c.Call.Return(_a0, _a1)
	return _c
}

func (_c *mockDataAccessWorkspaceRepository_ListSchemas_Call) RunAndReturn(run func(context.Context, string) ([]catalog.SchemaInfo, error)) *mockDataAccessWorkspaceRepository_ListSchemas_Call {
	_c.Call.Return(run)
	return _c
}

// ListTables provides a mock function with given fields: ctx, catalogName, schemaName
func (_m *mockDataAccessWorkspaceRepository) ListTables(ctx context.Context, catalogName string, schemaName string) ([]catalog.TableInfo, error) {
	ret := _m.Called(ctx, catalogName, schemaName)

	if len(ret) == 0 {
		panic("no return value specified for ListTables")
	}

	var r0 []catalog.TableInfo
	var r1 error
	if rf, ok := ret.Get(0).(func(context.Context, string, string) ([]catalog.TableInfo, error)); ok {
		return rf(ctx, catalogName, schemaName)
	}
	if rf, ok := ret.Get(0).(func(context.Context, string, string) []catalog.TableInfo); ok {
		r0 = rf(ctx, catalogName, schemaName)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).([]catalog.TableInfo)
		}
	}

	if rf, ok := ret.Get(1).(func(context.Context, string, string) error); ok {
		r1 = rf(ctx, catalogName, schemaName)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// mockDataAccessWorkspaceRepository_ListTables_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'ListTables'
type mockDataAccessWorkspaceRepository_ListTables_Call struct {
	*mock.Call
}

// ListTables is a helper method to define mock.On call
//   - ctx context.Context
//   - catalogName string
//   - schemaName string
func (_e *mockDataAccessWorkspaceRepository_Expecter) ListTables(ctx interface{}, catalogName interface{}, schemaName interface{}) *mockDataAccessWorkspaceRepository_ListTables_Call {
	return &mockDataAccessWorkspaceRepository_ListTables_Call{Call: _e.mock.On("ListTables", ctx, catalogName, schemaName)}
}

func (_c *mockDataAccessWorkspaceRepository_ListTables_Call) Run(run func(ctx context.Context, catalogName string, schemaName string)) *mockDataAccessWorkspaceRepository_ListTables_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(context.Context), args[1].(string), args[2].(string))
	})
	return _c
}

func (_c *mockDataAccessWorkspaceRepository_ListTables_Call) Return(_a0 []catalog.TableInfo, _a1 error) *mockDataAccessWorkspaceRepository_ListTables_Call {
	_c.Call.Return(_a0, _a1)
	return _c
}

func (_c *mockDataAccessWorkspaceRepository_ListTables_Call) RunAndReturn(run func(context.Context, string, string) ([]catalog.TableInfo, error)) *mockDataAccessWorkspaceRepository_ListTables_Call {
	_c.Call.Return(run)
	return _c
}

// Ping provides a mock function with given fields: ctx
func (_m *mockDataAccessWorkspaceRepository) Ping(ctx context.Context) error {
	ret := _m.Called(ctx)

	if len(ret) == 0 {
		panic("no return value specified for Ping")
	}

	var r0 error
	if rf, ok := ret.Get(0).(func(context.Context) error); ok {
		r0 = rf(ctx)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// mockDataAccessWorkspaceRepository_Ping_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'Ping'
type mockDataAccessWorkspaceRepository_Ping_Call struct {
	*mock.Call
}

// Ping is a helper method to define mock.On call
//   - ctx context.Context
func (_e *mockDataAccessWorkspaceRepository_Expecter) Ping(ctx interface{}) *mockDataAccessWorkspaceRepository_Ping_Call {
	return &mockDataAccessWorkspaceRepository_Ping_Call{Call: _e.mock.On("Ping", ctx)}
}

func (_c *mockDataAccessWorkspaceRepository_Ping_Call) Run(run func(ctx context.Context)) *mockDataAccessWorkspaceRepository_Ping_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(context.Context))
	})
	return _c
}

func (_c *mockDataAccessWorkspaceRepository_Ping_Call) Return(_a0 error) *mockDataAccessWorkspaceRepository_Ping_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *mockDataAccessWorkspaceRepository_Ping_Call) RunAndReturn(run func(context.Context) error) *mockDataAccessWorkspaceRepository_Ping_Call {
	_c.Call.Return(run)
	return _c
}

// SetPermissionsOnResource provides a mock function with given fields: ctx, securableType, fullName, changes
func (_m *mockDataAccessWorkspaceRepository) SetPermissionsOnResource(ctx context.Context, securableType catalog.SecurableType, fullName string, changes ...catalog.PermissionsChange) error {
	_va := make([]interface{}, len(changes))
	for _i := range changes {
		_va[_i] = changes[_i]
	}
	var _ca []interface{}
	_ca = append(_ca, ctx, securableType, fullName)
	_ca = append(_ca, _va...)
	ret := _m.Called(_ca...)

	if len(ret) == 0 {
		panic("no return value specified for SetPermissionsOnResource")
	}

	var r0 error
	if rf, ok := ret.Get(0).(func(context.Context, catalog.SecurableType, string, ...catalog.PermissionsChange) error); ok {
		r0 = rf(ctx, securableType, fullName, changes...)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// mockDataAccessWorkspaceRepository_SetPermissionsOnResource_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'SetPermissionsOnResource'
type mockDataAccessWorkspaceRepository_SetPermissionsOnResource_Call struct {
	*mock.Call
}

// SetPermissionsOnResource is a helper method to define mock.On call
//   - ctx context.Context
//   - securableType catalog.SecurableType
//   - fullName string
//   - changes ...catalog.PermissionsChange
func (_e *mockDataAccessWorkspaceRepository_Expecter) SetPermissionsOnResource(ctx interface{}, securableType interface{}, fullName interface{}, changes ...interface{}) *mockDataAccessWorkspaceRepository_SetPermissionsOnResource_Call {
	return &mockDataAccessWorkspaceRepository_SetPermissionsOnResource_Call{Call: _e.mock.On("SetPermissionsOnResource",
		append([]interface{}{ctx, securableType, fullName}, changes...)...)}
}

func (_c *mockDataAccessWorkspaceRepository_SetPermissionsOnResource_Call) Run(run func(ctx context.Context, securableType catalog.SecurableType, fullName string, changes ...catalog.PermissionsChange)) *mockDataAccessWorkspaceRepository_SetPermissionsOnResource_Call {
	_c.Call.Run(func(args mock.Arguments) {
		variadicArgs := make([]catalog.PermissionsChange, len(args)-3)
		for i, a := range args[3:] {
			if a != nil {
				variadicArgs[i] = a.(catalog.PermissionsChange)
			}
		}
		run(args[0].(context.Context), args[1].(catalog.SecurableType), args[2].(string), variadicArgs...)
	})
	return _c
}

func (_c *mockDataAccessWorkspaceRepository_SetPermissionsOnResource_Call) Return(_a0 error) *mockDataAccessWorkspaceRepository_SetPermissionsOnResource_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *mockDataAccessWorkspaceRepository_SetPermissionsOnResource_Call) RunAndReturn(run func(context.Context, catalog.SecurableType, string, ...catalog.PermissionsChange) error) *mockDataAccessWorkspaceRepository_SetPermissionsOnResource_Call {
	_c.Call.Return(run)
	return _c
}

// SqlWarehouseRepository provides a mock function with given fields: warehouseId
func (_m *mockDataAccessWorkspaceRepository) SqlWarehouseRepository(warehouseId string) repo.WarehouseRepository {
	ret := _m.Called(warehouseId)

	if len(ret) == 0 {
		panic("no return value specified for SqlWarehouseRepository")
	}

	var r0 repo.WarehouseRepository
	if rf, ok := ret.Get(0).(func(string) repo.WarehouseRepository); ok {
		r0 = rf(warehouseId)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(repo.WarehouseRepository)
		}
	}

	return r0
}

// mockDataAccessWorkspaceRepository_SqlWarehouseRepository_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'SqlWarehouseRepository'
type mockDataAccessWorkspaceRepository_SqlWarehouseRepository_Call struct {
	*mock.Call
}

// SqlWarehouseRepository is a helper method to define mock.On call
//   - warehouseId string
func (_e *mockDataAccessWorkspaceRepository_Expecter) SqlWarehouseRepository(warehouseId interface{}) *mockDataAccessWorkspaceRepository_SqlWarehouseRepository_Call {
	return &mockDataAccessWorkspaceRepository_SqlWarehouseRepository_Call{Call: _e.mock.On("SqlWarehouseRepository", warehouseId)}
}

func (_c *mockDataAccessWorkspaceRepository_SqlWarehouseRepository_Call) Run(run func(warehouseId string)) *mockDataAccessWorkspaceRepository_SqlWarehouseRepository_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(string))
	})
	return _c
}

func (_c *mockDataAccessWorkspaceRepository_SqlWarehouseRepository_Call) Return(_a0 repo.WarehouseRepository) *mockDataAccessWorkspaceRepository_SqlWarehouseRepository_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *mockDataAccessWorkspaceRepository_SqlWarehouseRepository_Call) RunAndReturn(run func(string) repo.WarehouseRepository) *mockDataAccessWorkspaceRepository_SqlWarehouseRepository_Call {
	_c.Call.Return(run)
	return _c
}

// newMockDataAccessWorkspaceRepository creates a new instance of mockDataAccessWorkspaceRepository. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
// The first argument is typically a *testing.T value.
func newMockDataAccessWorkspaceRepository(t interface {
	mock.TestingT
	Cleanup(func())
}) *mockDataAccessWorkspaceRepository {
	mock := &mockDataAccessWorkspaceRepository{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}
