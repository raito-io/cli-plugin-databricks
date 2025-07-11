// Code generated by mockery v2.53.3. DO NOT EDIT.

package databricks

import (
	context "context"

	catalog "github.com/databricks/databricks-sdk-go/service/catalog"

	iam "github.com/databricks/databricks-sdk-go/service/iam"

	mock "github.com/stretchr/testify/mock"

	repo "cli-plugin-databricks/databricks/repo"
)

// mockDataSourceWorkspaceRepository is an autogenerated mock type for the dataSourceWorkspaceRepository type
type mockDataSourceWorkspaceRepository struct {
	mock.Mock
}

type mockDataSourceWorkspaceRepository_Expecter struct {
	mock *mock.Mock
}

func (_m *mockDataSourceWorkspaceRepository) EXPECT() *mockDataSourceWorkspaceRepository_Expecter {
	return &mockDataSourceWorkspaceRepository_Expecter{mock: &_m.Mock}
}

// ListAllTables provides a mock function with given fields: ctx, catalogName, schemaName
func (_m *mockDataSourceWorkspaceRepository) ListAllTables(ctx context.Context, catalogName string, schemaName string) ([]catalog.TableInfo, error) {
	ret := _m.Called(ctx, catalogName, schemaName)

	if len(ret) == 0 {
		panic("no return value specified for ListAllTables")
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

// mockDataSourceWorkspaceRepository_ListAllTables_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'ListAllTables'
type mockDataSourceWorkspaceRepository_ListAllTables_Call struct {
	*mock.Call
}

// ListAllTables is a helper method to define mock.On call
//   - ctx context.Context
//   - catalogName string
//   - schemaName string
func (_e *mockDataSourceWorkspaceRepository_Expecter) ListAllTables(ctx interface{}, catalogName interface{}, schemaName interface{}) *mockDataSourceWorkspaceRepository_ListAllTables_Call {
	return &mockDataSourceWorkspaceRepository_ListAllTables_Call{Call: _e.mock.On("ListAllTables", ctx, catalogName, schemaName)}
}

func (_c *mockDataSourceWorkspaceRepository_ListAllTables_Call) Run(run func(ctx context.Context, catalogName string, schemaName string)) *mockDataSourceWorkspaceRepository_ListAllTables_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(context.Context), args[1].(string), args[2].(string))
	})
	return _c
}

func (_c *mockDataSourceWorkspaceRepository_ListAllTables_Call) Return(_a0 []catalog.TableInfo, _a1 error) *mockDataSourceWorkspaceRepository_ListAllTables_Call {
	_c.Call.Return(_a0, _a1)
	return _c
}

func (_c *mockDataSourceWorkspaceRepository_ListAllTables_Call) RunAndReturn(run func(context.Context, string, string) ([]catalog.TableInfo, error)) *mockDataSourceWorkspaceRepository_ListAllTables_Call {
	_c.Call.Return(run)
	return _c
}

// ListCatalogs provides a mock function with given fields: ctx
func (_m *mockDataSourceWorkspaceRepository) ListCatalogs(ctx context.Context) <-chan repo.ChannelItem[catalog.CatalogInfo] {
	ret := _m.Called(ctx)

	if len(ret) == 0 {
		panic("no return value specified for ListCatalogs")
	}

	var r0 <-chan repo.ChannelItem[catalog.CatalogInfo]
	if rf, ok := ret.Get(0).(func(context.Context) <-chan repo.ChannelItem[catalog.CatalogInfo]); ok {
		r0 = rf(ctx)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(<-chan repo.ChannelItem[catalog.CatalogInfo])
		}
	}

	return r0
}

// mockDataSourceWorkspaceRepository_ListCatalogs_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'ListCatalogs'
type mockDataSourceWorkspaceRepository_ListCatalogs_Call struct {
	*mock.Call
}

// ListCatalogs is a helper method to define mock.On call
//   - ctx context.Context
func (_e *mockDataSourceWorkspaceRepository_Expecter) ListCatalogs(ctx interface{}) *mockDataSourceWorkspaceRepository_ListCatalogs_Call {
	return &mockDataSourceWorkspaceRepository_ListCatalogs_Call{Call: _e.mock.On("ListCatalogs", ctx)}
}

func (_c *mockDataSourceWorkspaceRepository_ListCatalogs_Call) Run(run func(ctx context.Context)) *mockDataSourceWorkspaceRepository_ListCatalogs_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(context.Context))
	})
	return _c
}

func (_c *mockDataSourceWorkspaceRepository_ListCatalogs_Call) Return(_a0 <-chan repo.ChannelItem[catalog.CatalogInfo]) *mockDataSourceWorkspaceRepository_ListCatalogs_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *mockDataSourceWorkspaceRepository_ListCatalogs_Call) RunAndReturn(run func(context.Context) <-chan repo.ChannelItem[catalog.CatalogInfo]) *mockDataSourceWorkspaceRepository_ListCatalogs_Call {
	_c.Call.Return(run)
	return _c
}

// ListFunctions provides a mock function with given fields: ctx, catalogName, schemaName
func (_m *mockDataSourceWorkspaceRepository) ListFunctions(ctx context.Context, catalogName string, schemaName string) <-chan repo.ChannelItem[catalog.FunctionInfo] {
	ret := _m.Called(ctx, catalogName, schemaName)

	if len(ret) == 0 {
		panic("no return value specified for ListFunctions")
	}

	var r0 <-chan repo.ChannelItem[catalog.FunctionInfo]
	if rf, ok := ret.Get(0).(func(context.Context, string, string) <-chan repo.ChannelItem[catalog.FunctionInfo]); ok {
		r0 = rf(ctx, catalogName, schemaName)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(<-chan repo.ChannelItem[catalog.FunctionInfo])
		}
	}

	return r0
}

// mockDataSourceWorkspaceRepository_ListFunctions_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'ListFunctions'
type mockDataSourceWorkspaceRepository_ListFunctions_Call struct {
	*mock.Call
}

// ListFunctions is a helper method to define mock.On call
//   - ctx context.Context
//   - catalogName string
//   - schemaName string
func (_e *mockDataSourceWorkspaceRepository_Expecter) ListFunctions(ctx interface{}, catalogName interface{}, schemaName interface{}) *mockDataSourceWorkspaceRepository_ListFunctions_Call {
	return &mockDataSourceWorkspaceRepository_ListFunctions_Call{Call: _e.mock.On("ListFunctions", ctx, catalogName, schemaName)}
}

func (_c *mockDataSourceWorkspaceRepository_ListFunctions_Call) Run(run func(ctx context.Context, catalogName string, schemaName string)) *mockDataSourceWorkspaceRepository_ListFunctions_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(context.Context), args[1].(string), args[2].(string))
	})
	return _c
}

func (_c *mockDataSourceWorkspaceRepository_ListFunctions_Call) Return(_a0 <-chan repo.ChannelItem[catalog.FunctionInfo]) *mockDataSourceWorkspaceRepository_ListFunctions_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *mockDataSourceWorkspaceRepository_ListFunctions_Call) RunAndReturn(run func(context.Context, string, string) <-chan repo.ChannelItem[catalog.FunctionInfo]) *mockDataSourceWorkspaceRepository_ListFunctions_Call {
	_c.Call.Return(run)
	return _c
}

// ListSchemas provides a mock function with given fields: ctx, catalogName
func (_m *mockDataSourceWorkspaceRepository) ListSchemas(ctx context.Context, catalogName string) <-chan repo.ChannelItem[catalog.SchemaInfo] {
	ret := _m.Called(ctx, catalogName)

	if len(ret) == 0 {
		panic("no return value specified for ListSchemas")
	}

	var r0 <-chan repo.ChannelItem[catalog.SchemaInfo]
	if rf, ok := ret.Get(0).(func(context.Context, string) <-chan repo.ChannelItem[catalog.SchemaInfo]); ok {
		r0 = rf(ctx, catalogName)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(<-chan repo.ChannelItem[catalog.SchemaInfo])
		}
	}

	return r0
}

// mockDataSourceWorkspaceRepository_ListSchemas_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'ListSchemas'
type mockDataSourceWorkspaceRepository_ListSchemas_Call struct {
	*mock.Call
}

// ListSchemas is a helper method to define mock.On call
//   - ctx context.Context
//   - catalogName string
func (_e *mockDataSourceWorkspaceRepository_Expecter) ListSchemas(ctx interface{}, catalogName interface{}) *mockDataSourceWorkspaceRepository_ListSchemas_Call {
	return &mockDataSourceWorkspaceRepository_ListSchemas_Call{Call: _e.mock.On("ListSchemas", ctx, catalogName)}
}

func (_c *mockDataSourceWorkspaceRepository_ListSchemas_Call) Run(run func(ctx context.Context, catalogName string)) *mockDataSourceWorkspaceRepository_ListSchemas_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(context.Context), args[1].(string))
	})
	return _c
}

func (_c *mockDataSourceWorkspaceRepository_ListSchemas_Call) Return(_a0 <-chan repo.ChannelItem[catalog.SchemaInfo]) *mockDataSourceWorkspaceRepository_ListSchemas_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *mockDataSourceWorkspaceRepository_ListSchemas_Call) RunAndReturn(run func(context.Context, string) <-chan repo.ChannelItem[catalog.SchemaInfo]) *mockDataSourceWorkspaceRepository_ListSchemas_Call {
	_c.Call.Return(run)
	return _c
}

// Me provides a mock function with given fields: ctx
func (_m *mockDataSourceWorkspaceRepository) Me(ctx context.Context) (*iam.User, error) {
	ret := _m.Called(ctx)

	if len(ret) == 0 {
		panic("no return value specified for Me")
	}

	var r0 *iam.User
	var r1 error
	if rf, ok := ret.Get(0).(func(context.Context) (*iam.User, error)); ok {
		return rf(ctx)
	}
	if rf, ok := ret.Get(0).(func(context.Context) *iam.User); ok {
		r0 = rf(ctx)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*iam.User)
		}
	}

	if rf, ok := ret.Get(1).(func(context.Context) error); ok {
		r1 = rf(ctx)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// mockDataSourceWorkspaceRepository_Me_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'Me'
type mockDataSourceWorkspaceRepository_Me_Call struct {
	*mock.Call
}

// Me is a helper method to define mock.On call
//   - ctx context.Context
func (_e *mockDataSourceWorkspaceRepository_Expecter) Me(ctx interface{}) *mockDataSourceWorkspaceRepository_Me_Call {
	return &mockDataSourceWorkspaceRepository_Me_Call{Call: _e.mock.On("Me", ctx)}
}

func (_c *mockDataSourceWorkspaceRepository_Me_Call) Run(run func(ctx context.Context)) *mockDataSourceWorkspaceRepository_Me_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(context.Context))
	})
	return _c
}

func (_c *mockDataSourceWorkspaceRepository_Me_Call) Return(_a0 *iam.User, _a1 error) *mockDataSourceWorkspaceRepository_Me_Call {
	_c.Call.Return(_a0, _a1)
	return _c
}

func (_c *mockDataSourceWorkspaceRepository_Me_Call) RunAndReturn(run func(context.Context) (*iam.User, error)) *mockDataSourceWorkspaceRepository_Me_Call {
	_c.Call.Return(run)
	return _c
}

// Ping provides a mock function with given fields: ctx
func (_m *mockDataSourceWorkspaceRepository) Ping(ctx context.Context) error {
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

// mockDataSourceWorkspaceRepository_Ping_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'Ping'
type mockDataSourceWorkspaceRepository_Ping_Call struct {
	*mock.Call
}

// Ping is a helper method to define mock.On call
//   - ctx context.Context
func (_e *mockDataSourceWorkspaceRepository_Expecter) Ping(ctx interface{}) *mockDataSourceWorkspaceRepository_Ping_Call {
	return &mockDataSourceWorkspaceRepository_Ping_Call{Call: _e.mock.On("Ping", ctx)}
}

func (_c *mockDataSourceWorkspaceRepository_Ping_Call) Run(run func(ctx context.Context)) *mockDataSourceWorkspaceRepository_Ping_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(context.Context))
	})
	return _c
}

func (_c *mockDataSourceWorkspaceRepository_Ping_Call) Return(_a0 error) *mockDataSourceWorkspaceRepository_Ping_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *mockDataSourceWorkspaceRepository_Ping_Call) RunAndReturn(run func(context.Context) error) *mockDataSourceWorkspaceRepository_Ping_Call {
	_c.Call.Return(run)
	return _c
}

// SetPermissionsOnResource provides a mock function with given fields: ctx, securableType, fullName, changes
func (_m *mockDataSourceWorkspaceRepository) SetPermissionsOnResource(ctx context.Context, securableType catalog.SecurableType, fullName string, changes ...catalog.PermissionsChange) error {
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

// mockDataSourceWorkspaceRepository_SetPermissionsOnResource_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'SetPermissionsOnResource'
type mockDataSourceWorkspaceRepository_SetPermissionsOnResource_Call struct {
	*mock.Call
}

// SetPermissionsOnResource is a helper method to define mock.On call
//   - ctx context.Context
//   - securableType catalog.SecurableType
//   - fullName string
//   - changes ...catalog.PermissionsChange
func (_e *mockDataSourceWorkspaceRepository_Expecter) SetPermissionsOnResource(ctx interface{}, securableType interface{}, fullName interface{}, changes ...interface{}) *mockDataSourceWorkspaceRepository_SetPermissionsOnResource_Call {
	return &mockDataSourceWorkspaceRepository_SetPermissionsOnResource_Call{Call: _e.mock.On("SetPermissionsOnResource",
		append([]interface{}{ctx, securableType, fullName}, changes...)...)}
}

func (_c *mockDataSourceWorkspaceRepository_SetPermissionsOnResource_Call) Run(run func(ctx context.Context, securableType catalog.SecurableType, fullName string, changes ...catalog.PermissionsChange)) *mockDataSourceWorkspaceRepository_SetPermissionsOnResource_Call {
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

func (_c *mockDataSourceWorkspaceRepository_SetPermissionsOnResource_Call) Return(_a0 error) *mockDataSourceWorkspaceRepository_SetPermissionsOnResource_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *mockDataSourceWorkspaceRepository_SetPermissionsOnResource_Call) RunAndReturn(run func(context.Context, catalog.SecurableType, string, ...catalog.PermissionsChange) error) *mockDataSourceWorkspaceRepository_SetPermissionsOnResource_Call {
	_c.Call.Return(run)
	return _c
}

// SqlWarehouseRepository provides a mock function with given fields: warehouseId
func (_m *mockDataSourceWorkspaceRepository) SqlWarehouseRepository(warehouseId string) repo.WarehouseRepository {
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

// mockDataSourceWorkspaceRepository_SqlWarehouseRepository_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'SqlWarehouseRepository'
type mockDataSourceWorkspaceRepository_SqlWarehouseRepository_Call struct {
	*mock.Call
}

// SqlWarehouseRepository is a helper method to define mock.On call
//   - warehouseId string
func (_e *mockDataSourceWorkspaceRepository_Expecter) SqlWarehouseRepository(warehouseId interface{}) *mockDataSourceWorkspaceRepository_SqlWarehouseRepository_Call {
	return &mockDataSourceWorkspaceRepository_SqlWarehouseRepository_Call{Call: _e.mock.On("SqlWarehouseRepository", warehouseId)}
}

func (_c *mockDataSourceWorkspaceRepository_SqlWarehouseRepository_Call) Run(run func(warehouseId string)) *mockDataSourceWorkspaceRepository_SqlWarehouseRepository_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(string))
	})
	return _c
}

func (_c *mockDataSourceWorkspaceRepository_SqlWarehouseRepository_Call) Return(_a0 repo.WarehouseRepository) *mockDataSourceWorkspaceRepository_SqlWarehouseRepository_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *mockDataSourceWorkspaceRepository_SqlWarehouseRepository_Call) RunAndReturn(run func(string) repo.WarehouseRepository) *mockDataSourceWorkspaceRepository_SqlWarehouseRepository_Call {
	_c.Call.Return(run)
	return _c
}

// newMockDataSourceWorkspaceRepository creates a new instance of mockDataSourceWorkspaceRepository. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
// The first argument is typically a *testing.T value.
func newMockDataSourceWorkspaceRepository(t interface {
	mock.TestingT
	Cleanup(func())
}) *mockDataSourceWorkspaceRepository {
	mock := &mockDataSourceWorkspaceRepository{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}
