// Code generated by mockery v2.36.0. DO NOT EDIT.

package databricks

import (
	context "context"

	catalog "github.com/databricks/databricks-sdk-go/service/catalog"

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

// ListCatalogs provides a mock function with given fields: ctx
func (_m *mockDataSourceWorkspaceRepository) ListCatalogs(ctx context.Context) ([]catalog.CatalogInfo, error) {
	ret := _m.Called(ctx)

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

func (_c *mockDataSourceWorkspaceRepository_ListCatalogs_Call) Return(_a0 []catalog.CatalogInfo, _a1 error) *mockDataSourceWorkspaceRepository_ListCatalogs_Call {
	_c.Call.Return(_a0, _a1)
	return _c
}

func (_c *mockDataSourceWorkspaceRepository_ListCatalogs_Call) RunAndReturn(run func(context.Context) ([]catalog.CatalogInfo, error)) *mockDataSourceWorkspaceRepository_ListCatalogs_Call {
	_c.Call.Return(run)
	return _c
}

// ListFunctions provides a mock function with given fields: ctx, catalogName, schemaName
func (_m *mockDataSourceWorkspaceRepository) ListFunctions(ctx context.Context, catalogName string, schemaName string) ([]repo.FunctionInfo, error) {
	ret := _m.Called(ctx, catalogName, schemaName)

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

func (_c *mockDataSourceWorkspaceRepository_ListFunctions_Call) Return(_a0 []repo.FunctionInfo, _a1 error) *mockDataSourceWorkspaceRepository_ListFunctions_Call {
	_c.Call.Return(_a0, _a1)
	return _c
}

func (_c *mockDataSourceWorkspaceRepository_ListFunctions_Call) RunAndReturn(run func(context.Context, string, string) ([]repo.FunctionInfo, error)) *mockDataSourceWorkspaceRepository_ListFunctions_Call {
	_c.Call.Return(run)
	return _c
}

// ListSchemas provides a mock function with given fields: ctx, catalogName
func (_m *mockDataSourceWorkspaceRepository) ListSchemas(ctx context.Context, catalogName string) ([]catalog.SchemaInfo, error) {
	ret := _m.Called(ctx, catalogName)

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

func (_c *mockDataSourceWorkspaceRepository_ListSchemas_Call) Return(_a0 []catalog.SchemaInfo, _a1 error) *mockDataSourceWorkspaceRepository_ListSchemas_Call {
	_c.Call.Return(_a0, _a1)
	return _c
}

func (_c *mockDataSourceWorkspaceRepository_ListSchemas_Call) RunAndReturn(run func(context.Context, string) ([]catalog.SchemaInfo, error)) *mockDataSourceWorkspaceRepository_ListSchemas_Call {
	_c.Call.Return(run)
	return _c
}

// ListTables provides a mock function with given fields: ctx, catalogName, schemaName
func (_m *mockDataSourceWorkspaceRepository) ListTables(ctx context.Context, catalogName string, schemaName string) ([]catalog.TableInfo, error) {
	ret := _m.Called(ctx, catalogName, schemaName)

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

// mockDataSourceWorkspaceRepository_ListTables_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'ListTables'
type mockDataSourceWorkspaceRepository_ListTables_Call struct {
	*mock.Call
}

// ListTables is a helper method to define mock.On call
//   - ctx context.Context
//   - catalogName string
//   - schemaName string
func (_e *mockDataSourceWorkspaceRepository_Expecter) ListTables(ctx interface{}, catalogName interface{}, schemaName interface{}) *mockDataSourceWorkspaceRepository_ListTables_Call {
	return &mockDataSourceWorkspaceRepository_ListTables_Call{Call: _e.mock.On("ListTables", ctx, catalogName, schemaName)}
}

func (_c *mockDataSourceWorkspaceRepository_ListTables_Call) Run(run func(ctx context.Context, catalogName string, schemaName string)) *mockDataSourceWorkspaceRepository_ListTables_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(context.Context), args[1].(string), args[2].(string))
	})
	return _c
}

func (_c *mockDataSourceWorkspaceRepository_ListTables_Call) Return(_a0 []catalog.TableInfo, _a1 error) *mockDataSourceWorkspaceRepository_ListTables_Call {
	_c.Call.Return(_a0, _a1)
	return _c
}

func (_c *mockDataSourceWorkspaceRepository_ListTables_Call) RunAndReturn(run func(context.Context, string, string) ([]catalog.TableInfo, error)) *mockDataSourceWorkspaceRepository_ListTables_Call {
	_c.Call.Return(run)
	return _c
}

// Ping provides a mock function with given fields: ctx
func (_m *mockDataSourceWorkspaceRepository) Ping(ctx context.Context) error {
	ret := _m.Called(ctx)

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
