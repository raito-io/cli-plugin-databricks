// Code generated by mockery v2.33.0. DO NOT EDIT.

package databricks

import (
	context "context"

	catalog "github.com/databricks/databricks-sdk-go/service/catalog"

	mock "github.com/stretchr/testify/mock"

	sql "github.com/databricks/databricks-sdk-go/service/sql"

	time "time"
)

// mockDataUsageWorkspaceRepository is an autogenerated mock type for the dataUsageWorkspaceRepository type
type mockDataUsageWorkspaceRepository struct {
	mock.Mock
}

type mockDataUsageWorkspaceRepository_Expecter struct {
	mock *mock.Mock
}

func (_m *mockDataUsageWorkspaceRepository) EXPECT() *mockDataUsageWorkspaceRepository_Expecter {
	return &mockDataUsageWorkspaceRepository_Expecter{mock: &_m.Mock}
}

// ListCatalogs provides a mock function with given fields: ctx
func (_m *mockDataUsageWorkspaceRepository) ListCatalogs(ctx context.Context) ([]catalog.CatalogInfo, error) {
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

// mockDataUsageWorkspaceRepository_ListCatalogs_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'ListCatalogs'
type mockDataUsageWorkspaceRepository_ListCatalogs_Call struct {
	*mock.Call
}

// ListCatalogs is a helper method to define mock.On call
//   - ctx context.Context
func (_e *mockDataUsageWorkspaceRepository_Expecter) ListCatalogs(ctx interface{}) *mockDataUsageWorkspaceRepository_ListCatalogs_Call {
	return &mockDataUsageWorkspaceRepository_ListCatalogs_Call{Call: _e.mock.On("ListCatalogs", ctx)}
}

func (_c *mockDataUsageWorkspaceRepository_ListCatalogs_Call) Run(run func(ctx context.Context)) *mockDataUsageWorkspaceRepository_ListCatalogs_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(context.Context))
	})
	return _c
}

func (_c *mockDataUsageWorkspaceRepository_ListCatalogs_Call) Return(_a0 []catalog.CatalogInfo, _a1 error) *mockDataUsageWorkspaceRepository_ListCatalogs_Call {
	_c.Call.Return(_a0, _a1)
	return _c
}

func (_c *mockDataUsageWorkspaceRepository_ListCatalogs_Call) RunAndReturn(run func(context.Context) ([]catalog.CatalogInfo, error)) *mockDataUsageWorkspaceRepository_ListCatalogs_Call {
	_c.Call.Return(run)
	return _c
}

// ListSchemas provides a mock function with given fields: ctx, catalogName
func (_m *mockDataUsageWorkspaceRepository) ListSchemas(ctx context.Context, catalogName string) ([]catalog.SchemaInfo, error) {
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

// mockDataUsageWorkspaceRepository_ListSchemas_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'ListSchemas'
type mockDataUsageWorkspaceRepository_ListSchemas_Call struct {
	*mock.Call
}

// ListSchemas is a helper method to define mock.On call
//   - ctx context.Context
//   - catalogName string
func (_e *mockDataUsageWorkspaceRepository_Expecter) ListSchemas(ctx interface{}, catalogName interface{}) *mockDataUsageWorkspaceRepository_ListSchemas_Call {
	return &mockDataUsageWorkspaceRepository_ListSchemas_Call{Call: _e.mock.On("ListSchemas", ctx, catalogName)}
}

func (_c *mockDataUsageWorkspaceRepository_ListSchemas_Call) Run(run func(ctx context.Context, catalogName string)) *mockDataUsageWorkspaceRepository_ListSchemas_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(context.Context), args[1].(string))
	})
	return _c
}

func (_c *mockDataUsageWorkspaceRepository_ListSchemas_Call) Return(_a0 []catalog.SchemaInfo, _a1 error) *mockDataUsageWorkspaceRepository_ListSchemas_Call {
	_c.Call.Return(_a0, _a1)
	return _c
}

func (_c *mockDataUsageWorkspaceRepository_ListSchemas_Call) RunAndReturn(run func(context.Context, string) ([]catalog.SchemaInfo, error)) *mockDataUsageWorkspaceRepository_ListSchemas_Call {
	_c.Call.Return(run)
	return _c
}

// ListTables provides a mock function with given fields: ctx, catalogName, schemaName
func (_m *mockDataUsageWorkspaceRepository) ListTables(ctx context.Context, catalogName string, schemaName string) ([]catalog.TableInfo, error) {
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

// mockDataUsageWorkspaceRepository_ListTables_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'ListTables'
type mockDataUsageWorkspaceRepository_ListTables_Call struct {
	*mock.Call
}

// ListTables is a helper method to define mock.On call
//   - ctx context.Context
//   - catalogName string
//   - schemaName string
func (_e *mockDataUsageWorkspaceRepository_Expecter) ListTables(ctx interface{}, catalogName interface{}, schemaName interface{}) *mockDataUsageWorkspaceRepository_ListTables_Call {
	return &mockDataUsageWorkspaceRepository_ListTables_Call{Call: _e.mock.On("ListTables", ctx, catalogName, schemaName)}
}

func (_c *mockDataUsageWorkspaceRepository_ListTables_Call) Run(run func(ctx context.Context, catalogName string, schemaName string)) *mockDataUsageWorkspaceRepository_ListTables_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(context.Context), args[1].(string), args[2].(string))
	})
	return _c
}

func (_c *mockDataUsageWorkspaceRepository_ListTables_Call) Return(_a0 []catalog.TableInfo, _a1 error) *mockDataUsageWorkspaceRepository_ListTables_Call {
	_c.Call.Return(_a0, _a1)
	return _c
}

func (_c *mockDataUsageWorkspaceRepository_ListTables_Call) RunAndReturn(run func(context.Context, string, string) ([]catalog.TableInfo, error)) *mockDataUsageWorkspaceRepository_ListTables_Call {
	_c.Call.Return(run)
	return _c
}

// QueryHistory provides a mock function with given fields: ctx, startTime
func (_m *mockDataUsageWorkspaceRepository) QueryHistory(ctx context.Context, startTime *time.Time) ([]sql.QueryInfo, error) {
	ret := _m.Called(ctx, startTime)

	var r0 []sql.QueryInfo
	var r1 error
	if rf, ok := ret.Get(0).(func(context.Context, *time.Time) ([]sql.QueryInfo, error)); ok {
		return rf(ctx, startTime)
	}
	if rf, ok := ret.Get(0).(func(context.Context, *time.Time) []sql.QueryInfo); ok {
		r0 = rf(ctx, startTime)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).([]sql.QueryInfo)
		}
	}

	if rf, ok := ret.Get(1).(func(context.Context, *time.Time) error); ok {
		r1 = rf(ctx, startTime)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// mockDataUsageWorkspaceRepository_QueryHistory_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'QueryHistory'
type mockDataUsageWorkspaceRepository_QueryHistory_Call struct {
	*mock.Call
}

// QueryHistory is a helper method to define mock.On call
//   - ctx context.Context
//   - startTime *time.Time
func (_e *mockDataUsageWorkspaceRepository_Expecter) QueryHistory(ctx interface{}, startTime interface{}) *mockDataUsageWorkspaceRepository_QueryHistory_Call {
	return &mockDataUsageWorkspaceRepository_QueryHistory_Call{Call: _e.mock.On("QueryHistory", ctx, startTime)}
}

func (_c *mockDataUsageWorkspaceRepository_QueryHistory_Call) Run(run func(ctx context.Context, startTime *time.Time)) *mockDataUsageWorkspaceRepository_QueryHistory_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(context.Context), args[1].(*time.Time))
	})
	return _c
}

func (_c *mockDataUsageWorkspaceRepository_QueryHistory_Call) Return(_a0 []sql.QueryInfo, _a1 error) *mockDataUsageWorkspaceRepository_QueryHistory_Call {
	_c.Call.Return(_a0, _a1)
	return _c
}

func (_c *mockDataUsageWorkspaceRepository_QueryHistory_Call) RunAndReturn(run func(context.Context, *time.Time) ([]sql.QueryInfo, error)) *mockDataUsageWorkspaceRepository_QueryHistory_Call {
	_c.Call.Return(run)
	return _c
}

// newMockDataUsageWorkspaceRepository creates a new instance of mockDataUsageWorkspaceRepository. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
// The first argument is typically a *testing.T value.
func newMockDataUsageWorkspaceRepository(t interface {
	mock.TestingT
	Cleanup(func())
}) *mockDataUsageWorkspaceRepository {
	mock := &mockDataUsageWorkspaceRepository{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}
