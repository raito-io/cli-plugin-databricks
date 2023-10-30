// Code generated by mockery v2.36.0. DO NOT EDIT.

package repo

import (
	context "context"

	sql "github.com/databricks/databricks-sdk-go/service/sql"
	mock "github.com/stretchr/testify/mock"
)

// MockWarehouseRepository is an autogenerated mock type for the WarehouseRepository type
type MockWarehouseRepository struct {
	mock.Mock
}

type MockWarehouseRepository_Expecter struct {
	mock *mock.Mock
}

func (_m *MockWarehouseRepository) EXPECT() *MockWarehouseRepository_Expecter {
	return &MockWarehouseRepository_Expecter{mock: &_m.Mock}
}

// DropFunction provides a mock function with given fields: ctx, catalog, schema, functionName
func (_m *MockWarehouseRepository) DropFunction(ctx context.Context, catalog string, schema string, functionName string) error {
	ret := _m.Called(ctx, catalog, schema, functionName)

	var r0 error
	if rf, ok := ret.Get(0).(func(context.Context, string, string, string) error); ok {
		r0 = rf(ctx, catalog, schema, functionName)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// MockWarehouseRepository_DropFunction_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'DropFunction'
type MockWarehouseRepository_DropFunction_Call struct {
	*mock.Call
}

// DropFunction is a helper method to define mock.On call
//   - ctx context.Context
//   - catalog string
//   - schema string
//   - functionName string
func (_e *MockWarehouseRepository_Expecter) DropFunction(ctx interface{}, catalog interface{}, schema interface{}, functionName interface{}) *MockWarehouseRepository_DropFunction_Call {
	return &MockWarehouseRepository_DropFunction_Call{Call: _e.mock.On("DropFunction", ctx, catalog, schema, functionName)}
}

func (_c *MockWarehouseRepository_DropFunction_Call) Run(run func(ctx context.Context, catalog string, schema string, functionName string)) *MockWarehouseRepository_DropFunction_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(context.Context), args[1].(string), args[2].(string), args[3].(string))
	})
	return _c
}

func (_c *MockWarehouseRepository_DropFunction_Call) Return(_a0 error) *MockWarehouseRepository_DropFunction_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *MockWarehouseRepository_DropFunction_Call) RunAndReturn(run func(context.Context, string, string, string) error) *MockWarehouseRepository_DropFunction_Call {
	_c.Call.Return(run)
	return _c
}

// DropMask provides a mock function with given fields: ctx, catalog, schema, table, column
func (_m *MockWarehouseRepository) DropMask(ctx context.Context, catalog string, schema string, table string, column string) error {
	ret := _m.Called(ctx, catalog, schema, table, column)

	var r0 error
	if rf, ok := ret.Get(0).(func(context.Context, string, string, string, string) error); ok {
		r0 = rf(ctx, catalog, schema, table, column)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// MockWarehouseRepository_DropMask_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'DropMask'
type MockWarehouseRepository_DropMask_Call struct {
	*mock.Call
}

// DropMask is a helper method to define mock.On call
//   - ctx context.Context
//   - catalog string
//   - schema string
//   - table string
//   - column string
func (_e *MockWarehouseRepository_Expecter) DropMask(ctx interface{}, catalog interface{}, schema interface{}, table interface{}, column interface{}) *MockWarehouseRepository_DropMask_Call {
	return &MockWarehouseRepository_DropMask_Call{Call: _e.mock.On("DropMask", ctx, catalog, schema, table, column)}
}

func (_c *MockWarehouseRepository_DropMask_Call) Run(run func(ctx context.Context, catalog string, schema string, table string, column string)) *MockWarehouseRepository_DropMask_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(context.Context), args[1].(string), args[2].(string), args[3].(string), args[4].(string))
	})
	return _c
}

func (_c *MockWarehouseRepository_DropMask_Call) Return(_a0 error) *MockWarehouseRepository_DropMask_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *MockWarehouseRepository_DropMask_Call) RunAndReturn(run func(context.Context, string, string, string, string) error) *MockWarehouseRepository_DropMask_Call {
	_c.Call.Return(run)
	return _c
}

// ExecuteStatement provides a mock function with given fields: ctx, catalog, schema, statement, parameters
func (_m *MockWarehouseRepository) ExecuteStatement(ctx context.Context, catalog string, schema string, statement string, parameters ...sql.StatementParameterListItem) (*sql.ExecuteStatementResponse, error) {
	_va := make([]interface{}, len(parameters))
	for _i := range parameters {
		_va[_i] = parameters[_i]
	}
	var _ca []interface{}
	_ca = append(_ca, ctx, catalog, schema, statement)
	_ca = append(_ca, _va...)
	ret := _m.Called(_ca...)

	var r0 *sql.ExecuteStatementResponse
	var r1 error
	if rf, ok := ret.Get(0).(func(context.Context, string, string, string, ...sql.StatementParameterListItem) (*sql.ExecuteStatementResponse, error)); ok {
		return rf(ctx, catalog, schema, statement, parameters...)
	}
	if rf, ok := ret.Get(0).(func(context.Context, string, string, string, ...sql.StatementParameterListItem) *sql.ExecuteStatementResponse); ok {
		r0 = rf(ctx, catalog, schema, statement, parameters...)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*sql.ExecuteStatementResponse)
		}
	}

	if rf, ok := ret.Get(1).(func(context.Context, string, string, string, ...sql.StatementParameterListItem) error); ok {
		r1 = rf(ctx, catalog, schema, statement, parameters...)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// MockWarehouseRepository_ExecuteStatement_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'ExecuteStatement'
type MockWarehouseRepository_ExecuteStatement_Call struct {
	*mock.Call
}

// ExecuteStatement is a helper method to define mock.On call
//   - ctx context.Context
//   - catalog string
//   - schema string
//   - statement string
//   - parameters ...sql.StatementParameterListItem
func (_e *MockWarehouseRepository_Expecter) ExecuteStatement(ctx interface{}, catalog interface{}, schema interface{}, statement interface{}, parameters ...interface{}) *MockWarehouseRepository_ExecuteStatement_Call {
	return &MockWarehouseRepository_ExecuteStatement_Call{Call: _e.mock.On("ExecuteStatement",
		append([]interface{}{ctx, catalog, schema, statement}, parameters...)...)}
}

func (_c *MockWarehouseRepository_ExecuteStatement_Call) Run(run func(ctx context.Context, catalog string, schema string, statement string, parameters ...sql.StatementParameterListItem)) *MockWarehouseRepository_ExecuteStatement_Call {
	_c.Call.Run(func(args mock.Arguments) {
		variadicArgs := make([]sql.StatementParameterListItem, len(args)-4)
		for i, a := range args[4:] {
			if a != nil {
				variadicArgs[i] = a.(sql.StatementParameterListItem)
			}
		}
		run(args[0].(context.Context), args[1].(string), args[2].(string), args[3].(string), variadicArgs...)
	})
	return _c
}

func (_c *MockWarehouseRepository_ExecuteStatement_Call) Return(_a0 *sql.ExecuteStatementResponse, _a1 error) *MockWarehouseRepository_ExecuteStatement_Call {
	_c.Call.Return(_a0, _a1)
	return _c
}

func (_c *MockWarehouseRepository_ExecuteStatement_Call) RunAndReturn(run func(context.Context, string, string, string, ...sql.StatementParameterListItem) (*sql.ExecuteStatementResponse, error)) *MockWarehouseRepository_ExecuteStatement_Call {
	_c.Call.Return(run)
	return _c
}

// GetTableInformation provides a mock function with given fields: ctx, catalog, schema, tableName
func (_m *MockWarehouseRepository) GetTableInformation(ctx context.Context, catalog string, schema string, tableName string) (map[string]*ColumnInformation, error) {
	ret := _m.Called(ctx, catalog, schema, tableName)

	var r0 map[string]*ColumnInformation
	var r1 error
	if rf, ok := ret.Get(0).(func(context.Context, string, string, string) (map[string]*ColumnInformation, error)); ok {
		return rf(ctx, catalog, schema, tableName)
	}
	if rf, ok := ret.Get(0).(func(context.Context, string, string, string) map[string]*ColumnInformation); ok {
		r0 = rf(ctx, catalog, schema, tableName)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(map[string]*ColumnInformation)
		}
	}

	if rf, ok := ret.Get(1).(func(context.Context, string, string, string) error); ok {
		r1 = rf(ctx, catalog, schema, tableName)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// MockWarehouseRepository_GetTableInformation_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'GetTableInformation'
type MockWarehouseRepository_GetTableInformation_Call struct {
	*mock.Call
}

// GetTableInformation is a helper method to define mock.On call
//   - ctx context.Context
//   - catalog string
//   - schema string
//   - tableName string
func (_e *MockWarehouseRepository_Expecter) GetTableInformation(ctx interface{}, catalog interface{}, schema interface{}, tableName interface{}) *MockWarehouseRepository_GetTableInformation_Call {
	return &MockWarehouseRepository_GetTableInformation_Call{Call: _e.mock.On("GetTableInformation", ctx, catalog, schema, tableName)}
}

func (_c *MockWarehouseRepository_GetTableInformation_Call) Run(run func(ctx context.Context, catalog string, schema string, tableName string)) *MockWarehouseRepository_GetTableInformation_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(context.Context), args[1].(string), args[2].(string), args[3].(string))
	})
	return _c
}

func (_c *MockWarehouseRepository_GetTableInformation_Call) Return(_a0 map[string]*ColumnInformation, _a1 error) *MockWarehouseRepository_GetTableInformation_Call {
	_c.Call.Return(_a0, _a1)
	return _c
}

func (_c *MockWarehouseRepository_GetTableInformation_Call) RunAndReturn(run func(context.Context, string, string, string) (map[string]*ColumnInformation, error)) *MockWarehouseRepository_GetTableInformation_Call {
	_c.Call.Return(run)
	return _c
}

// SetMask provides a mock function with given fields: ctx, catalog, schema, table, column, function
func (_m *MockWarehouseRepository) SetMask(ctx context.Context, catalog string, schema string, table string, column string, function string) error {
	ret := _m.Called(ctx, catalog, schema, table, column, function)

	var r0 error
	if rf, ok := ret.Get(0).(func(context.Context, string, string, string, string, string) error); ok {
		r0 = rf(ctx, catalog, schema, table, column, function)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// MockWarehouseRepository_SetMask_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'SetMask'
type MockWarehouseRepository_SetMask_Call struct {
	*mock.Call
}

// SetMask is a helper method to define mock.On call
//   - ctx context.Context
//   - catalog string
//   - schema string
//   - table string
//   - column string
//   - function string
func (_e *MockWarehouseRepository_Expecter) SetMask(ctx interface{}, catalog interface{}, schema interface{}, table interface{}, column interface{}, function interface{}) *MockWarehouseRepository_SetMask_Call {
	return &MockWarehouseRepository_SetMask_Call{Call: _e.mock.On("SetMask", ctx, catalog, schema, table, column, function)}
}

func (_c *MockWarehouseRepository_SetMask_Call) Run(run func(ctx context.Context, catalog string, schema string, table string, column string, function string)) *MockWarehouseRepository_SetMask_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(context.Context), args[1].(string), args[2].(string), args[3].(string), args[4].(string), args[5].(string))
	})
	return _c
}

func (_c *MockWarehouseRepository_SetMask_Call) Return(_a0 error) *MockWarehouseRepository_SetMask_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *MockWarehouseRepository_SetMask_Call) RunAndReturn(run func(context.Context, string, string, string, string, string) error) *MockWarehouseRepository_SetMask_Call {
	_c.Call.Return(run)
	return _c
}

// NewMockWarehouseRepository creates a new instance of MockWarehouseRepository. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
// The first argument is typically a *testing.T value.
func NewMockWarehouseRepository(t interface {
	mock.TestingT
	Cleanup(func())
}) *MockWarehouseRepository {
	mock := &MockWarehouseRepository{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}
