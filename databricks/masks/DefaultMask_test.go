package masks

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_defaultMaskMethod_MaskMethod(t *testing.T) {
	type args struct {
		columnType SqlDataType
	}
	tests := []struct {
		columnType    SqlDataType
		expectedValue string
	}{
		{
			columnType:    DataTypeBigInt,
			expectedValue: "0",
		},
		{
			columnType:    DataTypeDecimal,
			expectedValue: "0",
		},
		{
			columnType:    DataTypeDouble,
			expectedValue: "0",
		},
		{
			columnType:    DataTypeFloat,
			expectedValue: "0",
		},
		{
			columnType:    DataTypeInt,
			expectedValue: "0",
		},
		{
			columnType:    DataTypeSmallInt,
			expectedValue: "0",
		},
		{
			columnType:    DataTypeTinyInt,
			expectedValue: "0",
		},
		{
			columnType:    DataTypeInterval,
			expectedValue: "0",
		},
		{
			columnType:    DataTypeDate,
			expectedValue: "'0000'",
		},
		{
			columnType:    DataTypeTimestamp,
			expectedValue: "'0000'",
		},
		{
			columnType:    DataTypeTimestamp_NTZ,
			expectedValue: "'0000'",
		},
		{
			columnType:    DataTypeBinary,
			expectedValue: "X''",
		},
		{
			columnType:    DataTypeBoolean,
			expectedValue: "false",
		},
		{
			columnType:    DataTypeString,
			expectedValue: "'*****'",
		},
		{
			columnType:    DataTypeArray,
			expectedValue: "ARRAY()",
		},
		{
			columnType:    DataTypeMap,
			expectedValue: "map()",
		},
		{
			columnType:    DataTypeStruct,
			expectedValue: "NULL",
		},
		{
			columnType:    DataTypeVoid,
			expectedValue: "NULL",
		},
	}
	for _, tt := range tests {
		t.Run(tt.columnType.String(), func(t *testing.T) {
			m := &defaultMaskMethod{}
			actualValue := m.MaskMethod("", tt.columnType)
			assert.Equal(t, tt.expectedValue, actualValue)
		})
	}
}

func Test_defaultMaskMethod_SupportedType(t *testing.T) {
	m := &defaultMaskMethod{}
	assert.True(t, m.SupportedType(DataTypeBigInt))
}
