package masks

/////////////////
// DEFAULT MASK//
/////////////////

func DefaultMask() MaskGenerator {
	return NewSimpleMaskGenerator(&defaultMaskMethod{})
}

type defaultMaskMethod struct{}

func (m *defaultMaskMethod) MaskMethod(_ string, columnType SqlDataType) string {
	switch columnType {
	case DataTypeBigInt, DataTypeDecimal, DataTypeDouble, DataTypeFloat, DataTypeInt, DataTypeSmallInt, DataTypeTinyInt, DataTypeInterval:
		return "0"
	case DataTypeDate, DataTypeTimestamp, DataTypeTimestamp_NTZ:
		return "'0000'"
	case DataTypeBinary:
		return "X''"
	case DataTypeBoolean:
		return "false"
	case DataTypeString:
		return "'*****'"
	case DataTypeArray:
		return "ARRAY()"
	case DataTypeMap:
		return "map()"
	case DataTypeStruct, DataTypeVoid:
		return "NULL"
	}

	return "NULL"
}

func (m *defaultMaskMethod) SupportedType(_ SqlDataType) bool {
	return true
}
