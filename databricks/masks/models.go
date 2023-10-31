package masks

//go:generate go run github.com/raito-io/enumer -type=SqlDataType -trimprefix=DataType -transform=lower
type SqlDataType int

const (
	DataTypeBigInt SqlDataType = iota + 1
	DataTypeBinary
	DataTypeBoolean
	DataTypeDate
	DataTypeDecimal
	DataTypeDouble
	DataTypeFloat
	DataTypeInt
	DataTypeInterval
	DataTypeVoid
	DataTypeSmallInt
	DataTypeString
	DataTypeTimestamp
	DataTypeTimestamp_NTZ
	DataTypeTinyInt
	DataTypeArray
	DataTypeMap
	DataTypeStruct
)
