package masks

import "fmt"

func HashSha256Mask() MaskGenerator {
	return NewSimpleMaskGenerator(&hashMaskMethod{bitLength: 256})
}

type hashMaskMethod struct {
	bitLength int
}

func (m *hashMaskMethod) MaskMethod(variableName string, _ SqlDataType) string {
	return fmt.Sprintf("sha2(%s, %d)", variableName, m.bitLength)
}

func (m *hashMaskMethod) SupportedType(columnType SqlDataType) bool {
	return columnType == DataTypeString || columnType == DataTypeBinary
}
