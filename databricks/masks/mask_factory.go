package masks

import (
	"fmt"
	"regexp"
	"strings"
	"unicode"
)

var _maskFactory *MaskFactory

const (
	DefaultMaskId = "DEFAULT_MASK"
	SHA256MaskId  = "SHA256_MASK"
)

func init() {
	_maskFactory = NewMaskFactory()
	_maskFactory.RegisterMaskGenerator(DefaultMaskId, DefaultMask())
	_maskFactory.RegisterMaskGenerator(SHA256MaskId, HashSha256Mask())
}

//go:generate go run github.com/vektra/mockery/v2 --name=MaskGenerator --with-expecter --inpackage
type MaskGenerator interface {
	Generate(maskName string, columnType string, beneficiaries *MaskingBeneficiaries) (MaskingPolicy, error)
}

//go:generate go run github.com/vektra/mockery/v2 --name=SimpleMaskMethod --with-expecter --inpackage
type SimpleMaskMethod interface {
	MaskMethod(variableName string, columnType SqlDataType) string
	SupportedType(columnType SqlDataType) bool
}

type SimpleMaskGenerator struct {
	SimpleMaskMethod
}

type MaskingBeneficiaries struct {
	Users  []string
	Groups []string
}

type MaskFactory struct {
	maskGenerators map[string]MaskGenerator
}

type MaskingPolicy string

func NewMaskFactory() *MaskFactory {
	if _maskFactory == nil {
		_maskFactory = &MaskFactory{
			maskGenerators: make(map[string]MaskGenerator),
		}
	}

	return _maskFactory
}

func (f *MaskFactory) RegisterMaskGenerator(maskType string, maskGenerator MaskGenerator) {
	f.maskGenerators[maskType] = maskGenerator
}

func (f *MaskFactory) CreateMask(maskName string, columnType string, maskType *string, beneficiaries *MaskingBeneficiaries) (string, MaskingPolicy, error) {
	policyName := fmt.Sprintf("%s_%s", maskName, columnType)

	allowedPolicyNameArray := make([]rune, 0, len(policyName))

	for _, r := range policyName {
		if unicode.IsLetter(r) || unicode.IsDigit(r) || r == '_' {
			allowedPolicyNameArray = append(allowedPolicyNameArray, r)
		}
	}

	policyName = string(allowedPolicyNameArray)

	maskGen := DefaultMask()

	if maskType != nil {
		if gen, ok := f.maskGenerators[*maskType]; ok {
			maskGen = gen
		}
	}

	policy, err := maskGen.Generate(policyName, columnType, beneficiaries)
	if err != nil {
		maskGen = DefaultMask()

		policy, _ = maskGen.Generate(policyName, columnType, beneficiaries) // NULLMASK may never return an error and is used as a fallback
	}

	return policyName, policy, err
}

func NewSimpleMaskGenerator(method SimpleMaskMethod) *SimpleMaskGenerator {
	return &SimpleMaskGenerator{
		SimpleMaskMethod: method,
	}
}

var typeParseRegex = regexp.MustCompile("(^[a-zA-Z]*)")

func (g *SimpleMaskGenerator) Generate(maskName string, columnTypeString string, beneficiaries *MaskingBeneficiaries) (MaskingPolicy, error) {
	trimmedColumnType := typeParseRegex.ReplaceAllString(columnTypeString, "${1}")

	columnType, err := SqlDataTypeString(trimmedColumnType)
	if err != nil {
		return "", err
	}

	if !g.SupportedType(columnType) {
		return "", fmt.Errorf("unsupported type %s", columnType.String())
	}

	var maskingPolicyBuilder strings.Builder

	maskingPolicyBuilder.WriteString(fmt.Sprintf("CREATE OR REPLACE FUNCTION %s(val %s)\nRETURN ", maskName, columnType))

	var cases []string

	if len(beneficiaries.Users) > 0 {
		var users []string
		for _, user := range beneficiaries.Users {
			users = append(users, fmt.Sprintf("'%s'", user))
		}

		cases = append(cases, fmt.Sprintf("WHEN current_user() IN (%s) THEN val", strings.Join(users, ", ")))
	}

	if len(beneficiaries.Groups) > 0 {
		for _, group := range beneficiaries.Groups {
			cases = append(cases, fmt.Sprintf("WHEN is_account_group_member('%s') THEN val", group))
		}
	}

	maskFn := g.MaskMethod("val", columnType)

	if len(cases) == 0 {
		maskingPolicyBuilder.WriteString(maskFn)
	} else {
		maskingPolicyBuilder.WriteString("CASE\n")

		for _, c := range cases {
			maskingPolicyBuilder.WriteString(fmt.Sprintf("\t%s\n", c))
		}

		maskingPolicyBuilder.WriteString(fmt.Sprintf("\tELSE %s\n", maskFn))
		maskingPolicyBuilder.WriteString("END")
	}

	maskingPolicyBuilder.WriteString(";")

	return MaskingPolicy(maskingPolicyBuilder.String()), nil
}
