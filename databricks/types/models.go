package types

import (
	"fmt"
	"iter"
	"regexp"
	"strings"

	"github.com/raito-io/cli/base/data_source"
	"github.com/raito-io/golang-set/set"
)

var nonAlphanumericRegex = regexp.MustCompile(`[^a-zA-Z0-9-_]+`)

type SecurableItemKey struct {
	Type     string
	FullName string
}

type PrivilegesChanges struct {
	Add    set.Set[string]
	Remove set.Set[string]

	AssociatedAPs set.Set[string]
}

type PrivilegesChangeCollection struct {
	M map[SecurableItemKey]map[string]*PrivilegesChanges
}

func NewPrivilegesChangeCollection() PrivilegesChangeCollection {
	return PrivilegesChangeCollection{
		M: make(map[SecurableItemKey]map[string]*PrivilegesChanges),
	}
}

func (c *PrivilegesChangeCollection) Iterator() iter.Seq2[SecurableItemKey, map[string]*PrivilegesChanges] {
	return func(yield func(SecurableItemKey, map[string]*PrivilegesChanges) bool) {
		for k, v := range c.M {
			if !yield(k, v) {
				return
			}
		}
	}
}

func (c *PrivilegesChangeCollection) AddPrivilege(securableItem SecurableItemKey, apID string, principal string, privilege ...string) {
	if _, ok := c.M[securableItem]; !ok {
		c.M[securableItem] = make(map[string]*PrivilegesChanges)
	}

	if _, ok := c.M[securableItem][principal]; !ok {
		c.M[securableItem][principal] = &PrivilegesChanges{Add: set.NewSet[string](privilege...), Remove: set.NewSet[string](), AssociatedAPs: set.NewSet[string](apID)}
	} else {
		c.M[securableItem][principal].Add.Add(privilege...)
		c.M[securableItem][principal].AssociatedAPs.Add(apID)
	}
}

func (c *PrivilegesChangeCollection) RemovePrivilege(securableItem SecurableItemKey, principal string, privilege ...string) {
	if _, ok := c.M[securableItem]; !ok {
		c.M[securableItem] = make(map[string]*PrivilegesChanges)
	}

	if _, ok := c.M[securableItem][principal]; !ok {
		c.M[securableItem][principal] = &PrivilegesChanges{Add: set.NewSet[string](), Remove: set.NewSet(privilege...), AssociatedAPs: set.NewSet[string]()}
	} else {
		c.M[securableItem][principal].Remove.Add(privilege...)
	}
}

type PrivilegeCache struct {
	m map[data_source.DataObjectReference]map[string]set.Set[string]
}

func NewPrivilegeCache() PrivilegeCache {
	return PrivilegeCache{
		m: make(map[data_source.DataObjectReference]map[string]set.Set[string]),
	}
}

func (c *PrivilegeCache) AddPrivilege(item data_source.DataObjectReference, principal string, privilege ...string) {
	if _, ok := c.m[item]; !ok {
		c.m[item] = make(map[string]set.Set[string])
	}

	if _, ok := c.m[item][principal]; !ok {
		c.m[item][principal] = set.NewSet(privilege...)
	} else {
		c.m[item][principal].Add(privilege...)
	}
}

func (c *PrivilegeCache) ContainsPrivilege(item data_source.DataObjectReference, principal string, privilege string) bool {
	if _, ok := c.m[item]; !ok {
		return false
	}

	if _, ok := c.m[item][principal]; !ok {
		return false
	}

	return c.m[item][principal].Contains(privilege)
}

type MaskDataObjectsOfSchema struct {
	DataObjects        map[string][]string // Table Name => []Column Name
	DeletedDataObjects map[string][]string // Table Name => []Column Name
}

func (m *MaskDataObjectsOfSchema) AllDataObjects() map[string][]string {
	result := make(map[string][]string)

	for k, v := range m.DataObjects {
		result[k] = append(result[k], v...)
	}

	for k, v := range m.DeletedDataObjects {
		result[k] = append(result[k], v...)
	}

	return result
}

type WarehouseDetails struct {
	Workspace string `json:"workspace"`
	Warehouse string `json:"warehouse"`
}

type StoredFunctions struct {
	Masks   map[string][]string
	Filters map[string][]string
}

func NewStoredFunctions() StoredFunctions {
	return StoredFunctions{
		Masks:   make(map[string][]string),
		Filters: make(map[string][]string),
	}
}

func (s *StoredFunctions) AddMask(functionId string, columnId string) {
	s.Masks[functionId] = append(s.Masks[functionId], columnId)
}

func (s *StoredFunctions) AddFilter(functionId string, tableId string) {
	s.Filters[functionId] = append(s.Filters[functionId], tableId)
}

type ColumnReference string

func (c ColumnReference) Escaped() string {
	escapedString := string(c)

	if !strings.HasPrefix(escapedString, "`") || !strings.HasSuffix(escapedString, "`") {
		escapedString = fmt.Sprintf("`%s`", escapedString)
	}

	return escapedString
}

func (c ColumnReference) Trimmed() string {
	return TrimName(string(c))
}

func TrimName(name string) string {
	return nonAlphanumericRegex.ReplaceAllString(name, "")
}
