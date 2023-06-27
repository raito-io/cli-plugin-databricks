package databricks

import (
	"fmt"
	"strings"

	"github.com/raito-io/cli/base/data_source"
	"github.com/raito-io/golang-set/set"
)

type MetastoreAssignment struct {
	WorkspaceIds []int `json:"workspace_ids,omitempty"`
}

type Workspace struct {
	WorkspaceId     int    `json:"workspace_id"`
	WorkspaceName   string `json:"workspace_name"`
	WorkspaceStatus string `json:"workspace_status"`
	DeploymentName  string `json:"deployment_name"`
}

type SecurableItemKey struct {
	Type     string
	FullName string
}

type PrivilegesChanges struct {
	Add    set.Set[string]
	Remove set.Set[string]
}

type PrivilegesChangeCollection struct {
	m map[SecurableItemKey]map[string]*PrivilegesChanges
}

func NewPrivilegesChangeCollection() PrivilegesChangeCollection {
	return PrivilegesChangeCollection{
		m: make(map[SecurableItemKey]map[string]*PrivilegesChanges),
	}
}

func (c *PrivilegesChangeCollection) AddPrivilege(securableItem SecurableItemKey, principal string, privilege ...string) {
	if _, ok := c.m[securableItem]; !ok {
		c.m[securableItem] = make(map[string]*PrivilegesChanges)
	}

	if _, ok := c.m[securableItem][principal]; !ok {
		c.m[securableItem][principal] = &PrivilegesChanges{Add: set.NewSet[string](privilege...), Remove: set.NewSet[string]()}
	} else {
		c.m[securableItem][principal].Add.Add(privilege...)
	}
}

func (c *PrivilegesChangeCollection) RemovePrivilege(securableItem SecurableItemKey, principal string, privilege ...string) {
	if _, ok := c.m[securableItem]; !ok {
		c.m[securableItem] = make(map[string]*PrivilegesChanges)
	}

	if _, ok := c.m[securableItem][principal]; !ok {
		c.m[securableItem][principal] = &PrivilegesChanges{Add: set.NewSet[string](), Remove: set.NewSet(privilege...)}
	} else {
		c.m[securableItem][principal].Remove.Add(privilege...)
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

func (c *PrivilegeCache) DebugString() string {
	var builder strings.Builder

	builder.WriteString("Privilege Cache: {\n")

	for item := range c.m {
		builder.WriteString(fmt.Sprintf("{{FullName: %s, Type: %s} : [", item.FullName, item.Type))

		for principal, privileges := range c.m[item] {
			builder.WriteString(fmt.Sprintf("{Pirincipal: %s Privilages [", principal))
			//builder.WriteString(strings.Join(array.Map(privileges.Slice(), func(i *catalog.Privilege) string { return i.String() }), ", "))
			builder.WriteString(strings.Join(privileges.Slice(), ", "))
			builder.WriteString("]}")
		}
		builder.WriteString("]}\n")
	}

	builder.WriteString("}")

	return builder.String()
}

type databricksUsersFilter struct {
	username *string
}
