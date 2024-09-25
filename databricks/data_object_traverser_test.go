package databricks

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestObjectFilter_IncludeObject(t *testing.T) {
	type fields struct {
		excludeExpressions string
		includeExpressions string
	}
	type test struct {
		objectName string
		want       bool
	}
	tests := []struct {
		name   string
		fields fields
		tests  []test
	}{
		{
			name: "No filters",
			fields: fields{
				excludeExpressions: "",
				includeExpressions: "",
			},
			tests: []test{
				{
					objectName: "object1",
					want:       true,
				},
				{
					objectName: "schema2",
					want:       true,
				},
				{
					objectName: "object3",
					want:       true,
				},
			},
		},
		{
			name: "Exclude filter",
			fields: fields{
				excludeExpressions: "obj.*1,.*2",
			},
			tests: []test{
				{
					objectName: "object1",
					want:       false,
				},
				{
					objectName: "schema2",
					want:       false,
				},
				{
					objectName: "object3",
					want:       true,
				},
			},
		},
		{
			name: "Include filter",
			fields: fields{
				includeExpressions: "obj.*1,.*2",
			},
			tests: []test{
				{
					objectName: "object1",
					want:       true,
				},
				{
					objectName: "schema2",
					want:       true,
				},
				{
					objectName: "object3",
					want:       false,
				},
			},
		},
		{
			name: "Include and exclude filter",
			fields: fields{
				excludeExpressions: "obj.*1",
				includeExpressions: ".*1",
			},
			tests: []test{
				{
					objectName: "object1",
					want:       false,
				},
				{
					objectName: "schema2",
					want:       true,
				},
				{
					objectName: "object3",
					want:       true,
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			o, err := NewObjectFilter(tt.fields.excludeExpressions, tt.fields.includeExpressions)
			require.NoError(t, err)

			for _, objectTest := range tt.tests {
				t.Run(objectTest.objectName, func(t *testing.T) {
					assert.Equal(t, objectTest.want, o.IncludeObject(objectTest.objectName))
				})
			}
		})
	}
}
