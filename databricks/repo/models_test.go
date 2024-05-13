package repo

import (
	"fmt"
	"testing"

	"github.com/raito-io/bexpression/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestChannelItem_Interface(t *testing.T) {
	type testCase[T any] struct {
		name string
		c    ChannelItem[T]
		want interface{}
	}
	tests := []testCase[string]{
		{
			name: "error",
			c:    ChannelItem[string]{Err: assert.AnError},
			want: assert.AnError,
		},
		{
			name: "item",
			c:    ChannelItem[string]{I: utils.Ptr("value")},
			want: "value",
		},
		{
			name: "nil",
			c:    ChannelItem[string]{},
			want: nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equalf(t, tt.want, tt.c.Interface(), "Interface()")
		})
	}
}

func TestChannelItem_Error(t *testing.T) {
	type testCase[T any] struct {
		name    string
		c       ChannelItem[T]
		wantErr assert.ErrorAssertionFunc
	}
	tests := []testCase[string]{
		{
			name:    "error",
			c:       ChannelItem[string]{Err: assert.AnError},
			wantErr: assert.Error,
		},
		{
			name:    "item",
			c:       ChannelItem[string]{},
			wantErr: assert.NoError,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.wantErr(t, tt.c.Error(), fmt.Sprintf("Error()"))
		})
	}
}

func TestChannelItem_Item(t *testing.T) {
	type testCase[T any] struct {
		name string
		c    ChannelItem[T]
		want T
	}
	tests := []testCase[string]{
		{
			name: "item",
			c:    ChannelItem[string]{I: utils.Ptr("value")},
			want: "value",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equalf(t, tt.want, tt.c.Item(), "Item()")
		})
	}
}

func TestChannelItem_HasError(t *testing.T) {
	type testCase[T any] struct {
		name string
		c    ChannelItem[T]
		want bool
	}
	tests := []testCase[string]{
		{
			name: "error",
			c:    ChannelItem[string]{Err: assert.AnError},
			want: true,
		},
		{
			name: "item",
			c:    ChannelItem[string]{},
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equalf(t, tt.want, tt.c.HasError(), "HasError()")
		})
	}
}

func TestChannelItem_HasItem(t *testing.T) {
	type testCase[T any] struct {
		name string
		c    ChannelItem[T]
		want bool
	}
	tests := []testCase[string]{
		{
			name: "item",
			c:    ChannelItem[string]{I: utils.Ptr("value")},
			want: true,
		},
		{
			name: "nil",
			c:    ChannelItem[string]{},
			want: false,
		},
		{
			name: "error",
			c:    ChannelItem[string]{Err: assert.AnError},
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equalf(t, tt.want, tt.c.HasItem(), "HasItem()")
		})
	}
}

func TestArrayToChannel(t *testing.T) {
	type args[T any] struct {
		a []T
	}
	type testCase[T any] struct {
		name string
		args args[T]
	}
	tests := []testCase[string]{
		{
			name: "empty",
			args: args[string]{},
		},
		{
			name: "single",
			args: args[string]{a: []string{"value"}},
		},
		{
			name: "multiple",
			args: args[string]{a: []string{"value1", "value2"}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ch := ArrayToChannel[string](tt.args.a)
			var result []string

			for item := range ch {
				require.NoError(t, item.Error())
				result = append(result, item.Item())
			}
		})
	}
}
