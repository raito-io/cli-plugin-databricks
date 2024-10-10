//go:build tools
// +build tools

package main

import (
	_ "github.com/vektra/mockery/v2"

	_ "github.com/raito-io/enumer"

	_ "golang.org/x/net/idna"
)
