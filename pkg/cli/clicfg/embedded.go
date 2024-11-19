// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package clicfg

import "fmt"

// PrintlnUnlessEmbedded is like fmt.Println except that nothing is printed if
// EmbeddedMode is set.
func (cliCtx *Context) PrintlnUnlessEmbedded(args ...interface{}) {
	if !cliCtx.EmbeddedMode {
		fmt.Println(args...)
	}
}

// PrintfUnlessEmbedded is like fmt.Printf except that nothing is printed if
// EmbeddedMode is set.
func (cliCtx *Context) PrintfUnlessEmbedded(f string, args ...interface{}) {
	if !cliCtx.EmbeddedMode {
		fmt.Printf(f, args...)
	}
}
