// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

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
