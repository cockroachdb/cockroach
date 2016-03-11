// Copyright 2016 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.
//
// Author: Raphael 'kena' Poss (knz@cockroachlabs.com)

package cli

import (
	"strings"

	"github.com/cockroachdb/cockroach/server"
)

type stringSliceValue []string

func (s *stringSliceValue) String() string {
	return strings.Join(*s, ";")
}

func (s *stringSliceValue) Type() string {
	return "stringSliceValue"
}

func (s *stringSliceValue) Set(value string) error {
	*s = append(*s, value)
	return nil
}

// Context contains global settings for the command-line client.
type Context struct {
	// Embed the server context.
	server.Context

	// execStmts is a list of statements to execute.
	execStmts stringSliceValue
}

// NewContext returns a Context with default values.
func NewContext() *Context {
	ctx := &Context{}
	ctx.InitDefaults()
	return ctx
}

// InitDefaults sets up the default values for a Context.
func (ctx *Context) InitDefaults() {
	ctx.Context.InitDefaults()
}
