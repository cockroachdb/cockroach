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

	"github.com/cockroachdb/cockroach/base"
)

// statementsValue is an implementation of pflag.Value that appends any
// argument to a slice.
type statementsValue []string

func (s *statementsValue) String() string {
	return strings.Join(*s, ";")
}

func (s *statementsValue) Type() string {
	return "statementsValue"
}

func (s *statementsValue) Set(value string) error {
	*s = append(*s, value)
	return nil
}

type cliContext struct {
	// Embed the base context.
	*base.Context

	// prettyFmt indicates whether tables should be pretty-formatted in
	// the output during non-interactive execution.
	prettyFmt bool
}

func (ctx *cliContext) InitCLIDefaults() {
	ctx.prettyFmt = false
}

type sqlContext struct {
	// Embed the cli context.
	*cliContext

	// execStmts is a list of statements to execute.
	execStmts statementsValue
}

type debugContext struct {
	startKey, endKey string
	typ              string
	values           bool
}
