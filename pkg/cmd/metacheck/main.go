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
// Author: Tamir Duberstein (tamird@gmail.com)

package main

import (
	"log"
	"os"

	"honnef.co/go/tools/lint"
	"honnef.co/go/tools/lint/lintutil"
	"honnef.co/go/tools/simple"
	"honnef.co/go/tools/staticcheck"
	"honnef.co/go/tools/unused"
)

type metaChecker struct {
	checkers []lint.Checker
}

func (m *metaChecker) Init(program *lint.Program) {
	for _, checker := range m.checkers {
		checker.Init(program)
	}
}

func (m *metaChecker) Funcs() map[string]lint.Func {
	funcs := make(map[string]lint.Func)
	for _, checker := range m.checkers {
		for k, v := range checker.Funcs() {
			if _, ok := funcs[k]; ok {
				log.Fatalf("duplicate lint function %s", k)
			} else {
				funcs[k] = v
			}
		}
	}
	return funcs
}

func main() {
	unusedChecker := unused.NewChecker(unused.CheckAll)
	unusedChecker.WholeProgram = true
	meta := metaChecker{
		checkers: []lint.Checker{
			simple.NewChecker(),
			staticcheck.NewChecker(),
			unused.NewLintChecker(unusedChecker),
		},
	}
	lintutil.ProcessArgs("metacheck", &meta, os.Args[1:])
}
