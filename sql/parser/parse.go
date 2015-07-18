// Copyright 2015 The Cockroach Authors.
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
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Peter Mattis (peter@cockroachlabs.com)

// This code was derived from https://github.com/youtube/vitess.
//
// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file

package parser

import (
	"bytes"
	"errors"
)

//go:generate make

// StatementList is a list of statements.
type StatementList []Statement

func (l StatementList) String() string {
	var buf bytes.Buffer
	for i, s := range l {
		if i > 0 {
			_, _ = buf.WriteString("; ")
		}
		_, _ = buf.WriteString(s.String())
	}
	return buf.String()
}

// Parse parses the sql and returns a list of statements.
func Parse(sql string) (StatementList, error) {
	s := newScanner(sql)
	if sqlParse(s) != 0 {
		return nil, errors.New(s.lastError)
	}
	return s.stmts, nil
}
