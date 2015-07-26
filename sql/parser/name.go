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

import "bytes"

// A Name is an SQL identifier.
type Name string

// String formats an SQL identifier, applying proper escaping rules.
func (n Name) String() string {
	return encIdent(string(n))
}

// A NameList is a list of identifier.
// TODO(tschottdorf): would be nicer to have []Name here but unless we want
// to introduce new types to the grammar, NameList([]string{...}) needs to work.
type NameList []string

// String formats the contained names as a comma-separated, escaped string.
func (l NameList) String() string {
	var buf bytes.Buffer
	for i, n := range l {
		if i > 0 {
			_, _ = buf.WriteString(", ")
		}
		buf.WriteString(Name(n).String())
	}
	return buf.String()
}
