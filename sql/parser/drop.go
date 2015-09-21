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

// DropDatabase represents a DROP DATABASE statement.
type DropDatabase struct {
	Name     Name
	IfExists bool
}

func (node *DropDatabase) String() string {
	var buf bytes.Buffer
	buf.WriteString("DROP DATABASE ")
	if node.IfExists {
		buf.WriteString("IF EXISTS ")
	}
	buf.WriteString(node.Name.String())
	return buf.String()
}

// DropIndex represents a DROP DATABASE statement.
type DropIndex struct {
	Names    QualifiedNames
	IfExists bool
}

func (node *DropIndex) String() string {
	var buf bytes.Buffer
	buf.WriteString("DROP INDEX ")
	if node.IfExists {
		buf.WriteString("IF EXISTS ")
	}
	buf.WriteString(node.Names.String())
	return buf.String()
}

// DropTable represents a DROP TABLE statement.
type DropTable struct {
	Names    QualifiedNames
	IfExists bool
}

func (node *DropTable) String() string {
	var buf bytes.Buffer
	buf.WriteString("DROP TABLE ")
	if node.IfExists {
		buf.WriteString("IF EXISTS ")
	}
	buf.WriteString(node.Names.String())
	return buf.String()
}
