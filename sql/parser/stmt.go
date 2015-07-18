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

import "fmt"

// Instructions for creating new types: If a type needs to satisfy an
// interface, declare that function along with that interface. This
// will help users identify the list of types to which they can assert
// those interfaces. If the member of a type has a string with a
// predefined list of values, declare those values as const following
// the type. For interfaces that define dummy functions to
// consolidate a set of types, define the function as typeName().
// This will help avoid name collisions.

// Statement represents a statement.
type Statement interface {
	fmt.Stringer
	statement()
}

func (*CreateDatabase) statement() {}
func (*CreateTable) statement()    {}
func (*Delete) statement()         {}
func (*DropDatabase) statement()   {}
func (*DropTable) statement()      {}
func (*Insert) statement()         {}
func (*Select) statement()         {}
func (*Set) statement()            {}
func (*ShowColumns) statement()    {}
func (*ShowDatabases) statement()  {}
func (*ShowIndex) statement()      {}
func (*ShowTables) statement()     {}
func (*Truncate) statement()       {}
func (*Union) statement()          {}
func (*Update) statement()         {}
func (Values) statement()          {}
