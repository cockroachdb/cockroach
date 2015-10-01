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

// Set represents a SET statement.
type Set struct {
	Name   *QualifiedName
	Values Exprs
}

func (node *Set) String() string {
	if node.Values == nil {
		return fmt.Sprintf("SET %s = DEFAULT", node.Name)
	}
	return fmt.Sprintf("SET %s = %v", node.Name, node.Values)
}

// SetTransaction represents a SET TRANSACTION statement.
type SetTransaction struct {
	Isolation IsolationLevel
}

func (node *SetTransaction) String() string {
	return fmt.Sprintf("SET TRANSACTION ISOLATION LEVEL %s", node.Isolation)
}

// SetTimeZone represents a SET TIME ZONE statement.
type SetTimeZone struct {
	Value Expr
}

func (node *SetTimeZone) String() string {
	prefix := "SET TIME ZONE"
	switch v := node.Value.(type) {
	case DInterval:
		return fmt.Sprintf("%s INTERVAL '%s'", prefix, v)

	case DString:
		if zone := string(v); zone == "DEFAULT" || zone == "LOCAL" {
			return fmt.Sprintf("%s %s", prefix, zone)
		}
	}
	return fmt.Sprintf("%s %s", prefix, node.Value)
}
