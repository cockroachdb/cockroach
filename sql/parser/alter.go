// Copyright 2014 The Cockroach Authors.
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

package parser

import "fmt"

func (*AlterTable) statement() {}
func (*AlterView) statement()  {}

// AlterTable represents a ALTER TABLE statement.
type AlterTable struct {
	Name string
}

func (node *AlterTable) String() string {
	return fmt.Sprintf("ALTER TABLE %s", node.Name)
}

// AlterView represents a ALTER VIEW statement.
type AlterView struct {
	Name string
}

func (node *AlterView) String() string {
	return fmt.Sprintf("ALTER VIEW %s", node.Name)
}
