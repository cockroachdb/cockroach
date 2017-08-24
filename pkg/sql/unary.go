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

package sql

import (
	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/sql/parser"
)

// unaryNode is a planNode with no columns and a single row with empty results
// which is used by select statements that have no table. It is used for its
// property as the join identity.
type unaryNode struct {
	consumed bool
}

func (*unaryNode) Values() parser.Datums { return nil }
func (*unaryNode) Start(runParams) error { return nil }
func (*unaryNode) Close(context.Context) {}

func (u *unaryNode) Next(runParams) (bool, error) {
	r := !u.consumed
	u.consumed = true
	return r, nil
}
