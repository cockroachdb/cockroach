// Copyright 2017 The Cockroach Authors.
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
	"fmt"
	"strings"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/sql/parser"
)

func (p *planner) Reset(ctx context.Context, n *parser.Reset) (planNode, error) {
	name := strings.ToUpper(n.Name.String())

	if v, ok := varGen[name]; ok {
		if v.Reset == nil {
			return nil, fmt.Errorf("variable \"%s\" cannot be changed", name)
		}

		if err := v.Reset(p); err != nil {
			return nil, err
		}
	} else {
		return nil, fmt.Errorf("unknown variable: %q", name)
	}

	return &emptyNode{}, nil
}
