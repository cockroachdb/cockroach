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
// Author: Dan Harrison (daniel.harrison@gmail.com)

package sql

import (
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/sql/parser"
)

func (p *planner) SelectOrderLimit(n *parser.SelectOrderLimit, autoCommit bool) (planNode, *roachpb.Error) {
	wrapped := n.Select
	for {
		switch s := wrapped.(type) {
		case *parser.ParenSelect:
			wrapped = s.Select
			continue
		case *parser.Select:
			// Select can potentially optimize index selection if it's being ordered.
			s.OrderBy = n.OrderBy
			s.Limit = n.Limit
			return p.Select(s)
		// TODO(dan): Union can also do optimizations when it has an ORDER BY, but
		// currently expects the ordering to be done externally, so we let it fall
		// through. Instead of continuing this special casing, it may be worth
		// investigating a general mechanism for passing some context down during
		// plan node construction.
		default:
			plan, pberr := p.makePlan(s, autoCommit)
			if pberr != nil {
				return nil, pberr
			}
			sort, pberr := p.orderBy(n.OrderBy, plan)
			if pberr != nil {
				return nil, pberr
			}
			var err error
			plan, err = p.limit(n.Limit, sort.wrap(plan))
			if err != nil {
				return nil, roachpb.NewError(err)
			}
			return plan, nil
		}
	}
}
