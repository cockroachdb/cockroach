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
// permissions and limitations under the License.

package sql

import (
	"bytes"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
)

// distinctNode de-duplicates rows returned by a wrapped planNode.
type distinctNode struct {
	plan planNode
	p    *planner
	// All the columns that are part of the Sort. Set to nil if no-sort, or
	// sort used an expression that was not part of the requested column set.
	columnsInOrder []bool
	// Encoding of the columnsInOrder columns for the previous row.
	prefixSeen   []byte
	prefixMemAcc WrappableMemoryAccount

	// Encoding of the non-columnInOrder columns for rows sharing the same
	// prefixSeen value.
	suffixSeen   map[string]struct{}
	suffixMemAcc WrappableMemoryAccount
}

// distinct constructs a distinctNode.
func (p *planner) Distinct(n *parser.SelectClause) *distinctNode {
	if !n.Distinct {
		return nil
	}
	d := &distinctNode{p: p}
	d.prefixMemAcc = p.session.TxnState.OpenAccount()
	d.suffixMemAcc = p.session.TxnState.OpenAccount()
	return d
}

func (n *distinctNode) Start(params runParams) error {
	n.suffixSeen = make(map[string]struct{})
	return n.plan.Start(params)
}

func (n *distinctNode) Values() parser.Datums { return n.plan.Values() }

func (n *distinctNode) addSuffixSeen(
	ctx context.Context, acc WrappedMemoryAccount, sKey string,
) error {
	sz := int64(len(sKey))
	if err := acc.Grow(ctx, sz); err != nil {
		return err
	}
	n.suffixSeen[sKey] = struct{}{}
	return nil
}

func (n *distinctNode) Next(params runParams) (bool, error) {
	ctx := params.ctx

	prefixMemAcc := n.prefixMemAcc.Wtxn(n.p.session)
	suffixMemAcc := n.suffixMemAcc.Wtxn(n.p.session)

	for {
		if err := params.p.cancelChecker.Check(); err != nil {
			return false, err
		}

		next, err := n.plan.Next(params)
		if !next {
			return false, err
		}

		// Detect duplicates
		prefix, suffix, err := n.encodeValues(n.Values())
		if err != nil {
			return false, err
		}

		if !bytes.Equal(prefix, n.prefixSeen) {
			// The prefix of the row which is ordered differs from the last row;
			// reset our seen set.
			if len(n.suffixSeen) > 0 {
				suffixMemAcc.Clear(ctx)
				n.suffixSeen = make(map[string]struct{})
			}
			if err := prefixMemAcc.ResizeItem(ctx, int64(len(n.prefixSeen)), int64(len(prefix))); err != nil {
				return false, err
			}
			n.prefixSeen = prefix
			if suffix != nil {
				if err := n.addSuffixSeen(ctx, suffixMemAcc, string(suffix)); err != nil {
					return false, err
				}
			}
			return true, nil
		}

		// The prefix of the row is the same as the last row; check
		// to see if the suffix which is not ordered has been seen.
		if suffix != nil {
			sKey := string(suffix)
			if _, ok := n.suffixSeen[sKey]; !ok {
				if err := n.addSuffixSeen(ctx, suffixMemAcc, sKey); err != nil {
					return false, err
				}
				return true, nil
			}
		}
	}
}

// TODO(irfansharif): This can be refactored away to use
// sqlbase.EncodeDatums([]byte, parser.Datums)
func (n *distinctNode) encodeValues(values parser.Datums) ([]byte, []byte, error) {
	var prefix, suffix []byte
	var err error
	for i, val := range values {
		if n.columnsInOrder != nil && n.columnsInOrder[i] {
			if prefix == nil {
				prefix = make([]byte, 0, 100)
			}
			prefix, err = sqlbase.EncodeDatum(prefix, val)
		} else {
			if suffix == nil {
				suffix = make([]byte, 0, 100)
			}
			suffix, err = sqlbase.EncodeDatum(suffix, val)
		}
		if err != nil {
			break
		}
	}
	return prefix, suffix, err
}

func (n *distinctNode) Close(ctx context.Context) {
	n.plan.Close(ctx)
	n.prefixSeen = nil
	n.prefixMemAcc.Wtxn(n.p.session).Close(ctx)
	n.suffixSeen = nil
	n.suffixMemAcc.Wtxn(n.p.session).Close(ctx)
}
