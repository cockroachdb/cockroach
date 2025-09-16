// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package hints

import "github.com/cockroachdb/cockroach/pkg/util/protoutil"

// NewStatementHints converts the raw bytes from system.statement_hints into a
// StatementHints object.
func NewStatementHints(bytes []byte) (*StatementHints, error) {
	res := &StatementHints{}
	if err := protoutil.Unmarshal(bytes, res); err != nil {
		return nil, err
	}
	return res, nil
}

// ToBytes converts the StatementHints to a raw bytes representation that can be
// inserted into the system.statement_hints table.
func (hints *StatementHints) ToBytes() ([]byte, error) {
	return protoutil.Marshal(hints)
}
