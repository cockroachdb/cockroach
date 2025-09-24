// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package hints

import "github.com/cockroachdb/cockroach/pkg/util/protoutil"

type StatementHint interface {
	protoutil.Message
}

// NewStatementHint converts the raw bytes from system.statement_hints into a
// StatementHintUnion object.
func NewStatementHint(bytes []byte) (*StatementHintUnion, error) {
	res := &StatementHintUnion{}
	if err := protoutil.Unmarshal(bytes, res); err != nil {
		return nil, err
	}
	return res, nil
}

// ToBytes converts the StatementHintUnion to a raw bytes representation that
// can be inserted into the system.statement_hints table.
func (hint *StatementHintUnion) ToBytes() ([]byte, error) {
	return protoutil.Marshal(hint)
}
