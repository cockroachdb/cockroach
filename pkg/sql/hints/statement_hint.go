// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

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
