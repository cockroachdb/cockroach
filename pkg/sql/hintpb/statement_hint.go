// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package hintpb

import (
	"encoding/json"

	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
)

// FromBytes converts the raw bytes from system.statement_hints into a
// StatementHintUnion object.
func FromBytes(bytes []byte) (StatementHintUnion, error) {
	res := StatementHintUnion{}
	if err := protoutil.Unmarshal(bytes, &res); err != nil {
		return StatementHintUnion{}, err
	}
	if res.GetValue() == nil {
		return StatementHintUnion{}, errors.New("invalid hint bytes: no value set")
	}
	return res, nil
}

// ToBytes converts the StatementHintUnion to a raw bytes representation that
// can be inserted into the system.statement_hints table.
func ToBytes(hint StatementHintUnion) ([]byte, error) {
	if hint.GetValue() == nil {
		return nil, errors.New("cannot convert empty hint to bytes")
	}
	return protoutil.Marshal(&hint)
}

// HintType returns a string representation of the hint type for display.
func (h StatementHintUnion) HintType() string {
	switch h.GetValue().(type) {
	case *InjectHints:
		return "inject_hints"
	default:
		return "unknown"
	}
}

// Details returns a JSON representation of the hint details. This is used for
// displaying hint information in SHOW STATEMENT HINTS WITH DETAILS.
func (h *StatementHintUnion) Details() (string, error) {
	// Create a simple map representation of the hint details.
	var details map[string]interface{}
	if injectHints := h.InjectHints; injectHints != nil {
		details = map[string]interface{}{"donor_sql": injectHints.DonorSQL}
	} else {
		return "", errors.New("unknown hint type")
	}
	bytes, err := json.Marshal(details)
	if err != nil {
		return "", err
	}
	return string(bytes), nil
}
