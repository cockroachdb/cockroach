// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package ltree

import (
	"unicode"

	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
)

// Postgres docs mention labels must be less than 256 bytes, but in practice,
// Postgres has a limit on up to 1000 characters.
const MaxLabelLength = 1000

var ErrEmptyLabel = pgerror.New(pgcode.Syntax, "label cannot be empty")

type Label string

func NewLabel(label string) (Label, error) {
	if len(label) > MaxLabelLength {
		return "", pgerror.Newf(pgcode.NameTooLong, "label length is %d, must be at most %d", len(label), MaxLabelLength)
	}
	if label == "" {
		return "", ErrEmptyLabel
	}
	for _, c := range label {
		if !unicode.IsLetter(c) && !unicode.IsDigit(c) && c != '_' && c != '-' {
			return "", pgerror.Newf(pgcode.Syntax, "label contains invalid character")
		}
	}
	return Label(label), nil
}
