// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package semenumpb

import "github.com/cockroachdb/redact"

var _ redact.SafeValue = ForeignKeyAction(0)

// SafeValue implements redact.SafeValue.
func (x ForeignKeyAction) SafeValue() {}
