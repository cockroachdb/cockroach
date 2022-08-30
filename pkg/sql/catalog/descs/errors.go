// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package descs

import (
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/lease"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
)

// IsTwoVersionInvariantViolationError is true if the error is the
// special error returned from CheckTwoVersionInvariant.
func IsTwoVersionInvariantViolationError(err error) bool {
	return errors.HasType(err, (*twoVersionInvariantViolationError)(nil))
}

type twoVersionInvariantViolationError struct {
	ids []lease.IDVersion
}

func (t *twoVersionInvariantViolationError) Error() string {
	return redact.Sprint(t).StripMarkers()
}

func (t twoVersionInvariantViolationError) SafeFormatError(p errors.Printer) (next error) {
	p.Printf("cannot publish new versions for descriptors: "+
		"%v, old versions still in use", t.ids)
	return nil
}

var _ errors.SafeFormatter = (*twoVersionInvariantViolationError)(nil)
