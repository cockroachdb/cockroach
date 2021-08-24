// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package eventpb

import (
	"fmt"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
)

var _ error = &CommonLargeRowDetails{}
var _ errors.SafeDetailer = &CommonLargeRowDetails{}
var _ fmt.Formatter = &CommonLargeRowDetails{}
var _ errors.SafeFormatter = &CommonLargeRowDetails{}

// Error is part of the error interface, which CommonLargeRowDetails implements.
func (r *CommonLargeRowDetails) Error() string {
	return fmt.Sprintf(
		"row larger than max row size: table %v family %v primary key %v size %v",
		errors.Safe(r.TableID), errors.Safe(r.FamilyID), r.PrimaryKey, errors.Safe(r.RowSize),
	)
}

// SafeDetails is part of the errors.SafeDetailer interface, which
// CommonLargeRowDetails implements.
func (r *CommonLargeRowDetails) SafeDetails() []string {
	return []string{
		fmt.Sprint(r.TableID),
		fmt.Sprint(r.FamilyID),
		fmt.Sprint(r.RowSize),
	}
}

// Format is part of the fmt.Formatter interface, which CommonLargeRowDetails
// implements.
func (r *CommonLargeRowDetails) Format(s fmt.State, verb rune) { errors.FormatError(r, s, verb) }

// SafeFormatError is part of the errors.SafeFormatter interface, which
// CommonLargeRowDetails implements.
func (r *CommonLargeRowDetails) SafeFormatError(p errors.Printer) (next error) {
	if p.Detail() {
		p.Printf(
			"row larger than max row size: table %v family %v size %v",
			errors.Safe(r.TableID), errors.Safe(r.FamilyID), errors.Safe(r.RowSize),
		)
	}
	return nil
}

var _ error = &CommonTxnRowsLimitDetails{}
var _ errors.SafeDetailer = &CommonTxnRowsLimitDetails{}
var _ fmt.Formatter = &CommonTxnRowsLimitDetails{}
var _ errors.SafeFormatter = &CommonTxnRowsLimitDetails{}

func (r *CommonTxnRowsLimitDetails) kind() string {
	if r.ReadKind {
		return "read"
	}
	return "written"
}

// Error is part of the error interface, which
// CommonTxnRowsReadLimitDetails implements.
func (r *CommonTxnRowsLimitDetails) Error() string {
	return fmt.Sprintf(
		"txn reached the number of rows %s: TxnID %v SessionID %v",
		r.kind(), redact.SafeString(r.TxnID), redact.SafeString(r.SessionID),
	)
}

// SafeDetails is part of the errors.SafeDetailer interface, which
// CommonTxnRowsReadLimitDetails implements.
func (r *CommonTxnRowsLimitDetails) SafeDetails() []string {
	return []string{r.TxnID, r.SessionID, r.kind()}
}

// Format is part of the fmt.Formatter interface, which
// CommonTxnRowsReadLimitDetails implements.
func (r *CommonTxnRowsLimitDetails) Format(s fmt.State, verb rune) {
	errors.FormatError(r, s, verb)
}

// SafeFormatError is part of the errors.SafeFormatter interface, which
// CommonLargeRowDetails implements.
func (r *CommonTxnRowsLimitDetails) SafeFormatError(p errors.Printer) (next error) {
	if p.Detail() {
		p.Printf(
			"txn reached the number of rows %s: TxnID %v SessionID %v",
			r.kind(), redact.SafeString(r.TxnID), redact.SafeString(r.SessionID),
		)
	}
	return nil
}
