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

func (d *CommonTxnRowsLimitDetails) kind() string {
	if d.ReadKind {
		return "read"
	}
	return "written"
}

// Error is part of the error interface, which CommonTxnRowsLimitDetails
// implements.
func (d *CommonTxnRowsLimitDetails) Error() string {
	return fmt.Sprintf(
		"txn reached the number of rows %s: TxnID %v SessionID %v",
		d.kind(), redact.SafeString(d.TxnID), redact.SafeString(d.SessionID),
	)
}

// SafeDetails is part of the errors.SafeDetailer interface, which
// CommonTxnRowsLimitDetails implements.
func (d *CommonTxnRowsLimitDetails) SafeDetails() []string {
	return []string{d.TxnID, d.SessionID, d.kind()}
}

// Format is part of the fmt.Formatter interface, which
// CommonTxnRowsLimitDetails implements.
func (d *CommonTxnRowsLimitDetails) Format(s fmt.State, verb rune) {
	errors.FormatError(d, s, verb)
}

// SafeFormatError is part of the errors.SafeFormatter interface, which
// CommonTxnRowsLimitDetails implements.
func (d *CommonTxnRowsLimitDetails) SafeFormatError(p errors.Printer) (next error) {
	if p.Detail() {
		p.Printf(
			"txn reached the number of rows %s: TxnID %v SessionID %v",
			d.kind(), redact.SafeString(d.TxnID), redact.SafeString(d.SessionID),
		)
	}
	return nil
}

var _ error = &TxnRowsWrittenLimit{}
var _ errors.SafeDetailer = &TxnRowsWrittenLimit{}
var _ fmt.Formatter = &TxnRowsWrittenLimit{}
var _ errors.SafeFormatter = &TxnRowsWrittenLimit{}

// Error is part of the error interface, which TxnRowsWrittenLimit implements.
func (d *TxnRowsWrittenLimit) Error() string {
	return fmt.Sprintf("%s\n%s", d.CommonTxnRowsLimitDetails.Error(), d.CommonSQLEventDetails.Error())
}

// SafeDetails is part of the errors.SafeDetailer interface, which
// TxnRowsWrittenLimit implements.
func (d *TxnRowsWrittenLimit) SafeDetails() []string {
	return append(d.CommonTxnRowsLimitDetails.SafeDetails(), d.CommonSQLEventDetails.SafeDetails()...)
}

// Format is part of the fmt.Formatter interface, which TxnRowsWrittenLimit
// implements.
func (d *TxnRowsWrittenLimit) Format(s fmt.State, verb rune) {
	errors.FormatError(d, s, verb)
}

// SafeFormatError is part of the errors.SafeFormatter interface, which
// TxnRowsWrittenLimit implements.
func (d *TxnRowsWrittenLimit) SafeFormatError(p errors.Printer) (next error) {
	next = d.CommonTxnRowsLimitDetails.SafeFormatError(p)
	if next == nil {
		next = d.CommonSQLEventDetails.SafeFormatError(p)
	}
	return next
}

var _ error = &TxnRowsReadLimit{}
var _ errors.SafeDetailer = &TxnRowsReadLimit{}
var _ fmt.Formatter = &TxnRowsReadLimit{}
var _ errors.SafeFormatter = &TxnRowsReadLimit{}

// Error is part of the error interface, which TxnRowsReadLimit implements.
func (d *TxnRowsReadLimit) Error() string {
	return fmt.Sprintf("%s\n%s", d.CommonTxnRowsLimitDetails.Error(), d.CommonSQLEventDetails.Error())
}

// SafeDetails is part of the errors.SafeDetailer interface, which
// TxnRowsReadLimit implements.
func (d *TxnRowsReadLimit) SafeDetails() []string {
	return append(d.CommonTxnRowsLimitDetails.SafeDetails(), d.CommonSQLEventDetails.SafeDetails()...)
}

// Format is part of the fmt.Formatter interface, which TxnRowsReadLimit
// implements.
func (d *TxnRowsReadLimit) Format(s fmt.State, verb rune) {
	errors.FormatError(d, s, verb)
}

// SafeFormatError is part of the errors.SafeFormatter interface, which
// TxnRowsReadLimit implements.
func (d *TxnRowsReadLimit) SafeFormatError(p errors.Printer) (next error) {
	next = d.CommonTxnRowsLimitDetails.SafeFormatError(p)
	if next == nil {
		next = d.CommonSQLEventDetails.SafeFormatError(p)
	}
	return next
}
