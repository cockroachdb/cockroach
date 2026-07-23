// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package eventpb

import (
	"fmt"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
)

var _ error = &CommonLargeRowDetails{}
var _ fmt.Formatter = &CommonLargeRowDetails{}
var _ errors.SafeFormatter = &CommonLargeRowDetails{}

// Error is part of the error interface, which CommonLargeRowDetails implements.
func (r *CommonLargeRowDetails) Error() string {
	return fmt.Sprintf(
		"row larger than max row size: table %v family %v primary key %v size %v",
		r.TableID, r.FamilyID, r.PrimaryKey, r.RowSize,
	)
}

// Format is part of the fmt.Formatter interface, which CommonLargeRowDetails
// implements.
func (r *CommonLargeRowDetails) Format(s fmt.State, verb rune) { errors.FormatError(r, s, verb) }

// SafeFormatError is part of the errors.SafeFormatter interface, which
// CommonLargeRowDetails implements.
func (r *CommonLargeRowDetails) SafeFormatError(p errors.Printer) (next error) {
	p.Printf(
		"row larger than max row size: table %v family %v size %v",
		r.TableID, r.FamilyID, r.RowSize,
	)
	return nil
}

// Error helps other structs embedding CommonTxnRowsLimitDetails implement the
// error interface. (Note that it does not implement the error interface
// itself.)
func (d *CommonTxnRowsLimitDetails) Error(kind string) string {
	return fmt.Sprintf(
		"txn has %s %d rows, which is above the limit: TxnID %v SessionID %v",
		kind, d.NumRows, d.TxnID, d.SessionID,
	)
}

// SafeFormatError helps other structs embedding CommonTxnRowsLimitDetails
// implement the errors.SafeFormatter interface. (Note that it does not
// implement the errors.SafeFormatter interface itself.)
func (d *CommonTxnRowsLimitDetails) SafeFormatError(p errors.Printer, kind string) (next error) {
	p.Printf(
		"txn has %s %d rows, which is above the limit: TxnID %v SessionID %v",
		kind, d.NumRows, redact.SafeString(d.TxnID), redact.SafeString(d.SessionID),
	)
	return nil
}
