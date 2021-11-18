// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package scerrors

import (
	"fmt"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/errors"
)

type notImplementedError struct {
	n      tree.NodeFormatter
	detail string
}

// TODO(ajwerner): Deal with redaction.

var _ error = (*notImplementedError)(nil)

// HasNotImplemented returns true if the error indicates that the builder does
// not support the provided statement.
func HasNotImplemented(err error) bool {
	return errors.HasType(err, (*notImplementedError)(nil))
}

func (e *notImplementedError) Error() string {
	var buf strings.Builder
	fmt.Fprintf(&buf, "%T not implemented in the new schema changer", e.n)
	if e.detail != "" {
		fmt.Fprintf(&buf, ": %s", e.detail)
	}
	return buf.String()
}

// NotImplementedError returns an error for which HasNotImplemented would
// return true.
func NotImplementedError(n tree.NodeFormatter) error {
	return &notImplementedError{n: n}
}

// NotImplementedErrorf returns an error for which HasNotImplemented would
// return true.
func NotImplementedErrorf(n tree.NodeFormatter, fmtstr string, args ...interface{}) error {
	return &notImplementedError{n: n, detail: fmt.Sprintf(fmtstr, args...)}
}

// concurrentSchemaChangeError indicates that building the schema change plan
// is not currently possible because there are other concurrent schema changes
// on one of the descriptors.
type concurrentSchemaChangeError struct {
	// TODO(ajwerner): Instead of waiting for one descriptor at a time, we should
	// get all the IDs of the descriptors we might be waiting for and return them
	// from the builder.
	descID descpb.ID
}

func (e *concurrentSchemaChangeError) Error() string {
	return fmt.Sprintf("descriptor %d is undergoing another schema change", e.descID)
}

// ConcurrentSchemaChangeDescID returns the ID of the descriptor which is
// undergoing a concurrent schema change, according to err, if applicable.
func ConcurrentSchemaChangeDescID(err error) descpb.ID {
	cscErr := (*concurrentSchemaChangeError)(nil)
	if !errors.As(err, &cscErr) {
		return descpb.InvalidID
	}
	return cscErr.descID
}

// ConcurrentSchemaChangeError returns a concurrent schema change error for the
// given table.
func ConcurrentSchemaChangeError(table catalog.TableDescriptor) error {
	return &concurrentSchemaChangeError{descID: table.GetID()}
}
