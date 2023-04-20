// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package op

import (
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/errors"
)

// ControlledOperation is an operation that is being managed by the Controller.
// It maintains a set of methods to inspect the lifecycle of an operation.
type ControlledOperation interface {
	// Next returns the next time that this operation should be checked.
	Next() hlc.Timestamp
	// Done returns whether this controlled operation is done, if so, when the
	// operation finished.
	Done() (bool, hlc.Timestamp)
	// Errors returns any errors that were recorded for this operation, if any
	// exist, otherwise nil.
	Errors() error
}

type baseOp struct {
	start, next, complete hlc.Timestamp
	done                  bool
	errs                  []error
}

func newBaseOp(tick hlc.Timestamp) baseOp {
	return baseOp{
		start:    tick,
		next:     tick,
		complete: hlc.Timestamp{},
		errs:     []error{},
	}
}

// Next returns the next time that this operation should be checked.
func (bo baseOp) Next() hlc.Timestamp {
	return bo.next
}

// Done returns whether this controlled operation is done, if so, when the
// operation finished.
func (bo baseOp) Done() (bool, hlc.Timestamp) {
	return bo.done, bo.complete
}

// Errors returns any errors that were recorded for this operation, if any
// exist, otherwise nil.
func (bo baseOp) Errors() error {
	var err error
	for i := range bo.errs {
		if i == 0 {
			err = bo.errs[i]
		} else {
			err = errors.CombineErrors(err, bo.errs[i])
		}
	}
	return err
}
