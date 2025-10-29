// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package op

import (
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/types"
	"github.com/cockroachdb/errors"
)

// ControlledOperation is an operation that is being managed by the Controller.
// It maintains a set of methods to inspect the lifecycle of an operation.
type ControlledOperation interface {
	// Next returns the next time that this operation should be checked.
	Next() types.Tick
	// Done returns whether this controlled operation is done, if so, when the
	// operation finished.
	Done() (bool, types.Tick)
	// Errors returns any errors that were recorded for this operation, if any
	// exist, otherwise nil.
	Errors() error
}

type baseOp struct {
	start, next, complete types.Tick
	done                  bool
	errs                  []error
}

func newBaseOp(tick types.Tick) baseOp {
	return baseOp{
		start:    tick,
		next:     tick,
		complete: types.Tick{},
		errs:     []error{},
	}
}

// Next returns the next time that this operation should be checked.
func (bo baseOp) Next() types.Tick {
	return bo.next
}

// Done returns whether this controlled operation is done, if so, when the
// operation finished.
func (bo baseOp) Done() (bool, types.Tick) {
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
