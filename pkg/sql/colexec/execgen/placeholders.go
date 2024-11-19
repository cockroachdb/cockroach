// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package execgen

import (
	"github.com/cockroachdb/cockroach/pkg/sql/colexecerror"
	"github.com/cockroachdb/errors"
)

const nonTemplatePanic = "do not call from non-template code"

// Remove unused warnings.
var (
	_ = COPYVAL
	_ = APPENDSLICE
	_ = APPENDVAL
	_ = SETVARIABLESIZE
)

// COPYVAL is a template function that can be used to set a scalar to the value
// of another scalar in such a way that the destination won't be modified if the
// source is.
func COPYVAL(dest, src interface{}) {
	colexecerror.InternalError(errors.AssertionFailedf(nonTemplatePanic))
}

// APPENDSLICE is a template function.
func APPENDSLICE(target, src, destIdx, srcStartIdx, srcEndIdx interface{}) {
	colexecerror.InternalError(errors.AssertionFailedf(nonTemplatePanic))
}

// APPENDVAL is a template function.
func APPENDVAL(target, v interface{}) {
	colexecerror.InternalError(errors.AssertionFailedf(nonTemplatePanic))
}

// SETVARIABLESIZE is a template function.
func SETVARIABLESIZE(target, value interface{}) interface{} {
	colexecerror.InternalError(errors.AssertionFailedf(nonTemplatePanic))
	return nil
}
