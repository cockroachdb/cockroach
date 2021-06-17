// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

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
// source is. You must use this on the result of UNSAFEGET if you wish to store
// that result past the lifetime of the batch you UNSAFEGET'd from.
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
