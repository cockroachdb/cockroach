// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package changefeedvalidators

import (
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/changefeedbase"
	"github.com/cockroachdb/cockroach/pkg/sql/exprutil"
)

func makeValMap(
	src map[string]changefeedbase.OptionPermittedValues,
) map[string]exprutil.KVStringOptValidate {
	dst := make(map[string]exprutil.KVStringOptValidate, len(src))
	for k, v := range src {
		if v.CanBeEmpty {
			dst[k] = exprutil.KVStringOptAny
		} else if v.Type == changefeedbase.OptionTypeFlag {
			dst[k] = exprutil.KVStringOptRequireNoValue
		} else {
			dst[k] = exprutil.KVStringOptRequireValue
		}
	}
	return dst
}

// CreateOptionValidations do a basic check on the WITH options in a CREATE CHANGEFEED.
var CreateOptionValidations = makeValMap(changefeedbase.ChangefeedOptionExpectValues)

// AlterOptionValidations do a basic check on the options in an ALTER CHANGEFEED.
var AlterOptionValidations = makeValMap(changefeedbase.AlterChangefeedOptionExpectValues)

// AlterTargetOptionValidations do a basic check on the target-level options in an ALTER CHANGEFEED.
var AlterTargetOptionValidations = makeValMap(changefeedbase.AlterChangefeedTargetOptions)
