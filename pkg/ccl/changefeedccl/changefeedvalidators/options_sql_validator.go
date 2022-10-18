// Copyright 2022 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

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
