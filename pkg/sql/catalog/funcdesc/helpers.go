// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package funcdesc

import (
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil/unimplemented"
)

// VolatilityToProto converts sql statement input volatility to protobuf
// type.
func VolatilityToProto(v tree.FunctionVolatility) (catpb.Function_Volatility, error) {
	switch v {
	case tree.FunctionImmutable:
		return catpb.Function_IMMUTABLE, nil
	case tree.FunctionStable:
		return catpb.Function_STABLE, nil
	case tree.FunctionVolatile:
		return catpb.Function_VOLATILE, nil
	}

	return -1, pgerror.Newf(pgcode.InvalidParameterValue, "Unknown function volatility %q", v)
}

// NullInputBehaviorToProto converts sql statement input null input
// behavior to protobuf type.
func NullInputBehaviorToProto(
	v tree.FunctionNullInputBehavior,
) (catpb.Function_NullInputBehavior, error) {
	switch v {
	case tree.FunctionCalledOnNullInput:
		return catpb.Function_CALLED_ON_NULL_INPUT, nil
	case tree.FunctionReturnsNullOnNullInput:
		return catpb.Function_RETURNS_NULL_ON_NULL_INPUT, nil
	case tree.FunctionStrict:
		return catpb.Function_STRICT, nil
	}

	return -1, pgerror.Newf(pgcode.InvalidParameterValue, "Unknown function null input behavior %q", v)
}

// FunctionLangToProto converts sql statement input language to protobuf type.
func FunctionLangToProto(v tree.FunctionLanguage) (catpb.Function_Language, error) {
	switch v {
	case tree.FunctionLangSQL:
		return catpb.Function_SQL, nil
	case tree.FunctionLangPlPgSQL:
		return -1, unimplemented.NewWithIssue(91569, "PL/pgSQL is not yet supported")
	}

	return -1, pgerror.Newf(pgcode.UndefinedObject, "language %q does not exist", v)
}

// ParamClassToProto converts sql statement input argument class to protobuf
// type.
func ParamClassToProto(v tree.FuncParamClass) (catpb.Function_Param_Class, error) {
	switch v {
	case tree.FunctionParamIn:
		return catpb.Function_Param_IN, nil
	case tree.FunctionParamOut:
		return catpb.Function_Param_OUT, nil
	case tree.FunctionParamInOut:
		return catpb.Function_Param_IN_OUT, nil
	case tree.FunctionParamVariadic:
		return catpb.Function_Param_VARIADIC, nil
	}

	return -1, pgerror.Newf(pgcode.InvalidParameterValue, "unknown function parameter class %q", v)
}
