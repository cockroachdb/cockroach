// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package funcinfo

import (
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/errors"
)

// VolatilityProperties are properties about the volatility of a user-defined
// function.
type VolatilityProperties struct {
	LeakProof  bool
	Volatility catpb.Function_Volatility
}

// MakeDefaultVolatilityProperties returns a VolatilityProperties with default
// property values (the values used when they are not specified when CREATE or
// REPLACE a function.
func MakeDefaultVolatilityProperties() VolatilityProperties {
	return VolatilityProperties{
		LeakProof:  catpb.DefaultFunctionLeakProof,
		Volatility: catpb.DefaultFunctionVolatility,
	}
}

// MakeVolatilityProperties returns a VolatilityProperties with the given
// property values.
func MakeVolatilityProperties(v catpb.Function_Volatility, l bool) VolatilityProperties {
	return VolatilityProperties{
		LeakProof:  l,
		Volatility: v,
	}
}

// Apply tries to find volatility relevant options and overwrite the current
// properties with them.
func (p *VolatilityProperties) Apply(options tree.FunctionOptions) error {
	for _, option := range options {
		switch t := option.(type) {
		case tree.FunctionVolatility:
			v, err := VolatilityToProto(t)
			if err != nil {
				return errors.WithAssertionFailure(err)
			}
			p.Volatility = v
		case tree.FunctionLeakproof:
			p.LeakProof = bool(t)
		}
	}
	return nil
}

// Validate returns an error if the properties values are not valid.
func (p *VolatilityProperties) Validate() error {
	if p.LeakProof && p.Volatility != catpb.Function_IMMUTABLE {
		return errors.Newf("leak proof function must be immutable, but got volatility: %s", p.Volatility)
	}
	return nil
}

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

	return -1, errors.AssertionFailedf("Unknown function volatility %q", v)
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

	return -1, errors.AssertionFailedf("Unknown function null input behavior %q", v)
}

// FunctionLangToProto converts sql statement input language to protobuf type.
func FunctionLangToProto(v tree.FunctionLanguage) (catpb.Function_Language, error) {
	switch v {
	case tree.FunctionLangSQL:
		return catpb.Function_SQL, nil
	case tree.FunctionLangPlPgSQL:
		return catpb.Function_PLPGSQL, nil
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

	return -1, errors.AssertionFailedf("unknown function parameter class %q", v)
}
