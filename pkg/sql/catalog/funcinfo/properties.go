// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package funcinfo

import (
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil/unimplemented"
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
func (p *VolatilityProperties) Apply(options tree.RoutineOptions) error {
	for _, option := range options {
		switch t := option.(type) {
		case tree.RoutineVolatility:
			v, err := VolatilityToProto(t)
			if err != nil {
				return errors.WithAssertionFailure(err)
			}
			p.Volatility = v
		case tree.RoutineLeakproof:
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
func VolatilityToProto(v tree.RoutineVolatility) (catpb.Function_Volatility, error) {
	switch v {
	case tree.RoutineImmutable:
		return catpb.Function_IMMUTABLE, nil
	case tree.RoutineStable:
		return catpb.Function_STABLE, nil
	case tree.RoutineVolatile:
		return catpb.Function_VOLATILE, nil
	}

	return -1, errors.AssertionFailedf("Unknown function volatility %q", v)
}

// NullInputBehaviorToProto converts sql statement input null input
// behavior to protobuf type.
func NullInputBehaviorToProto(
	v tree.RoutineNullInputBehavior,
) (catpb.Function_NullInputBehavior, error) {
	switch v {
	case tree.RoutineCalledOnNullInput:
		return catpb.Function_CALLED_ON_NULL_INPUT, nil
	case tree.RoutineReturnsNullOnNullInput:
		return catpb.Function_RETURNS_NULL_ON_NULL_INPUT, nil
	case tree.RoutineStrict:
		return catpb.Function_STRICT, nil
	}

	return -1, errors.AssertionFailedf("Unknown function null input behavior %q", v)
}

// FunctionLangToProto converts sql statement input language to protobuf type.
func FunctionLangToProto(v tree.RoutineLanguage) (catpb.Function_Language, error) {
	switch v {
	case tree.RoutineLangSQL:
		return catpb.Function_SQL, nil
	case tree.RoutineLangPLpgSQL:
		return catpb.Function_PLPGSQL, nil
	case tree.RoutineLangC:
		return -1, unimplemented.NewWithIssue(102201, "C is not yet supported")
	}

	return -1, pgerror.Newf(pgcode.UndefinedObject, "language %q does not exist", v)
}

// ParamClassToProto converts sql statement input argument class to protobuf
// type.
func ParamClassToProto(v tree.RoutineParamClass) (catpb.Function_Param_Class, error) {
	switch v {
	case tree.RoutineParamDefault:
		return catpb.Function_Param_DEFAULT, nil
	case tree.RoutineParamIn:
		return catpb.Function_Param_IN, nil
	case tree.RoutineParamOut:
		return catpb.Function_Param_OUT, nil
	case tree.RoutineParamInOut:
		return catpb.Function_Param_IN_OUT, nil
	case tree.RoutineParamVariadic:
		return catpb.Function_Param_VARIADIC, nil
	}

	return -1, errors.AssertionFailedf("unknown function parameter class %q", v)
}

// SecurityToProto converts sql statement input security to protobuf type.
func SecurityToProto(v tree.RoutineSecurity) (catpb.Function_Security, error) {
	switch v {
	case tree.RoutineInvoker:
		return catpb.Function_INVOKER, nil
	case tree.RoutineDefiner:
		return catpb.Function_DEFINER, nil
	}
	return -1, errors.AssertionFailedf("unknown function security class %q", v)
}
