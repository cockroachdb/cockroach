// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package execinfrapb

// Prettier aliases for AggregatorSpec_Func values.
const (
	AnyNotNull    = AggregatorSpec_ANY_NOT_NULL
	Avg           = AggregatorSpec_AVG
	BoolAnd       = AggregatorSpec_BOOL_AND
	BoolOr        = AggregatorSpec_BOOL_OR
	ConcatAgg     = AggregatorSpec_CONCAT_AGG
	Count         = AggregatorSpec_COUNT
	Max           = AggregatorSpec_MAX
	Min           = AggregatorSpec_MIN
	Stddev        = AggregatorSpec_STDDEV
	Sum           = AggregatorSpec_SUM
	SumInt        = AggregatorSpec_SUM_INT
	Variance      = AggregatorSpec_VARIANCE
	XorAgg        = AggregatorSpec_XOR_AGG
	CountRows     = AggregatorSpec_COUNT_ROWS
	Sqrdiff       = AggregatorSpec_SQRDIFF
	FinalVariance = AggregatorSpec_FINAL_VARIANCE
	FinalStddev   = AggregatorSpec_FINAL_STDDEV
	ArrayAgg      = AggregatorSpec_ARRAY_AGG
	JSONAgg       = AggregatorSpec_JSON_AGG
	// JSONBAgg is an alias for JSONAgg, they do the same thing.
	JSONBAgg           = AggregatorSpec_JSONB_AGG
	StringAgg          = AggregatorSpec_STRING_AGG
	BitAnd             = AggregatorSpec_BIT_AND
	BitOr              = AggregatorSpec_BIT_OR
	Corr               = AggregatorSpec_CORR
	PercentileDiscImpl = AggregatorSpec_PERCENTILE_DISC_IMPL
	PercentileContImpl = AggregatorSpec_PERCENTILE_CONT_IMPL
	JSONObjectAgg      = AggregatorSpec_JSON_OBJECT_AGG
	JSONBObjectAgg     = AggregatorSpec_JSONB_OBJECT_AGG
	VarPop             = AggregatorSpec_VAR_POP
	StddevPop          = AggregatorSpec_STDDEV_POP
	StMakeline         = AggregatorSpec_ST_MAKELINE
	StExtent           = AggregatorSpec_ST_EXTENT
	StUnion            = AggregatorSpec_ST_UNION
	StCollect          = AggregatorSpec_ST_COLLECT
	CovarPop           = AggregatorSpec_COVAR_POP
	CovarSamp          = AggregatorSpec_COVAR_SAMP
	RegrIntercept      = AggregatorSpec_REGR_INTERCEPT
	RegrR2             = AggregatorSpec_REGR_R2
	RegrSlope          = AggregatorSpec_REGR_SLOPE
	RegrSxx            = AggregatorSpec_REGR_SXX
	RegrSyy            = AggregatorSpec_REGR_SYY
	RegrSxy            = AggregatorSpec_REGR_SXY
	RegrCount          = AggregatorSpec_REGR_COUNT
	RegrAvgx           = AggregatorSpec_REGR_AVGX
	RegrAvgy           = AggregatorSpec_REGR_AVGY
)
