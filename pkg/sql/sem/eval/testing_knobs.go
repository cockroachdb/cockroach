// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package eval

import (
	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
)

// TestingKnobs contains test knobs.
type TestingKnobs struct {
	// AssertFuncExprReturnTypes indicates whether FuncExpr evaluations
	// should assert that the returned Datum matches the expected
	// ReturnType of the function.
	AssertFuncExprReturnTypes bool
	// AssertUnaryExprReturnTypes indicates whether UnaryExpr evaluations
	// should assert that the returned Datum matches the expected
	// ReturnType of the function.
	AssertUnaryExprReturnTypes bool
	// AssertBinaryExprReturnTypes indicates whether BinaryExpr
	// evaluations should assert that the returned Datum matches the
	// expected ReturnType of the function.
	AssertBinaryExprReturnTypes bool
	// DisableOptimizerRuleProbability is the probability that any given
	// transformation rule in the optimizer is disabled.
	DisableOptimizerRuleProbability float64
	// OptimizerCostPerturbation is used to randomly perturb the estimated
	// cost of each expression in the query tree for the purpose of creating
	// alternate query plans in the optimizer.
	OptimizerCostPerturbation float64
	// If set, mutations.MaxBatchSize, row.getKVBatchSize, and other values
	// randomized via the metamorphic testing will be overridden to use the
	// production value.
	ForceProductionValues bool

	CallbackGenerators map[string]*CallbackValueGenerator

	// We use clusterversion.Key rather than a roachpb.Version because it will be used
	// to get initial values to use during bootstrap.
	TenantLogicalVersionKeyOverride clusterversion.Key

	// This knob needs to be used in the following ways:
	// 1. In tests, there should be a way to override the allow unsafe variable.
	// 2. If a test is not setup with a value, true should be the default.
	// 3. There should be a way to setup a test so that there is no override.
	//
	// Conditions 2 and 3 remove the possibility for just a pointer to a boolean.
	// If the value were nil, should the system set the override to true, or leave it nil?
	// To fix the ambiguity, a function which returns nil can be used to indicate no override.
	UnsafeOverride func() *bool
}

var _ base.ModuleTestingKnobs = &TestingKnobs{}

// ModuleTestingKnobs is part of the base.ModuleTestingKnobs interface.
func (*TestingKnobs) ModuleTestingKnobs() {}
