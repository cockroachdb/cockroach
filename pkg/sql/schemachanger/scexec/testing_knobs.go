// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package scexec

import "github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scplan"

// TestingKnobs are testing knobs which affect the running of declarative
// schema changes.
type TestingKnobs struct {
	// BeforeStage is called before ops passed to the executor are executed.
	// Errors returned are injected into the executor.
	BeforeStage func(p scplan.Plan, stageIdx int) error

	// AfterStage is invoked after all ops are executed.
	// Errors returned are injected into the executor.
	AfterStage func(p scplan.Plan, stageIdx int) error

	// BeforeWaitingForConcurrentSchemaChanges is called at the start of waiting
	// for concurrent schema changes to finish.
	BeforeWaitingForConcurrentSchemaChanges func(stmts []string) error

	// WhileWaitingForConcurrentSchemaChanges is called while waiting
	// for concurrent schema changes to finish.
	WhileWaitingForConcurrentSchemaChanges func(stmts []string)

	// OnPostCommitPlanError is called whenever the schema changer job returns an
	// error on building the state or on planning the stages.
	OnPostCommitPlanError func(err error) error

	// OnPostCommitError is called whenever the schema changer job returns an
	// error during stage execution.
	OnPostCommitError func(p scplan.Plan, stageIdx int, err error) error

	// RunBeforeBackfill is called just before starting the backfill.
	RunBeforeBackfill func() error

	// RunBeforeMakingPostCommitPlan is called just before making the post commit
	// plan.
	RunBeforeMakingPostCommitPlan func(inRollback bool) error
}

// ModuleTestingKnobs is part of the base.ModuleTestingKnobs interface.
func (*TestingKnobs) ModuleTestingKnobs() {}
