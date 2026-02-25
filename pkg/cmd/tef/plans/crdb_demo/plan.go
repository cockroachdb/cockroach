// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Package crdb_demo provides a demonstration plan for the Task Execution Framework (TEF)
// in the CockroachDB repository. This plan showcases all task types with simulated operations.
package crdb_demo

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/tef/planners"
	"github.com/spf13/cobra"
)

const crdbDemoPlanName = "crdb_demo"

// CRDBDemo implements the Registry interface for the CockroachDB demo plan.
type CRDBDemo struct{}

// RegisterCRDBDemoPlans registers the CockroachDB demo plan with the plan registry.
func RegisterCRDBDemoPlans(pr *planners.PlanRegistry) {
	pr.Register(&CRDBDemo{})
}

var _ planners.Registry = &CRDBDemo{}

// PrepareExecution initializes any resources needed for plan execution.
func (d *CRDBDemo) PrepareExecution(_ context.Context) error {
	// No special initialization needed for this demo
	return nil
}

// GetPlanName returns the unique identifier for this plan.
func (d *CRDBDemo) GetPlanName() string {
	return crdbDemoPlanName
}

// GetPlanDescription returns a human-readable description of what this plan does.
func (d *CRDBDemo) GetPlanDescription() string {
	return "CockroachDB demo plan showcasing all task types with simulated operations"
}

// GetPlanVersion returns the current version of the workflow definition.
func (d *CRDBDemo) GetPlanVersion() int {
	return 1
}

// crdbDemoInput defines the input structure for the demo plan.
type crdbDemoInput struct {
	Name    string `json:"name,omitempty"`
	Delay   int    `json:"delay,omitempty"`    // delay in seconds for operations
	FailPct int    `json:"fail_pct,omitempty"` // percentage chance of random failures (0-100)
}

// ParsePlanInput parses and validates the user-provided input string.
func (d *CRDBDemo) ParsePlanInput(input string) (interface{}, error) {
	data := &crdbDemoInput{}
	if err := json.Unmarshal([]byte(input), data); err != nil {
		return nil, err
	}
	// Set defaults
	if data.Delay == 0 {
		data.Delay = 1
	}
	return data, nil
}

// AddStartWorkerCmdFlags allows the plan to add custom flags to worker commands.
func (d *CRDBDemo) AddStartWorkerCmdFlags(_ *cobra.Command) {
	// No custom flags needed for this demo
}

// GeneratePlan constructs the task execution graph for the CockroachDB demo plan.
func (d *CRDBDemo) GeneratePlan(ctx context.Context, p planners.Planner) {
	d.registerExecutors(ctx, p)

	// Task 1: Initialize environment
	initTask := p.NewExecutionTask(ctx, "initialize")
	initTask.ExecutorFn = initialize
	initTask.GetFailTask()

	// Task 2: Condition - Check if we should proceed with setup
	checkTask := p.NewConditionTask(ctx, "check prerequisites")
	checkTask.ExecutorFn = checkPrerequisites

	// Task 3a: Setup (taken if prerequisites are met)
	setupTask := p.NewExecutionTask(ctx, "setup")
	setupTask.ExecutorFn = performSetup

	// Task 3b: Skip setup (taken if prerequisites not met)
	skipTask := p.NewExecutionTask(ctx, "skip setup")
	skipTask.ExecutorFn = skipSetup

	// Task 4: Fork - Run parallel operations
	forkTask := p.NewForkTask(ctx, "parallel operations")
	forkJoinTask := p.NewForkJoinTask(ctx, "join parallel operations")

	// Fork Branch 1: Process data
	processTask := p.NewExecutionTask(ctx, "process data")
	processTask.ExecutorFn = processData

	// Fork Branch 2: Validate data
	validateTask := p.NewExecutionTask(ctx, "validate data")
	validateTask.ExecutorFn = validateData

	// Fork Branch 3: Transform data
	transformTask := p.NewExecutionTask(ctx, "transform data")
	transformTask.ExecutorFn = transformData

	// Task 5: Finalize
	finalizeTask := p.NewExecutionTask(ctx, "finalize")
	finalizeTask.ExecutorFn = finalize

	// End task
	endTask := p.NewEndTask(ctx, "end")

	// Build the task graph
	initTask.Next = checkTask

	// Condition branches
	checkTask.Then = setupTask
	checkTask.Else = skipTask

	// Both branches lead to fork
	setupTask.Next = forkTask
	skipTask.Next = forkTask

	// Fork branches
	processTask.Next = forkJoinTask
	validateTask.Next = forkJoinTask
	transformTask.Next = forkJoinTask
	forkTask.Tasks = []planners.Task{processTask, validateTask, transformTask}
	forkTask.Join = forkJoinTask

	// Fork continues to finalize
	forkTask.Next = finalizeTask

	// Finalize leads to end
	finalizeTask.Next = endTask

	// Register the plan
	p.RegisterPlan(ctx, initTask, finalizeTask)
}

// registerExecutors registers all executor functions with the planner.
func (d *CRDBDemo) registerExecutors(ctx context.Context, p planners.Planner) {
	p.RegisterExecutor(ctx, &planners.Executor{
		Name:        "initialize",
		Description: "Initialize the demo environment",
		Func:        initialize,
		Idempotent:  true,
	})

	p.RegisterExecutor(ctx, &planners.Executor{
		Name:        "checkPrerequisites",
		Description: "Check if prerequisites are met",
		Func:        checkPrerequisites,
		Idempotent:  true,
	})

	p.RegisterExecutor(ctx, &planners.Executor{
		Name:        "performSetup",
		Description: "Perform environment setup",
		Func:        performSetup,
		Idempotent:  true,
	})

	p.RegisterExecutor(ctx, &planners.Executor{
		Name:        "skipSetup",
		Description: "Skip setup",
		Func:        skipSetup,
		Idempotent:  true,
	})

	p.RegisterExecutor(ctx, &planners.Executor{
		Name:        "processData",
		Description: "Process data",
		Func:        processData,
		Idempotent:  true,
	})

	p.RegisterExecutor(ctx, &planners.Executor{
		Name:        "validateData",
		Description: "Validate data",
		Func:        validateData,
		Idempotent:  true,
	})

	p.RegisterExecutor(ctx, &planners.Executor{
		Name:        "transformData",
		Description: "Transform data",
		Func:        transformData,
		Idempotent:  true,
	})

	p.RegisterExecutor(ctx, &planners.Executor{
		Name:        "finalize",
		Description: "Finalize the workflow",
		Func:        finalize,
		Idempotent:  true,
	})
}

// Executor functions

func initialize(
	ctx context.Context, _ *planners.PlanExecutionInfo, demoInput *crdbDemoInput,
) (map[string]interface{}, error) {
	logger := planners.LoggerFromContext(ctx)
	logger.Info("Initializing environment", "name", demoInput.Name)

	time.Sleep(time.Duration(demoInput.Delay) * time.Second)

	if shouldFail(demoInput.FailPct) {
		return nil, fmt.Errorf("initialization failed (simulated failure)")
	}

	result := map[string]interface{}{
		"status":      "initialized",
		"name":        demoInput.Name,
		"initialized": true,
	}
	logger.Info("Environment initialized successfully")
	return result, nil
}

func checkPrerequisites(
	ctx context.Context, _ *planners.PlanExecutionInfo, demoInput *crdbDemoInput,
) (bool, error) {
	logger := planners.LoggerFromContext(ctx)

	time.Sleep(time.Duration(demoInput.Delay) * time.Second)

	// Randomly decide if prerequisites are met (70% chance)
	met := rand.Intn(100) < 70
	logger.Info("Prerequisites check", "met", met)
	return met, nil
}

func performSetup(
	ctx context.Context, _ *planners.PlanExecutionInfo, demoInput *crdbDemoInput,
) (map[string]interface{}, error) {
	logger := planners.LoggerFromContext(ctx)
	logger.Info("Performing setup")

	time.Sleep(time.Duration(demoInput.Delay) * time.Second)

	if shouldFail(demoInput.FailPct) {
		return nil, fmt.Errorf("setup failed (simulated failure)")
	}

	logger.Info("Setup completed successfully")
	return map[string]interface{}{"setup": "complete"}, nil
}

func skipSetup(
	ctx context.Context, _ *planners.PlanExecutionInfo, _ interface{},
) (map[string]interface{}, error) {
	logger := planners.LoggerFromContext(ctx)
	logger.Info("Skipping setup (prerequisites not met)")
	return map[string]interface{}{"setup": "skipped"}, nil
}

func processData(
	ctx context.Context, _ *planners.PlanExecutionInfo, demoInput *crdbDemoInput,
) (map[string]interface{}, error) {
	logger := planners.LoggerFromContext(ctx)
	logger.Info("Processing data")

	time.Sleep(time.Duration(demoInput.Delay) * time.Second)

	if shouldFail(demoInput.FailPct) {
		return nil, fmt.Errorf("data processing failed (simulated failure)")
	}

	logger.Info("Data processed successfully")
	return map[string]interface{}{"processed": true, "records": 1000}, nil
}

func validateData(
	ctx context.Context, _ *planners.PlanExecutionInfo, demoInput *crdbDemoInput,
) (map[string]interface{}, error) {
	logger := planners.LoggerFromContext(ctx)
	logger.Info("Validating data")

	time.Sleep(time.Duration(demoInput.Delay) * time.Second)

	if shouldFail(demoInput.FailPct) {
		return nil, fmt.Errorf("data validation failed (simulated failure)")
	}

	logger.Info("Data validated successfully")
	return map[string]interface{}{"valid": true}, nil
}

func transformData(
	ctx context.Context, _ *planners.PlanExecutionInfo, demoInput *crdbDemoInput,
) (map[string]interface{}, error) {
	logger := planners.LoggerFromContext(ctx)
	logger.Info("Transforming data")

	time.Sleep(time.Duration(demoInput.Delay) * time.Second)

	if shouldFail(demoInput.FailPct) {
		return nil, fmt.Errorf("data transformation failed (simulated failure)")
	}

	logger.Info("Data transformed successfully")
	return map[string]interface{}{"transformed": true}, nil
}

func finalize(
	ctx context.Context, _ *planners.PlanExecutionInfo, demoInput *crdbDemoInput,
) (map[string]interface{}, error) {
	logger := planners.LoggerFromContext(ctx)
	logger.Info("Finalizing workflow")

	time.Sleep(time.Duration(demoInput.Delay) * time.Second)

	if shouldFail(demoInput.FailPct) {
		return nil, fmt.Errorf("finalization failed (simulated failure)")
	}

	result := map[string]interface{}{
		"status":    "completed",
		"name":      demoInput.Name,
		"completed": true,
	}
	logger.Info("Workflow finalized successfully")
	return result, nil
}

// shouldFail returns true with the given probability (0-100).
func shouldFail(failPct int) bool {
	if failPct <= 0 {
		return false
	}
	if failPct >= 100 {
		return true
	}
	return rand.Intn(100) < failPct
}
