// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package schedulebase

import (
	"context"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/errors"
)

const (
	optFirstRun          = "first_run"
	optOnExecFailure     = "on_execution_failure"
	optOnPreviousRunning = "on_previous_running"
)

// OptionSetter specifies the KV option validation setting along with
// the function responsible for setting a value.
type OptionSetter struct {
	Expect sql.KVStringOptValidate
	Set    func(evalCtx *tree.EvalContext, v string) error
}

// ScheduleOptions is an interface describing set of schedule options.
type ScheduleOptions interface {
	// ExpectValues returns the list of valid options along with its setters.
	ExpectValues() map[string]OptionSetter
	// KVOptions serializes options back to its KVOptions representation
	KVOptions() (tree.KVOptions, error)
}

// CommonScheduleOptions is a ScheduleOptions common to most of the scheduled jobs.
type CommonScheduleOptions struct {
	// FirstRun specified when the schedule should execute for the first time.
	FirstRun time.Time
	// OnError specifies schedule behavior when job fails.
	OnError jobspb.ScheduleDetails_ErrorHandlingBehavior
	// Wait specifies the behavior when previous job started by this schedule still executes.
	Wait jobspb.ScheduleDetails_WaitBehavior
}

func setFirstRun(evalCtx *tree.EvalContext, v string, o *CommonScheduleOptions) error {
	firstRun, _, err := tree.ParseDTimestampTZ(evalCtx, v, time.Microsecond)
	if err != nil {
		return err
	}
	o.FirstRun = firstRun.Time
	return nil
}

func setOnError(v string, o *CommonScheduleOptions) error {
	switch onError := strings.ToLower(v); onError {
	case "retry":
		o.OnError = jobspb.ScheduleDetails_RETRY_SOON
	case "reschedule":
		o.OnError = jobspb.ScheduleDetails_RETRY_SCHED
	case "pause":
		o.OnError = jobspb.ScheduleDetails_PAUSE_SCHED
	default:
		return errors.Newf(
			"%q is not a valid on_execution_error; valid values are [retry|reschedule|pause]",
			onError)
	}
	return nil
}

func setWaitBehavior(v string, o *CommonScheduleOptions) error {
	switch wait := strings.ToLower(v); wait {
	case "start":
		o.Wait = jobspb.ScheduleDetails_NO_WAIT
	case "skip":
		o.Wait = jobspb.ScheduleDetails_SKIP
	case "wait":
		o.Wait = jobspb.ScheduleDetails_WAIT
	default:
		return errors.Newf(
			"%q is not a valid on_previous_running; valid values are [start|skip|wait]",
			wait)
	}
	return nil
}

// ExpectValues implements ScheduleOptions interface.
func (o *CommonScheduleOptions) ExpectValues() map[string]OptionSetter {
	return map[string]OptionSetter{
		optFirstRun: {
			Expect: sql.KVStringOptRequireValue,
			Set: func(evalCtx *tree.EvalContext, v string) error {
				return setFirstRun(evalCtx, v, o)
			},
		},

		optOnExecFailure: {
			Expect: sql.KVStringOptRequireValue,
			Set: func(evalCtx *tree.EvalContext, v string) error {
				return setOnError(v, o)
			},
		},

		optOnPreviousRunning: {
			Expect: sql.KVStringOptRequireValue,
			Set: func(evalCtx *tree.EvalContext, v string) error {
				return setWaitBehavior(v, o)
			},
		},
	}
}

var onErrorValueName = map[jobspb.ScheduleDetails_ErrorHandlingBehavior]string{
	jobspb.ScheduleDetails_RETRY_SCHED: "RESCHEDULE",
	jobspb.ScheduleDetails_RETRY_SOON:  "RETRY",
	jobspb.ScheduleDetails_PAUSE_SCHED: "PAUSE",
}

var onPreviousRunningName = map[jobspb.ScheduleDetails_WaitBehavior]string{
	jobspb.ScheduleDetails_WAIT:    "WAIT",
	jobspb.ScheduleDetails_NO_WAIT: "START",
	jobspb.ScheduleDetails_SKIP:    "SKIP",
}

// KVOptions implements ScheduleOptions interface
func (o *CommonScheduleOptions) KVOptions() (tree.KVOptions, error) {
	firstRun, err := tree.MakeDTimestampTZ(o.FirstRun, time.Microsecond)
	if err != nil {
		return nil, err
	}

	onError, ok := onErrorValueName[o.OnError]
	if !ok {
		return nil, errors.Newf("invalid OnError value %v", o.OnError)
	}

	wait, ok := onPreviousRunningName[o.Wait]
	if !ok {
		return nil, errors.Newf("invalid Wait value %v", o.Wait)
	}

	return tree.KVOptions{
		{Key: optFirstRun, Value: firstRun},
		{Key: optOnExecFailure, Value: tree.NewDString(onError)},
		{Key: optOnPreviousRunning, Value: tree.NewDString(wait)},
	}, nil
}

// MakeScheduleOptionsEval prepares KVOptions for evaluation.
// It returns a function, which when invoked parses and sets schedule options.
func MakeScheduleOptionsEval(
	ctx context.Context,
	p sql.PlanHookState,
	inputOptions tree.KVOptions,
	scheduleOptions ScheduleOptions,
) (func(evalCtx *tree.EvalContext) error, error) {
	expected := scheduleOptions.ExpectValues()
	kvExpect := make(map[string]sql.KVStringOptValidate, len(expected))
	for opt, expectation := range expected {
		kvExpect[opt] = expectation.Expect
	}
	evalFn, err := p.TypeAsStringOpts(ctx, inputOptions, kvExpect)

	if err != nil {
		return nil, err
	}

	setOptions := func(evalCtx *tree.EvalContext) error {
		opts, err := evalFn()
		if err != nil {
			return err
		}

		for k, v := range opts {
			if optSetter, ok := expected[k]; ok {
				if err := optSetter.Set(evalCtx, v); err != nil {
					return err
				}
			} else {
				return errors.Newf("unknown option %q", k)
			}
		}
		return nil
	}

	return setOptions, nil
}
