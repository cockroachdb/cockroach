// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package failures

import (
	"fmt"
	"regexp"

	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
)

type failureSpec struct {
	makeFailureFunc func(clusterName string, l *logger.Logger, secure bool) (FailureMode, error)
	args            FailureArgs
}
type FailureRegistry struct {
	failures map[string]failureSpec
}

func NewFailureRegistry() *FailureRegistry {
	return &FailureRegistry{
		failures: make(map[string]failureSpec),
	}
}

func (r *FailureRegistry) Register() {
	registerIPTablesPartitionFailure(r)
}

func (r *FailureRegistry) add(
	failureName string,
	args FailureArgs,
	makeFailureFunc func(clusterName string, l *logger.Logger, secure bool) (FailureMode, error),
) {
	if _, ok := r.failures[failureName]; ok {
		panic(fmt.Sprintf("failure %s already exists", failureName))
	}
	r.failures[failureName] = failureSpec{
		makeFailureFunc: makeFailureFunc,
		args:            args,
	}
}

func (r *FailureRegistry) List(regex string) []string {
	var filter *regexp.Regexp
	if regex == "" {
		filter = regexp.MustCompile(`.`)
	} else {
		filter = regexp.MustCompile(regex)
	}

	var matches []string
	for name := range r.failures {
		if filter.MatchString(name) {
			matches = append(matches, name)
		}
	}
	return matches
}

func (r *FailureRegistry) GetFailureMode(
	clusterName, failureName string, l *logger.Logger, secure bool,
) (FailureMode, error) {
	spec, ok := r.failures[failureName]
	if !ok {
		return nil, fmt.Errorf("unknown failure %s", failureName)
	}
	return spec.makeFailureFunc(clusterName, l, secure)
}
