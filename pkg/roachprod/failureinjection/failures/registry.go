// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package failures

import (
	"fmt"
	"regexp"
	"sync"

	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

// ClusterOptions represents options that can be passed to a
// Failer to describe how it should interact with the cluster.
// For now, this just denotes if the cluster is secure or not,
// but will be expanded on in the future to support multitenant clusters.
type ClusterOptions struct {
	secure bool
}

type failureSpec struct {
	makeFailureFunc func(clusterName string, l *logger.Logger, clusterOptions ClusterOptions) (FailureMode, error)
	args            FailureArgs
}
type FailureRegistry struct {
	syncutil.Mutex
	failures map[string]failureSpec
}

var (
	registry *FailureRegistry
	once     sync.Once
)

func GetFailureRegistry() *FailureRegistry {
	once.Do(func() {
		registry = &FailureRegistry{
			failures: make(map[string]failureSpec),
		}
		registry.register()
	})
	return registry
}

func (r *FailureRegistry) register() {
	registerCgroupDiskStall(r)
	registerDmsetupDiskStall(r)
	registerIPTablesPartitionFailure(r)
	registerNetworkLatencyFailure(r)
	registerResetVM(r)
	registerNoopFailure(r)
	registerProcessKillFailure(r)
}

func (r *FailureRegistry) add(
	failureName string,
	args FailureArgs,
	makeFailureFunc func(clusterName string, l *logger.Logger, clusterOpts ClusterOptions) (FailureMode, error),
) {
	r.Lock()
	defer r.Unlock()
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

func (r *FailureRegistry) GetFailer(
	clusterName, failureName string, l *logger.Logger, opts ...ClusterOptionFunc,
) (*Failer, error) {
	r.Lock()
	spec, ok := r.failures[failureName]
	r.Unlock()
	if !ok {
		return nil, fmt.Errorf("unknown failure %s", failureName)
	}

	clusterOpts := ClusterOptions{}
	for _, o := range opts {
		o(&clusterOpts)
	}
	failureMode, err := spec.makeFailureFunc(clusterName, l, clusterOpts)
	if err != nil {
		return nil, err
	}

	failer := &Failer{
		FailureMode: failureMode,
		state:       uninitialized,
	}
	return failer, nil
}
