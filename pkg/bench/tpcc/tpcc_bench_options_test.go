// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tpcc

import (
	"fmt"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
)

type option interface {
	fmt.Stringer
	apply(*benchmarkConfig)
}

type options []option

func (o options) String() string {
	var buf strings.Builder
	for i, opt := range o {
		if i > 0 {
			buf.WriteString(";")
		}
		buf.WriteString(opt.String())
	}
	return buf.String()
}

func (o options) apply(cfg *benchmarkConfig) {
	for _, opt := range o {
		opt.apply(cfg)
	}
}

type benchmarkConfig struct {
	workloadFlags []string
	argsGenerator argGeneratorFunc
	setupStmts    []string
}

type workloadFlagOption struct{ name, value string }

func (w workloadFlagOption) apply(cfg *benchmarkConfig) {
	cfg.workloadFlags = append(cfg.workloadFlags, "--"+w.name, w.value)
}

func (w workloadFlagOption) String() string {
	return fmt.Sprintf("%s=%s", w.name, w.value)
}

func workloadFlag(name, value string) option {
	return workloadFlagOption{name: name, value: value}
}

type argGeneratorFunc func(b testing.TB) (_ base.TestServerArgs, cleanup func())

type funcOption struct {
	name string
	f    func(*benchmarkConfig)
}

func mkOption(name string, f func(*benchmarkConfig)) option {
	return funcOption{name: name, f: f}
}

func (f funcOption) String() string             { return f.name }
func (f funcOption) apply(cfg *benchmarkConfig) { f.f(cfg) }

var _ option = funcOption{}
