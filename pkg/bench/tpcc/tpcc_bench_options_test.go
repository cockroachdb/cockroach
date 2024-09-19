// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tpcc

import (
	"fmt"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
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
	argsGenerator serverArgs
	setupServer   []func(b testing.TB, s serverutils.TestServerInterface)
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

type serverArgs func(b testing.TB) (_ base.TestServerArgs, cleanup func())

func (s serverArgs) apply(cfg *benchmarkConfig) {
	cfg.argsGenerator = s
}

func (s serverArgs) String() string { return "generator" }

type setupStmtOption string

func (s setupStmtOption) apply(cfg *benchmarkConfig) {
	cfg.setupStmts = append(cfg.setupStmts, string(s))
}

func setupStmt(stmt string) option {
	return setupStmtOption(stmt)
}

var _ = setupStmt // silence unused linter

func (s setupStmtOption) String() string { return string(s) }

func setupServer(fn func(tb testing.TB, s serverutils.TestServerInterface)) option {
	return setupServerOption{fn}
}

type setupServerOption struct {
	fn func(tb testing.TB, s serverutils.TestServerInterface)
}

func (s setupServerOption) apply(cfg *benchmarkConfig) {
	cfg.setupServer = append(cfg.setupServer, s.fn)
}

func (s setupServerOption) String() string { return "setup server" }
