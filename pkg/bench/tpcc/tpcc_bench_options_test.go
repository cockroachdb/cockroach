// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tpcc

import (
	"fmt"
	"strings"
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
