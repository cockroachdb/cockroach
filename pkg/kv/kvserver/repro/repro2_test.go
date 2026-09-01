// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package repro

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestRepro2(t *testing.T) {
	var s struct {
		Step1 Node
		Step2 Node
		Step3 NodeT[struct {
			a int
			b int
		}]
		Step4 Node
	}
	Struct(&s)

	go func() {
		time.Sleep(100 * time.Millisecond)
		s.Step1.Once()
		if step, d := &s.Step3, s.Step3.Data(); step.Gate() {
			d.a = 10
			d.b = 20
			step.Step()
		}
	}()

	s.Step2.Once()

	if step := &s.Step4; step.Gate() {
		require.Equal(t, 10, s.Step3.Data().a)
		require.Equal(t, 20, s.Step3.Data().b)
		step.Step()
	}
}

func TestRepro2NoOp(t *testing.T) {
	var s struct{ Step Node }
	Struct(&s)
	makeNoOp(&s)

	require.False(t, s.Step.IsNext())
	require.False(t, s.Step.Gate())
	require.False(t, s.Step.Once())
	require.Panics(t, func() {
		s.Step.Step()
	})
}
