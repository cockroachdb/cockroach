// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package operation

import "github.com/cockroachdb/cockroach/pkg/roachprod/logger"

type Operation interface {
	Cockroach() string
	Name() string
	// Spec returns the *registry.OperationSpec as an interface{}.
	Spec() interface{}
	Skip(args ...interface{})
	Skipf(format string, args ...interface{})
	Error(args ...interface{})
	Errorf(string, ...interface{})
	FailNow()
	Fatal(args ...interface{})
	Fatalf(format string, args ...interface{})
	Failed() bool

	ArtifactsDir() string

	L() *logger.Logger
	Progress(float64)
	Status(args ...interface{})

	GetCleanupState(string) string
	SetCleanupState(string, string)
}
