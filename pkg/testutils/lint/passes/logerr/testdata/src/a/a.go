// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package a

import (
	"context"
	"errors"
	"log"
)

// LogAndSwallow - this should be flagged.
func LogAndSwallow(ctx context.Context) error {
	if err := doSomething(); err != nil { // want `error is logged but not returned`
		log.Printf("operation failed: %v", err)
	}
	return nil
}

// LogAndReturn - this should NOT be flagged.
func LogAndReturn(ctx context.Context) error {
	if err := doSomething(); err != nil {
		log.Printf("operation failed: %v", err)
		return err
	}
	return nil
}

// LogAndAssign - this should NOT be flagged.
func LogAndAssign(ctx context.Context) error {
	var lastErr error
	if err := doSomething(); err != nil {
		log.Printf("operation failed: %v", err)
		lastErr = err
	}
	return lastErr
}

// NoLogCall - this should NOT be flagged (no logging).
func NoLogCall(ctx context.Context) error {
	if err := doSomething(); err != nil {
		// Silent swallow - not our pattern
		_ = err
	}
	return nil
}

// NolintComment - this should NOT be flagged.
func NolintComment(ctx context.Context) error {
	//nolint:logerr
	if err := doSomething(); err != nil {
		log.Printf("operation failed: %v", err)
	}
	return nil
}

// MultipleConditions - this should be flagged.
func MultipleConditions(ctx context.Context) error {
	if err := doSomething(); err != nil || false { // want `error is logged but not returned`
		log.Printf("operation failed: %v", err)
	}
	return nil
}

// LogWithWarning - this should be flagged.
func LogWithWarning(ctx context.Context) error {
	if err := doSomething(); err != nil { // want `error is logged but not returned`
		log.Println("warning:", err)
	}
	return nil
}

// ProperReturnInNestedFunc - this should be flagged (return is in nested func).
func ProperReturnInNestedFunc(ctx context.Context) error {
	if err := doSomething(); err != nil { // want `error is logged but not returned`
		log.Printf("operation failed: %v", err)
		func() {
			return // This return doesn't count
		}()
	}
	return nil
}

// NamedReturn - this should NOT be flagged.
func NamedReturn(ctx context.Context) (err error) {
	if err := doSomething(); err != nil {
		log.Printf("operation failed: %v", err)
		return err
	}
	return nil
}

func doSomething() error {
	return errors.New("error")
}

var _ = context.Background
