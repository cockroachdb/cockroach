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

// Sink represents a data sink for testing.
type Sink struct{}

func (s *Sink) Flush(ctx context.Context) error {
	return nil
}

func (s *Sink) Close() error {
	return nil
}

// SwallowedErrorInDefer - this should be flagged.
func SwallowedErrorInDefer(ctx context.Context) {
	sink := &Sink{}
	defer func() {
		if err := sink.Flush(ctx); err != nil { // want `error in defer block is logged but not propagated`
			log.Printf("failed to flush: %v", err)
		}
	}()
}

// SwallowedErrorWithWarning - this should be flagged.
func SwallowedErrorWithWarning(ctx context.Context) {
	sink := &Sink{}
	defer func() {
		if err := sink.Close(); err != nil { // want `error in defer block is logged but not propagated`
			log.Println("failed to close:", err)
		}
	}()
}

// ProperlyHandledWithPanic - this should NOT be flagged.
func ProperlyHandledWithPanic(ctx context.Context) {
	sink := &Sink{}
	defer func() {
		if err := sink.Flush(ctx); err != nil {
			log.Printf("failed to flush: %v", err)
			panic(err)
		}
	}()
}

// ProperlyHandledWithNamedReturn - this should NOT be flagged.
func ProperlyHandledWithNamedReturn(ctx context.Context) (err error) {
	sink := &Sink{}
	defer func() {
		if flushErr := sink.Flush(ctx); flushErr != nil {
			log.Printf("failed to flush: %v", flushErr)
			err = flushErr
		}
	}()
	return nil
}

// NoLogCall - this should NOT be flagged (no logging).
func NoLogCall(ctx context.Context) {
	sink := &Sink{}
	defer func() {
		if err := sink.Flush(ctx); err != nil {
			// Silent ignore - not our pattern
			_ = err
		}
	}()
}

// NolintComment - this should NOT be flagged.
func NolintComment(ctx context.Context) {
	sink := &Sink{}
	defer func() {
		//nolint:defererr
		if err := sink.Flush(ctx); err != nil {
			log.Printf("failed to flush: %v", err)
		}
	}()
}

// AssignsToOuterVariable - this should NOT be flagged.
func AssignsToOuterVariable(ctx context.Context) error {
	sink := &Sink{}
	var outerErr error
	defer func() {
		if err := sink.Flush(ctx); err != nil {
			log.Printf("failed to flush: %v", err)
			outerErr = err
		}
	}()
	return outerErr
}

// MultipleConditions - this should be flagged.
func MultipleConditions(ctx context.Context) {
	sink := &Sink{}
	defer func() {
		if err := sink.Flush(ctx); err != nil || false { // want `error in defer block is logged but not propagated`
			log.Printf("failed to flush: %v", err)
		}
	}()
}

// NonFuncLitDefer - this should NOT be flagged (not a func literal).
func NonFuncLitDefer(ctx context.Context) {
	sink := &Sink{}
	defer sink.Close()
}

// ErrorWithSimpleLog - this should be flagged.
func ErrorWithSimpleLog(ctx context.Context) {
	sink := &Sink{}
	defer func() {
		if err := sink.Close(); err != nil { // want `error in defer block is logged but not propagated`
			log.Print(err)
		}
	}()
}

// DeferWithErrors - helper type alias to avoid import errors.
var _ = errors.New
var _ = context.Background
