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

// DB simulates a database with transaction support.
type DB struct{}

// Txn simulates a transaction method.
func (db *DB) Txn(ctx context.Context, fn func(ctx context.Context) error) error {
	return fn(ctx)
}

// SwallowedErrorInTxn - this should be flagged.
func SwallowedErrorInTxn(ctx context.Context, db *DB) error {
	return db.Txn(ctx, func(ctx context.Context) error {
		if err := step1(); err != nil { // want `error in transaction closure is logged but not returned`
			log.Printf("step1 failed: %v", err)
		}
		return step2()
	})
}

// ProperErrorReturn - this should NOT be flagged.
func ProperErrorReturn(ctx context.Context, db *DB) error {
	return db.Txn(ctx, func(ctx context.Context) error {
		if err := step1(); err != nil {
			log.Printf("step1 failed: %v", err)
			return err
		}
		return step2()
	})
}

// ErrorAssigned - this should NOT be flagged.
func ErrorAssigned(ctx context.Context, db *DB) error {
	return db.Txn(ctx, func(ctx context.Context) error {
		var lastErr error
		if err := step1(); err != nil {
			log.Printf("step1 failed: %v", err)
			lastErr = err
		}
		if lastErr != nil {
			return lastErr
		}
		return step2()
	})
}

// NolintComment - this should NOT be flagged.
func NolintComment(ctx context.Context, db *DB) error {
	return db.Txn(ctx, func(ctx context.Context) error {
		//nolint:txnerr
		if err := step1(); err != nil {
			log.Printf("step1 failed: %v", err)
		}
		return step2()
	})
}

// NoLogCall - this should NOT be flagged (no logging).
func NoLogCall(ctx context.Context, db *DB) error {
	return db.Txn(ctx, func(ctx context.Context) error {
		if err := step1(); err != nil {
			_ = err // Silent ignore
		}
		return step2()
	})
}

// NonTxnMethod - this should NOT be flagged (not a transaction method).
func NonTxnMethod(ctx context.Context) error {
	return regularMethod(func() error {
		if err := step1(); err != nil {
			log.Printf("step1 failed: %v", err)
		}
		return step2()
	})
}

func regularMethod(fn func() error) error {
	return fn()
}

func step1() error {
	return errors.New("step1 error")
}

func step2() error {
	return nil
}

var _ = context.Background
