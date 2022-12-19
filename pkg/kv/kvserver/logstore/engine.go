// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package logstore

import "github.com/cockroachdb/cockroach/pkg/storage"

// Engine is the interface through which the log store interacts with the
// storage engine.
//
// This is identical to storage.Engine but extends it with a marker such that
// callers can be deliberate about the separation between the log engine and
// the state machine engine.
type Engine interface {
	storage.Engine
	logStoreEngine()
}

type logEngine struct {
	storage.Engine
}

func (eng *logEngine) logStoreEngine() {}

// NewLogEngine wraps the provided storage.Engine as an Engine.
func NewLogEngine(eng storage.Engine) Engine {
	return &logEngine{Engine: eng}
}
