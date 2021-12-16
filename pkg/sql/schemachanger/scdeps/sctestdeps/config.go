// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sctestdeps

import (
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/nstree"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scrun"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
)

// Option configures the TestState.
type Option interface {
	apply(*TestState)
}

type optionFunc func(*TestState)

func (o optionFunc) apply(state *TestState) { o(state) }

var _ Option = (optionFunc)(nil)

// WithNamespace sets the TestState namespace to the provided value.
func WithNamespace(ns map[descpb.NameInfo]descpb.ID) Option {
	return optionFunc(func(state *TestState) {
		state.namespace = ns
	})
}

// WithDescriptors sets the TestState descriptors to the provided value.
func WithDescriptors(descs nstree.Map) Option {
	return optionFunc(func(state *TestState) {
		state.descriptors = descs
	})
}

// WithSessionData sets the TestState sessiondata to the provided value.
func WithSessionData(sessionData sessiondata.SessionData) Option {
	return optionFunc(func(state *TestState) {
		state.sessionData = sessionData
	})
}

// WithTestingKnobs sets the TestState testing knobs to the provided value.
func WithTestingKnobs(testingKnobs *scrun.TestingKnobs) Option {
	return optionFunc(func(state *TestState) {
		state.testingKnobs = testingKnobs
	})
}

// WithStatements sets the TestState statement to the provided value.
func WithStatements(statements ...string) Option {
	return optionFunc(func(state *TestState) {
		state.statements = statements
	})
}

// WithCurrentDatabase sets the TestState current database to the provided value.
func WithCurrentDatabase(db string) Option {
	return optionFunc(func(state *TestState) {
		state.currentDatabase = db
	})
}

var defaultOptions = []Option{
	optionFunc(func(state *TestState) {
		state.namespace = make(map[descpb.NameInfo]descpb.ID)
	}),
}
