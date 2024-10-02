// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package isql

import (
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/util/admission/admissionpb"
)

type (
	// TxnOption is used to configure a Txn.
	TxnOption interface{ applyTxn(*TxnConfig) }

	// ExecutorOption configures an Executor.
	ExecutorOption interface{ applyEx(*ExecutorConfig) }

	// Option can configure both an Executor and a Txn.
	Option interface {
		TxnOption
		ExecutorOption
	}
)

// SteppingEnabled creates a TxnOption to determine whether the underlying
// transaction should have stepping enabled. If stepping is enabled, the
// transaction will implicitly use lower admission priority. However, the
// user will need to remember to Step the Txn to make writes visible. The
// Executor will automatically (for better or for worse) step the
// transaction when executing each statement.
func SteppingEnabled() TxnOption {
	return steppingEnabled(true)
}

// WithPriority allows the user to configure the priority for the transaction.
func WithPriority(p admissionpb.WorkPriority) TxnOption {
	return admissionPriority(p)
}

// WithSessionData allows the user to configure the session data for the Txn or
// Executor.
func WithSessionData(sd *sessiondata.SessionData) Option {
	return (*sessionDataOption)(sd)
}

// TxnConfig is the config to be set for txn.
type TxnConfig struct {
	ExecutorConfig
	steppingEnabled bool
	priority        *admissionpb.WorkPriority
}

// GetSteppingEnabled return the steppingEnabled setting from the txn config.
func (tc *TxnConfig) GetSteppingEnabled() bool {
	return tc.steppingEnabled
}

// GetAdmissionPriority returns the AdmissionControl configuration if it exists.
func (tc *TxnConfig) GetAdmissionPriority() (admissionpb.WorkPriority, bool) {
	if tc.priority != nil {
		return *tc.priority, true
	}
	return 0, false
}

func (tc *TxnConfig) Init(opts ...TxnOption) {
	for _, opt := range opts {
		opt.applyTxn(tc)
	}
}

// ExecutorConfig is the configuration used by the implementation of DB to
// set up the Executor.
type ExecutorConfig struct {
	sessionData *sessiondata.SessionData
}

func (ec *ExecutorConfig) GetSessionData() *sessiondata.SessionData {
	return ec.sessionData
}

// Init is used to initialize an ExecutorConfig.
func (ec *ExecutorConfig) Init(opts ...ExecutorOption) {
	for _, o := range opts {
		o.applyEx(ec)
	}
}

type sessionDataOption sessiondata.SessionData

func (o *sessionDataOption) applyEx(cfg *ExecutorConfig) {
	cfg.sessionData = (*sessiondata.SessionData)(o)
}
func (o *sessionDataOption) applyTxn(cfg *TxnConfig) {
	cfg.sessionData = (*sessiondata.SessionData)(o)
}

type steppingEnabled bool

func (s steppingEnabled) applyTxn(o *TxnConfig) { o.steppingEnabled = bool(s) }

type admissionPriority admissionpb.WorkPriority

func (a admissionPriority) applyTxn(config *TxnConfig) {
	config.priority = (*admissionpb.WorkPriority)(&a)
}
