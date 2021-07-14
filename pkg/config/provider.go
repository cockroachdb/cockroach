// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package config

// SystemConfigProvider is capable of providing the SystemConfig, as well as
// notifying clients of updates to the SystemConfig.
type SystemConfigProvider interface {
	// GetSystemConfig returns the local unmarshaled version of the system
	// config. Returns nil if the system config hasn't been set yet.
	GetSystemConfig() *SystemConfig

	// RegisterSystemConfigChannel registers a channel to signify updates for
	// the system config. It is notified after registration (if a system config
	// is already set), and whenever a new system config is successfully
	// unmarshaled.
	RegisterSystemConfigChannel() <-chan struct{}
}

// EmptySystemConfigProvider is an implementation of SystemConfigProvider that
// never provides a system config.
type EmptySystemConfigProvider struct{}

// GetSystemConfig implements the SystemConfigProvider interface.
func (EmptySystemConfigProvider) GetSystemConfig() *SystemConfig {
	return nil
}

// RegisterSystemConfigChannel implements the SystemConfigProvider interface.
func (EmptySystemConfigProvider) RegisterSystemConfigChannel() <-chan struct{} {
	// The system config will never be updated, so return a nil channel.
	return nil
}
