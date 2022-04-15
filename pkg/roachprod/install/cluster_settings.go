// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package install

import "github.com/cockroachdb/cockroach/pkg/roachprod/config"

// ClusterSettings contains various knobs that affect operations on a cluster.
type ClusterSettings struct {
	Binary        string
	Secure        bool
	PGUrlCertsDir string
	Env           []string
	Tag           string
	UseTreeDist   bool
	NumRacks      int
	// DebugDir is used to stash debug information.
	DebugDir string
}

// ClusterSettingOption is the interface satisfied by options to MakeClusterSettings.
type ClusterSettingOption interface {
	apply(settings *ClusterSettings)
}

// TagOption is used to pass a process tag.
type TagOption string

func (o TagOption) apply(settings *ClusterSettings) {
	settings.Tag = string(o)
}

// BinaryOption is used to pass a process tag.
type BinaryOption string

func (o BinaryOption) apply(settings *ClusterSettings) {
	settings.Binary = string(o)
}

// PGUrlCertsDirOption is used to pass certs dir for secure connections.
type PGUrlCertsDirOption string

func (o PGUrlCertsDirOption) apply(settings *ClusterSettings) {
	settings.PGUrlCertsDir = string(o)
}

// SecureOption is passed to create a secure cluster.
type SecureOption bool

func (o SecureOption) apply(settings *ClusterSettings) {
	settings.Secure = bool(o)
}

// UseTreeDistOption is passed to use treedist copy algorithm.
type UseTreeDistOption bool

func (o UseTreeDistOption) apply(settings *ClusterSettings) {
	settings.UseTreeDist = bool(o)
}

// EnvOption is used to pass environment variables to the cockroach process.
type EnvOption []string

var _ EnvOption

func (o EnvOption) apply(settings *ClusterSettings) {
	settings.Env = append(settings.Env, []string(o)...)
}

// NumRacksOption is used to pass the number of racks to partition the nodes into.
type NumRacksOption int

var _ NumRacksOption

func (o NumRacksOption) apply(settings *ClusterSettings) {
	settings.NumRacks = int(o)
}

// DebugDirOption is used to stash debug information.
type DebugDirOption string

var _ DebugDirOption

func (o DebugDirOption) apply(settings *ClusterSettings) {
	settings.DebugDir = string(o)
}

// MakeClusterSettings makes a ClusterSettings.
func MakeClusterSettings(opts ...ClusterSettingOption) ClusterSettings {
	clusterSettings := ClusterSettings{
		Binary:        config.Binary,
		Tag:           "",
		PGUrlCertsDir: "./certs",
		Secure:        false,
		UseTreeDist:   true,
		Env:           config.DefaultEnvVars(),
		NumRacks:      0,
	}
	// Override default values using the passed options (if any).
	for _, opt := range opts {
		opt.apply(&clusterSettings)
	}
	return clusterSettings
}
