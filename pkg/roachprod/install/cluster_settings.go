// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package install

import (
	"fmt"
	"os"

	"github.com/cockroachdb/cockroach/pkg/roachprod/config"
	"github.com/cockroachdb/cockroach/pkg/roachprod/roachprodutil/codec"
	"github.com/cockroachdb/cockroach/pkg/roachprod/vm/gce"
	"github.com/cockroachdb/errors"
	"gopkg.in/yaml.v3"
)

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
	// ClusterSettings are, eh, actual cluster settings, i.e.
	// SET CLUSTER SETTING foo = 'bar'. The name clash is unfortunate.
	ClusterSettings map[string]string

	// This is used to pass the CLI flag
	secureFlagsOpt SecureOption
}

type secureFlagsOpt struct {
	ForcedSecure   bool
	ForcedInsecure bool
	DefaultSecure  bool
}

// ClusterSettingOption is the interface satisfied by options to MakeClusterSettings.
type ClusterSettingOption interface {
	codec.DynamicType
	apply(settings *ClusterSettings)
}

// ClusterSettingsOption adds cluster settings via SET CLUSTER SETTING.
// typegen:reg
type ClusterSettingsOption map[string]string

func (o ClusterSettingsOption) apply(settings *ClusterSettings) {
	for name, value := range o {
		settings.ClusterSettings[name] = value
	}
}

// TagOption is used to pass a process tag.
// typegen:reg
type TagOption string

func (o TagOption) apply(settings *ClusterSettings) {
	settings.Tag = string(o)
}

// BinaryOption is used to pass a process tag.
// typegen:reg
type BinaryOption string

func (o BinaryOption) apply(settings *ClusterSettings) {
	settings.Binary = string(o)
}

// PGUrlCertsDirOption is used to pass certs dir for secure connections.
// typegen:reg
type PGUrlCertsDirOption string

func (o PGUrlCertsDirOption) apply(settings *ClusterSettings) {
	settings.PGUrlCertsDir = string(o)
}

// ComplexSecureOption is a complex type for secure options that keeps track of
// the user's intent regarding security.
// typegen:reg
type ComplexSecureOption secureFlagsOpt

func (o ComplexSecureOption) apply(settings *ClusterSettings) {
	settings.secureFlagsOpt = o

	// We precompute the Secure field with the default value or with forced flags.
	// NewSyncedCluster will call ComputeSecure() to compute the Secure value
	// based on the cluster settings and might override it based on the cluster
	// settings (if forced flags were passed).
	settings.Secure = o.DefaultSecure
	if o.ForcedSecure {
		settings.Secure = true
	} else if o.ForcedInsecure {
		settings.Secure = false
	}
}

// overrideBasedOnClusterSettings sets the ClusterSetting's Secure flag based
// on the ComplexSecureOption value on the SyncedCluster struct and the cluster
// settings.
func (o ComplexSecureOption) overrideBasedOnClusterSettings(c *SyncedCluster) error {

	switch {
	case o.ForcedSecure && o.ForcedInsecure:
		return errors.New("cannot set both secure and insecure to true")
	case o.ForcedSecure:
		c.Secure = true
	case o.ForcedInsecure:
		c.Secure = false
	default:
		// In case the cluster is a GCE cluster in the cockroach-ephemeral project,
		// we make it insecure by default. This is to avoid dealing with certificates
		// for ephemeral engineering test clusters.
		if len(c.Clouds()) == 1 && c.Clouds()[0] == fmt.Sprintf("%s:%s", gce.ProviderName, gce.DefaultProjectID) {
			fmt.Fprintf(os.Stderr, "WARN: cluster %s defaults to insecure, because it is in project %s\n",
				c.Name,
				gce.DefaultProjectID,
			)
			c.Secure = false
			return nil
		}

		// In every other case, we use the CLI flag default value.
		c.Secure = o.DefaultSecure
	}

	return nil
}

// SimpleSecureOption is a simple type that simplifies setting the secure flags
// in the cluster settings without keeping track of --secure or --insecure options.
// typegen:reg
type SimpleSecureOption bool

func (o SimpleSecureOption) apply(settings *ClusterSettings) {
	if bool(o) {
		settings.Secure = true
	} else {
		settings.Secure = false

	}
}

// overrideBasedOnClusterSettings satisfies the SecureOption interface and sets
// the Secure flag based on the SimpleSecureOption value.
func (o SimpleSecureOption) overrideBasedOnClusterSettings(c *SyncedCluster) error {
	c.Secure = bool(o)
	return nil
}

type SecureOption interface {
	ClusterSettingOption
	overrideBasedOnClusterSettings(c *SyncedCluster) error
}

// UseTreeDistOption is passed to use treedist copy algorithm.
// typegen:reg
type UseTreeDistOption bool

func (o UseTreeDistOption) apply(settings *ClusterSettings) {
	settings.UseTreeDist = bool(o)
}

// EnvOption is used to pass environment variables to the cockroach process.
// typegen:reg
type EnvOption []string

var _ EnvOption

func (o EnvOption) apply(settings *ClusterSettings) {
	settings.Env = append(settings.Env, []string(o)...)
}

// NumRacksOption is used to pass the number of racks to partition the nodes into.
// typegen:reg
type NumRacksOption int

var _ NumRacksOption

func (o NumRacksOption) apply(settings *ClusterSettings) {
	settings.NumRacks = int(o)
}

// DebugDirOption is used to stash debug information.
// typegen:reg
type DebugDirOption string

var _ DebugDirOption

func (o DebugDirOption) apply(settings *ClusterSettings) {
	settings.DebugDir = string(o)
}

// MakeClusterSettings makes a ClusterSettings.
func MakeClusterSettings(opts ...ClusterSettingOption) ClusterSettings {
	clusterSettings := ClusterSettings{
		Binary:          config.Binary,
		Tag:             "",
		PGUrlCertsDir:   fmt.Sprintf("./%s", CockroachNodeCertsDir),
		Secure:          true,
		UseTreeDist:     true,
		Env:             config.DefaultEnvVars(),
		NumRacks:        0,
		ClusterSettings: map[string]string{},
	}
	// Override default values using the passed options (if any).
	for _, opt := range opts {
		opt.apply(&clusterSettings)
	}
	return clusterSettings
}

// ClusterSettingOptionList is a list of ClusterSettingOption that can be
// serialized to YAML. It uses codec.ListWrapper to handle the dynamic types.
type ClusterSettingOptionList []ClusterSettingOption

func (o ClusterSettingOptionList) MarshalYAML() (any, error) {
	return codec.WrapList(o), nil
}

func (o *ClusterSettingOptionList) UnmarshalYAML(value *yaml.Node) error {
	var lw codec.ListWrapper[ClusterSettingOption]
	if err := value.Decode(&lw); err != nil {
		return err
	}
	*o = lw.Get()
	return nil
}
