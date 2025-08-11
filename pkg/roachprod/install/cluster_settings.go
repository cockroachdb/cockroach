// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package install

import (
	"fmt"
	"reflect"

	"github.com/cockroachdb/cockroach/pkg/roachprod/config"
	"github.com/cockroachdb/cockroach/pkg/roachprod/roachprodutil/serde"
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
}

// ClusterSettingOption is the interface satisfied by options to MakeClusterSettings.
type ClusterSettingOption interface {
	DynamicType
	apply(settings *ClusterSettings)
}

type DynamicType interface {
	getTypeName(*ClusterSettingOptionTypes) (string, error)
}

// ClusterSettingsOption adds cluster settings via SET CLUSTER SETTING.
type ClusterSettingsOption map[string]string

func (o ClusterSettingsOption) apply(settings *ClusterSettings) {
	for name, value := range o {
		settings.ClusterSettings[name] = value
	}
}

func (o ClusterSettingsOption) getTypeName(t *ClusterSettingOptionTypes) (string, error) {
	return t.getTypeName(o, &t.ClusterSettingsOption)
}

// TagOption is used to pass a process tag.
type TagOption string

func (o TagOption) apply(settings *ClusterSettings) {
	settings.Tag = string(o)
}

func (o TagOption) getTypeName(t *ClusterSettingOptionTypes) (string, error) {
	return t.getTypeName(o, &t.TagOption)
}

// BinaryOption is used to pass a process tag.
type BinaryOption string

func (o BinaryOption) apply(settings *ClusterSettings) {
	settings.Binary = string(o)
}

func (o BinaryOption) getTypeName(t *ClusterSettingOptionTypes) (string, error) {
	return t.getTypeName(o, &t.BinaryOption)
}

// PGUrlCertsDirOption is used to pass certs dir for secure connections.
type PGUrlCertsDirOption string

func (o PGUrlCertsDirOption) apply(settings *ClusterSettings) {
	settings.PGUrlCertsDir = string(o)
}

func (o PGUrlCertsDirOption) getTypeName(t *ClusterSettingOptionTypes) (string, error) {
	return t.getTypeName(o, &t.PGUrlCertsDirOption)
}

// SecureOption is passed to create a secure cluster.
type SecureOption bool

func (o SecureOption) apply(settings *ClusterSettings) {
	settings.Secure = bool(o)
}

func (o SecureOption) getTypeName(t *ClusterSettingOptionTypes) (string, error) {
	return t.getTypeName(o, &t.SecureOption)
}

// UseTreeDistOption is passed to use treedist copy algorithm.
type UseTreeDistOption bool

func (o UseTreeDistOption) apply(settings *ClusterSettings) {
	settings.UseTreeDist = bool(o)
}

func (o UseTreeDistOption) getTypeName(t *ClusterSettingOptionTypes) (string, error) {
	return t.getTypeName(o, &t.UseTreeDistOption)
}

// EnvOption is used to pass environment variables to the cockroach process.
type EnvOption []string

var _ EnvOption

func (o EnvOption) apply(settings *ClusterSettings) {
	settings.Env = append(settings.Env, []string(o)...)
}

func (o EnvOption) getTypeName(t *ClusterSettingOptionTypes) (string, error) {
	return t.getTypeName(o, &t.EnvOption)
}

// NumRacksOption is used to pass the number of racks to partition the nodes into.
type NumRacksOption int

var _ NumRacksOption

func (o NumRacksOption) apply(settings *ClusterSettings) {
	settings.NumRacks = int(o)
}

func (o NumRacksOption) getTypeName(t *ClusterSettingOptionTypes) (string, error) {
	return t.getTypeName(o, &t.NumRacksOption)
}

// DebugDirOption is used to stash debug information.
type DebugDirOption string

var _ DebugDirOption

func (o DebugDirOption) apply(settings *ClusterSettings) {
	settings.DebugDir = string(o)
}

func (o DebugDirOption) getTypeName(t *ClusterSettingOptionTypes) (string, error) {
	return t.getTypeName(o, &t.DebugDirOption)
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

// ClusterSettingOptionList is a serializable list of ClusterSettingOption.
// These types are dynamic, so we are required to keep a type registry of all
// the option types.
type (
	ClusterSettingOptionList []ClusterSettingOption
)

type ClusterSettingOptionTypes struct {
	TagOption
	BinaryOption
	PGUrlCertsDirOption
	SecureOption
	UseTreeDistOption
	EnvOption
	NumRacksOption
	DebugDirOption
	ClusterSettingsOption
}

// clusterSettingOptionTypes is a map of all the types that can be
// serialized as a ClusterSettingOption.
var clusterSettingOptionTypes = (&ClusterSettingOptionTypes{}).toTypeMap()

// getTypeName returns the type name of `dynamicType`. It also validates that
// `validationRef` is of the same type and present in
// `ClusterSettingOptionTypes`.
func (t *ClusterSettingOptionTypes) getTypeName(
	dynamicType DynamicType, validationRef DynamicType,
) (string, error) {
	tValue := reflect.ValueOf(t).Elem()
	if reflect.TypeOf(validationRef).Kind() != reflect.Pointer {
		return "", fmt.Errorf("validationRef %T must be passed as a pointer type", validationRef)
	}
	validationRefValue := reflect.ValueOf(validationRef).Elem()
	for i := 0; i < tValue.NumField(); i++ {
		field := tValue.Field(i)
		fieldPtr := field.Addr().Interface()
		if fieldPtr == validationRefValue.Addr().Interface() {
			if reflect.TypeOf(dynamicType) != reflect.TypeOf(validationRef).Elem() {
				return "", fmt.Errorf("type mismatch: %T is not the same as validation type %T", dynamicType, validationRef)
			}
			return reflect.TypeOf(dynamicType).Name(), nil
		}
	}
	return "", fmt.Errorf("validationRef %T is not a field of ClusterSettingOptionTypes", validationRef)
}

// toTypeMap returns a map of all the types in ClusterSettingOptionTypes.
func (t *ClusterSettingOptionTypes) toTypeMap() serde.TypeMap {
	return serde.ToTypeMap(t)
}

type TypeWrapper serde.TypeWrapper

func (o ClusterSettingOptionList) MarshalYAML() (any, error) {
	result := make([]TypeWrapper, 0, len(o))
	for _, v := range o {
		typeName, err := v.getTypeName(&ClusterSettingOptionTypes{})
		if err != nil {
			return nil, fmt.Errorf("failed to get type name for %T: %w", v, err)
		}
		result = append(result, TypeWrapper{
			Type: typeName,
			Val:  v,
		})
	}
	return result, nil
}

func (o *ClusterSettingOptionList) UnmarshalYAML(value *yaml.Node) error {
	var containerList []TypeWrapper
	err := value.Decode(&containerList)
	if err != nil {
		return fmt.Errorf("failed to decode ClusterSettingOptionList: %w", err)
	}
	// Unwrap the container list items into their respective types.
	list := make(ClusterSettingOptionList, 0, len(containerList))
	for _, container := range containerList {
		list = append(list, container.Val.(ClusterSettingOption))
	}
	*o = list
	return nil
}

func (t *TypeWrapper) UnmarshalYAML(value *yaml.Node) error {
	return (*serde.TypeWrapper)(t).UnmarshalYAML(clusterSettingOptionTypes, value)
}
