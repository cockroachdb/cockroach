// Copyright 2020-2021 Buf Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package protoc

import (
	"errors"
	"fmt"
)

var (
	errNoInputFiles = errors.New("no input files specified")
	errArgEmpty     = errors.New("empty argument specified")
)

func newCannotSpecifyOptWithoutOutError(pluginName string) error {
	return fmt.Errorf("cannot specify --%s_opt without --%s_out", pluginName, pluginName)
}

func newCannotSpecifyPathWithoutOutError(pluginName string) error {
	return fmt.Errorf("cannot specify --%s=protoc-gen-%s without --%s_out", pluginPathValuesFlagName, pluginName, pluginName)
}

func newRecursiveReferenceError(flagFilePath string) error {
	return fmt.Errorf("%s recursively referenced", flagFilePath)
}

func newDuplicateOutError(pluginName string) error {
	return fmt.Errorf("duplicate --%s_out", pluginName)
}

func newEmptyOptError(pluginName string) error {
	return fmt.Errorf("empty option value for %s", pluginName)
}

func newPluginPathValueEmptyError() error {
	return fmt.Errorf("--%s had an empty value", pluginPathValuesFlagName)
}

func newPluginPathValueInvalidError(pluginPathValue string) error {
	return fmt.Errorf("--%s value invalid: %s", pluginPathValuesFlagName, pluginPathValue)
}

func newPluginPathNameInvalidPrefixError(pluginName string) error {
	return fmt.Errorf(`--%s had name %q which must be prefixed by "protoc-gen-"`, pluginPathValuesFlagName, pluginName)
}

func newDuplicatePluginPathError(pluginName string) error {
	return fmt.Errorf("duplicate --%s for protoc-gen-%s", pluginPathValuesFlagName, pluginName)
}

func newEncodeNotSupportedError() error {
	return fmt.Errorf(
		`--%s is not supported by buf.

Buf only handles the binary and JSON formats for now, however we can support this flag if there is sufficient demand.
Please email us at support@buf.build if this is a need for your organization.`,
		encodeFlagName,
	)
}

func newDecodeNotSupportedError() error {
	return fmt.Errorf(
		`--%s is not supported by buf.

Buf only handles the binary and JSON formats for now, however we can support this flag if there is sufficient demand.
Please email us at support@buf.build if this is a need for your organization.`,
		decodeFlagName,
	)
}

func newDecodeRawNotSupportedError() error {
	return fmt.Errorf(
		`--%s is not supported by buf.

Buf only handles the binary and JSON formats for now, however we can support this flag if there is sufficient demand.
Please email us at support@buf.build if this is a need for your organization.`,
		decodeRawFlagName,
	)
}

func newDescriptorSetInNotSupportedError() error {
	return fmt.Errorf(
		`--%s is not supported by buf.

Buf will work with cross-repository imports Buf Schema Registry, which will be based on source files, not pre-compiled images.
We think this is a much safer option that leads to less errors and more consistent results.

Please email us at support@buf.build if this is a need for your organization.`,
		descriptorSetInFlagName,
	)
}
