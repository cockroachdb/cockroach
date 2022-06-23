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

package appprotoexec

import (
	"errors"
	"fmt"
	"strconv"
	"strings"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/pluginpb"
)

func newVersion(major int32, minor int32, patch int32, suffix string) *pluginpb.Version {
	version := &pluginpb.Version{
		Major: proto.Int32(major),
		Minor: proto.Int32(minor),
		Patch: proto.Int32(patch),
	}
	if suffix != "" {
		version.Suffix = proto.String(suffix)
	}
	return version
}

func parseVersionForCLIVersion(value string) (_ *pluginpb.Version, retErr error) {
	defer func() {
		if retErr != nil {
			retErr = fmt.Errorf("cannot parse protoc version %q: %w", value, retErr)
		}
	}()

	// protoc always starts with "libprotoc "
	value = strings.TrimPrefix(value, "libprotoc ")
	split := strings.Split(value, ".")
	if len(split) != 3 {
		return nil, errors.New("more than three components split by '.'")
	}
	major, err := strconv.ParseInt(split[0], 10, 32)
	if err != nil {
		return nil, err
	}
	minor, err := strconv.ParseInt(split[1], 10, 32)
	if err != nil {
		return nil, err
	}
	patchSplit := strings.Split(split[2], "-")
	patch, err := strconv.ParseInt(patchSplit[0], 10, 32)
	if err != nil {
		return nil, err
	}
	var suffix string
	switch len(patchSplit) {
	case 1:
	case 2:
		suffix = patchSplit[1]
	default:
		return nil, errors.New("more than two patch components split by '-'")
	}
	return newVersion(int32(major), int32(minor), int32(patch), suffix), nil
}

func versionString(version *pluginpb.Version) string {
	value := fmt.Sprintf("%d.%d.%d", version.GetMajor(), version.GetMinor(), version.GetPatch())
	if version.Suffix != nil {
		value = value + "-" + version.GetSuffix()
	}
	return value
}

func getFeatureProto3Optional(version *pluginpb.Version) bool {
	if version.GetSuffix() == "buf" {
		return true
	}
	if version.GetMajor() != 3 {
		return false
	}
	return version.GetMinor() > 11 && version.GetMinor() < 15
}

func getKotlinSupported(version *pluginpb.Version) bool {
	if version.GetSuffix() == "buf" {
		return true
	}
	if version.GetMajor() != 3 {
		return false
	}
	return version.GetMinor() > 16
}
