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

//go:build windows
// +build windows

package app

import (
	"errors"
)

const (
	// DevStdinFilePath is the equivalent of /dev/stdin.
	//
	// This will be /dev/stdin for darwin and linux.
	// This does not exist for windows.
	DevStdinFilePath = ""
	// DevStdoutFilePath is the equivalent of /dev/stdout.
	//
	// This will be /dev/stdout for darwin and linux.
	// This does not exist for windows.
	DevStdoutFilePath = ""
	// DevStderrFilePath is the equivalent of /dev/stderr.
	//
	// This will be /dev/stderr for darwin and linux.
	// This does not exist for windows.
	DevStderrFilePath = ""
	// DevNullFilePath is the equivalent of /dev/null.
	//
	// This will be /dev/null for darwin and linux.
	// This will be nul for windows.
	DevNullFilePath = "nul"
)

// HomeDirPath returns the home directory path.
//
// This will be $HOME for darwin and linux.
// This will be %USERPROFILE% for windows.
func HomeDirPath(envContainer EnvContainer) (string, error) {
	if value := envContainer.Env("USERPROFILE"); value != "" {
		return value, nil
	}
	return "", errors.New("%USERPROFILE% is not set")
}

// CacheDirPath returns the cache directory path.
//
// This will be $XDG_CACHE_HOME for darwin and linux, falling back to $HOME/.cache.
// This will be %LocalAppData% for windows.
//
// See https://specifications.freedesktop.org/basedir-spec/basedir-spec-latest.html
// for darwin and linux. Note that we use the same for darwin and linux as this is
// what developers expect, as opposed to ~/Library/Preferences etc as the stdlib
// does for Go.
//
// Users cannot assume that CacheDirPath, ConfigDirPath, and DataDirPath are unique.
func CacheDirPath(envContainer EnvContainer) (string, error) {
	if value := envContainer.Env("LOCALAPPDATA"); value != "" {
		return value, nil
	}
	return "", errors.New("%LocalAppData% is not set")
}

// ConfigDirPath returns the config directory path.
//
// This will be $XDG_CONFIG_HOME for darwin and linux, falling back to $HOME/.config.
// This will be %AppData% for windows.
//
// See https://specifications.freedesktop.org/basedir-spec/basedir-spec-latest.html
// for darwin and linux. Note that we use the same for darwin and linux as this is
// what developers expect, as opposed to ~/Library/Preferences etc as the stdlib
// does for Go.
//
// Users cannot assume that CacheDirPath, ConfigDirPath, and DataDirPath are unique.
func ConfigDirPath(envContainer EnvContainer) (string, error) {
	if value := envContainer.Env("APPDATA"); value != "" {
		return value, nil
	}
	return "", errors.New("%AppData% is not set")
}

// DataDirPath returns the data directory path.
//
// This will be $XDG_DATA_HOME for darwin and linux, falling back to $HOME/.local/share.
// This will be %LocalAppData% for windows.
//
// See https://specifications.freedesktop.org/basedir-spec/basedir-spec-latest.html
// for darwin and linux. Note that we use the same for darwin and linux as this is
// what developers expect, as opposed to ~/Library/Preferences etc as the stdlib
// does for Go.
//
// Users cannot assume that CacheDirPath, ConfigDirPath, and DataDirPath are unique.
func DataDirPath(envContainer EnvContainer) (string, error) {
	if value := envContainer.Env("LOCALAPPDATA"); value != "" {
		return value, nil
	}
	return "", errors.New("%LocalAppData% is not set")
}
