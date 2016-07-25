// Copyright 2016 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.
//
// Author: Raphael 'kena' Poss (knz@cockroachlabs.com)

package envutil

import (
	"bytes"
	"fmt"
	"os"
	"runtime"
	"strconv"
	"strings"
	"time"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/util/humanizeutil"
	"github.com/cockroachdb/cockroach/util/log"
	"github.com/cockroachdb/cockroach/util/syncutil"
)

type envVarInfo struct {
	consumer string
	present  bool
	value    string
}

var envVarRegistry struct {
	mu    syncutil.Mutex
	cache map[string]envVarInfo
}

func init() {
	ClearEnvCache()
}

// VarName returns the name of the environment variable
// corresponding to a configuration key or command-line flag name.
func VarName(name string) string {
	return "COCKROACH_" + strings.ToUpper(strings.Replace(name, "-", "_", -1))
}

// getEnv retrieves an environment variable, keeps track of where
// it was accessed, and checks that each environment variable is accessed
// from at most one place.
// The bookkeeping enables a report of all influential environment
// variables with "cockroach debug env". To keep this report useful,
// all relevant environment variables should be read during start up.
func getEnv(name string) (string, bool) {
	_, consumer, _, _ := runtime.Caller(2)
	varName := VarName(name)

	envVarRegistry.mu.Lock()
	defer envVarRegistry.mu.Unlock()

	if f, ok := envVarRegistry.cache[varName]; ok {
		if f.consumer != consumer {
			panic("environment variable " + varName + " already used from " + f.consumer)
		}
		return f.value, f.present
	}
	v, found := os.LookupEnv(varName)
	envVarRegistry.cache[varName] = envVarInfo{consumer: consumer, present: found, value: v}
	return v, found
}

// ClearEnvCache clears saved environment values so that
// a new read access the environment again. (Used for testing)
func ClearEnvCache() {
	envVarRegistry.mu.Lock()
	defer envVarRegistry.mu.Unlock()

	envVarRegistry.cache = make(map[string]envVarInfo)
}

// GetEnvReport dumps all configuration variables that may have been
// used and their value.
func GetEnvReport() string {
	envVarRegistry.mu.Lock()
	defer envVarRegistry.mu.Unlock()

	var b bytes.Buffer
	for k, v := range envVarRegistry.cache {
		if v.present {
			fmt.Fprintf(&b, "%s = %s # %s\n", k, v.value, v.consumer)
		} else {
			fmt.Fprintf(&b, "# %s is not set (read from %s)\n", k, v.consumer)
		}
	}
	return b.String()
}

// GetShellCommand returns a complete command to run with a prefix of the command line.
func GetShellCommand(cmd string) []string {
	if runtime.GOOS == "windows" {
		if shell := os.Getenv("COMSPEC"); len(shell) > 0 {
			return []string{shell, "/C", cmd}
		}
		return []string{`C:\Windows\system32\cmd.exe`, "/C", cmd}
	}
	if shell := os.Getenv("SHELL"); len(shell) > 0 {
		return []string{shell, "-c", cmd}
	}

	return []string{"/bin/sh", "-c", cmd}
}

// EnvOrDefaultString returns the value set by the specified
// environment variable, if any, otherwise the specified default
// value.
func EnvOrDefaultString(name string, value string) string {
	if v, present := getEnv(name); present {
		return v
	}
	return value
}

// EnvOrDefaultBool returns the value set by the specified environment
// variable, if any, otherwise the specified default value.
func EnvOrDefaultBool(name string, value bool) bool {
	if str, present := getEnv(name); present {
		v, err := strconv.ParseBool(str)
		if err != nil {
			log.Errorf(context.TODO(), "error parsing %s: %s", VarName(name), err)
			return value
		}
		return v
	}
	return value
}

// EnvOrDefaultInt returns the value set by the specified environment
// variable, if any, otherwise the specified default value.
func EnvOrDefaultInt(name string, value int) int {
	if str, present := getEnv(name); present {
		v, err := strconv.ParseInt(str, 0, 0)
		if err != nil {
			log.Errorf(context.TODO(), "error parsing %s: %s", VarName(name), err)
			return value
		}
		return int(v)
	}
	return value
}

// EnvOrDefaultInt64 returns the value set by the specified environment
// variable, if any, otherwise the specified default value.
func EnvOrDefaultInt64(name string, value int64) int64 {
	if str, present := getEnv(name); present {
		v, err := strconv.ParseInt(str, 0, 64)
		if err != nil {
			log.Errorf(context.TODO(), "error parsing %s: %s", VarName(name), err)
			return value
		}
		return v
	}
	return value
}

// EnvOrDefaultBytes returns the value set by the specified environment
// variable, if any, otherwise the specified default value.
func EnvOrDefaultBytes(name string, value int64) int64 {
	if str, present := getEnv(name); present {
		v, err := humanizeutil.ParseBytes(str)
		if err != nil {
			log.Errorf(context.TODO(), "error parsing %s: %s", VarName(name), err)
			return value
		}
		return v
	}
	return value
}

// EnvOrDefaultDuration returns the value set by the specified environment
// variable, if any, otherwise the specified default value.
func EnvOrDefaultDuration(name string, value time.Duration) time.Duration {
	if str, present := getEnv(name); present {
		v, err := time.ParseDuration(str)
		if err != nil {
			log.Errorf(context.TODO(), "error parsing %s: %s", VarName(name), err)
			return value
		}
		return v
	}
	return value
}
