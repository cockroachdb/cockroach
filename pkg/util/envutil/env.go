// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package envutil

import (
	"bytes"
	"fmt"
	"os"
	"os/user"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/humanizeutil"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
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

func checkVarName(name string) {
	// Env vars must:
	//  - start with COCKROACH_
	//  - be uppercase
	//  - only contain letters, digits, and _
	valid := strings.HasPrefix(name, "COCKROACH_")
	for i := 0; valid && i < len(name); i++ {
		c := name[i]
		valid = ((c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '_')
	}
	if !valid {
		panic("invalid env var name " + name)
	}
}

// getEnv retrieves an environment variable, keeps track of where
// it was accessed, and checks that each environment variable is accessed
// from at most one place.
// The bookkeeping enables a report of all influential environment
// variables with "cockroach debug env". To keep this report useful,
// all relevant environment variables should be read during start up.
func getEnv(varName string, depth int) (string, bool) {
	_, consumer, _, _ := runtime.Caller(depth + 1)
	checkVarName(varName)

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

// GetEnvVarsUsed returns the names of all environment variables that
// may have been used.
func GetEnvVarsUsed() []string {
	envVarRegistry.mu.Lock()
	defer envVarRegistry.mu.Unlock()

	var vars []string
	for k, v := range envVarRegistry.cache {
		if v.present {
			vars = append(vars, k+"="+os.Getenv(k))
		}
	}

	for _, name := range []string{"GOGC", "GODEBUG", "GOMAXPROCS", "GOTRACEBACK"} {
		if val, ok := os.LookupEnv(name); ok {
			vars = append(vars, name+"="+val)
		}
	}
	return vars
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

// HomeDir returns the user's home directory, as determined by the env
// var HOME, if it exists, and otherwise the system's idea of the user
// configuration (e.g. on non-UNIX systems).
func HomeDir() (string, error) {
	if homeDir := os.Getenv("HOME"); len(homeDir) > 0 {
		return homeDir, nil
	}
	userAcct, err := user.Current()
	if err != nil {
		return "", err
	}
	return userAcct.HomeDir, nil
}

// EnvString returns the value set by the specified environment variable. The
// depth argument indicates the stack depth of the caller that should be
// associated with the variable.
// The returned boolean flag indicates if the variable is set.
func EnvString(name string, depth int) (string, bool) {
	return getEnv(name, depth+1)
}

// EnvOrDefaultString returns the value set by the specified
// environment variable, if any, otherwise the specified default
// value.
func EnvOrDefaultString(name string, value string) string {
	if v, present := getEnv(name, 1); present {
		return v
	}
	return value
}

// EnvOrDefaultBool returns the value set by the specified environment
// variable, if any, otherwise the specified default value.
func EnvOrDefaultBool(name string, value bool) bool {
	if str, present := getEnv(name, 1); present {
		v, err := strconv.ParseBool(str)
		if err != nil {
			panic(fmt.Sprintf("error parsing %s: %s", name, err))
		}
		return v
	}
	return value
}

// EnvOrDefaultInt returns the value set by the specified environment
// variable, if any, otherwise the specified default value.
func EnvOrDefaultInt(name string, value int) int {
	if str, present := getEnv(name, 1); present {
		v, err := strconv.ParseInt(str, 0, 0)
		if err != nil {
			panic(fmt.Sprintf("error parsing %s: %s", name, err))
		}
		return int(v)
	}
	return value
}

// EnvOrDefaultInt64 returns the value set by the specified environment
// variable, if any, otherwise the specified default value.
func EnvOrDefaultInt64(name string, value int64) int64 {
	if str, present := getEnv(name, 1); present {
		v, err := strconv.ParseInt(str, 0, 64)
		if err != nil {
			panic(fmt.Sprintf("error parsing %s: %s", name, err))
		}
		return v
	}
	return value
}

// EnvOrDefaultFloat64 returns the value set by the specified environment
// variable, if any, otherwise the specified default value.
func EnvOrDefaultFloat64(name string, value float64) float64 {
	if str, present := getEnv(name, 1); present {
		v, err := strconv.ParseFloat(str, 64)
		if err != nil {
			panic(fmt.Sprintf("error parsing %s: %s", name, err))
		}
		return v
	}
	return value
}

var _ = EnvOrDefaultFloat64 // silence unused warning

// EnvOrDefaultBytes returns the value set by the specified environment
// variable, if any, otherwise the specified default value.
func EnvOrDefaultBytes(name string, value int64) int64 {
	if str, present := getEnv(name, 1); present {
		v, err := humanizeutil.ParseBytes(str)
		if err != nil {
			panic(fmt.Sprintf("error parsing %s: %s", name, err))
		}
		return v
	}
	return value
}

// EnvOrDefaultDuration returns the value set by the specified environment
// variable, if any, otherwise the specified default value.
func EnvOrDefaultDuration(name string, value time.Duration) time.Duration {
	if str, present := getEnv(name, 1); present {
		v, err := time.ParseDuration(str)
		if err != nil {
			panic(fmt.Sprintf("error parsing %s: %s", name, err))
		}
		return v
	}
	return value
}
