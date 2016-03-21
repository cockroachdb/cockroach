package envutil

import (
	"os"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/util/humanizeutil"
	"github.com/cockroachdb/cockroach/util/log"
)

type envVarInfo struct {
	consumer string
	present  bool
	value    string
}

var envVarRegistry map[string]envVarInfo

func init() {
	ClearEnvCache()
}

func varName(name string) string {
	return "COCKROACH_" + strings.ToUpper(strings.Replace(name, "-", "_", -1))
}

// GetEnv retrieves an environment variable and keeps track of where
// it was accessed. This enables a report of all influential environment
// variables with "cockroach env".
func getEnv(name string, consumer string) (string, bool) {
	varName := varName(name)
	f, ok := envVarRegistry[varName]
	if ok {
		if f.consumer != consumer {
			panic("environment variable " + varName + " already used from " + f.consumer)
		}
		return f.value, f.present
	}
	v, found := os.LookupEnv(varName)
	envVarRegistry[varName] = envVarInfo{consumer: consumer, present: found, value: v}
	return v, found
}

// ClearEnvCache clears saved environment values so that
// a new read access the environment again. (Used for testing)
func ClearEnvCache() {
	envVarRegistry = make(map[string]envVarInfo)
}

// GetEnvReport dumps all configuration variables that may have been
// used and their value.
func GetEnvReport() string {
	s := ""
	for k, v := range envVarRegistry {
		if v.present {
			s += k + "=" + v.value + " # " + v.consumer + "\n"
		} else {
			s += "# " + k + " is not set (read from " + v.consumer + ")\n"
		}
	}
	return s
}

// EnvOrDefaultString returns the value set by the specified
// environment variable, if any, otherwise the specified default
// value.
func EnvOrDefaultString(name string, value string) string {
	_, c, _, _ := runtime.Caller(1)
	if v, present := getEnv(name, c); present {
		return v
	}
	return value
}

// EnvOrDefaultBool returns the value set by the specified environment
// variable, if any, otherwise the specified default value.
func EnvOrDefaultBool(name string, value bool) bool {
	_, c, _, _ := runtime.Caller(1)
	if str, present := getEnv(name, c); present {
		v, err := strconv.ParseBool(str)
		if err != nil {
			log.Errorf("error parsing %s: %s", varName(name), err)
			return value
		}
		return v
	}
	return value
}

// EnvOrDefaultInt returns the value set by the specified environment
// variable, if any, otherwise the specified default value.
func EnvOrDefaultInt(name string, value int) int {
	_, c, _, _ := runtime.Caller(1)
	if str, present := getEnv(name, c); present {
		v, err := strconv.ParseInt(str, 0, 0)
		if err != nil {
			log.Errorf("error parsing %s: %s", varName(name), err)
			return value
		}
		return int(v)
	}
	return value
}

// EnvOrDefaultInt64 returns the value set by the specified environment
// variable, if any, otherwise the specified default value.
func EnvOrDefaultInt64(name string, value int64) int64 {
	_, c, _, _ := runtime.Caller(1)
	if str, present := getEnv(name, c); present {
		v, err := strconv.ParseInt(str, 0, 64)
		if err != nil {
			log.Errorf("error parsing %s: %s", varName(name), err)
			return value
		}
		return v
	}
	return value
}

// EnvOrDefaultBytes returns the value set by the specified environment
// variable, if any, otherwise the specified default value.
func EnvOrDefaultBytes(name string, value int64) int64 {
	_, c, _, _ := runtime.Caller(1)
	if str, present := getEnv(name, c); present {
		v, err := humanizeutil.ParseBytes(str)
		if err != nil {
			log.Errorf("error parsing %s: %s", varName(name), err)
			return value
		}
		return v
	}
	return value
}

// EnvOrDefaultDuration returns the value set by the specified environment
// variable, if any, otherwise the specified default value.
func EnvOrDefaultDuration(name string, value time.Duration) time.Duration {
	_, c, _, _ := runtime.Caller(1)
	if str, present := getEnv(name, c); present {
		v, err := time.ParseDuration(str)
		if err != nil {
			log.Errorf("error parsing %s: %s", varName(name), err)
			return value
		}
		return v
	}
	return value
}
