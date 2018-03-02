// Copyright 2018 The Cockroach Authors.
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

package workload

import (
	"fmt"
	"net/url"
	"runtime"
	"strings"

	"github.com/spf13/pflag"
)

// ConnFlags is helper of common flags that are relevant to QueryLoads.
type ConnFlags struct {
	*pflag.FlagSet
	DBOverride  string
	Concurrency int
}

// NewConnFlags returns an initialized ConnFlags.
func NewConnFlags(genFlags *Flags) *ConnFlags {
	c := &ConnFlags{}
	c.FlagSet = pflag.NewFlagSet(`conn`, pflag.ContinueOnError)
	c.StringVar(&c.DBOverride, `db`, ``,
		`Override for the SQL database to use. If empty, defaults to the generator name`)
	c.IntVar(&c.Concurrency, `concurrency`, 2*runtime.NumCPU(),
		`Number of concurrent workers`)
	genFlags.AddFlagSet(c.FlagSet)
	if genFlags.Meta == nil {
		genFlags.Meta = make(map[string]FlagMeta)
	}
	genFlags.Meta[`db`] = FlagMeta{RuntimeOnly: true}
	genFlags.Meta[`concurrency`] = FlagMeta{RuntimeOnly: true}
	return c
}

// SanitizeUrls verifies that the give SQL connection strings have the correct
// SQL database set, rewriting them in place if necessary. This database name is
// returned.
func SanitizeUrls(gen Generator, dbOverride string, urls []string) (string, error) {
	dbName := gen.Meta().Name
	if dbOverride != `` {
		dbName = dbOverride
	}
	for i := range urls {
		parsed, err := url.Parse(urls[i])
		if err != nil {
			return "", err
		}
		if d := strings.TrimPrefix(parsed.Path, `/`); d != `` && d != dbName {
			return "", fmt.Errorf(`%s specifies database %q, but database %q is expected`,
				urls[i], d, dbName)
		}
		parsed.Path = dbName

		switch parsed.Scheme {
		case "postgres", "postgresql":
			urls[i] = parsed.String()
		default:
			return ``, fmt.Errorf(`unsupported scheme: %s`, parsed.Scheme)
		}
	}
	return dbName, nil
}
