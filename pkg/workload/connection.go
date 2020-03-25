// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

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
	// Method for issuing queries; see SQLRunner.
	Method string
}

// NewConnFlags returns an initialized ConnFlags.
func NewConnFlags(genFlags *Flags) *ConnFlags {
	c := &ConnFlags{}
	c.FlagSet = pflag.NewFlagSet(`conn`, pflag.ContinueOnError)
	c.StringVar(&c.DBOverride, `db`, ``,
		`Override for the SQL database to use. If empty, defaults to the generator name`)
	c.IntVar(&c.Concurrency, `concurrency`, 2*runtime.NumCPU(),
		`Number of concurrent workers`)
	c.StringVar(&c.Method, `method`, `prepare`, `SQL issue method (prepare, noprepare, simple)`)
	genFlags.AddFlagSet(c.FlagSet)
	if genFlags.Meta == nil {
		genFlags.Meta = make(map[string]FlagMeta)
	}
	genFlags.Meta[`db`] = FlagMeta{RuntimeOnly: true}
	genFlags.Meta[`concurrency`] = FlagMeta{RuntimeOnly: true}
	genFlags.Meta[`method`] = FlagMeta{RuntimeOnly: true}
	return c
}

// SanitizeUrls verifies that the give SQL connection strings have the correct
// SQL database set, rewriting them in place if necessary. This database name is
// returned.
func SanitizeUrls(gen Generator, dbOverride string, urls []string) (string, error) {
	genName := gen.Meta().Name
	dbName := genName
	if dbOverride != `` {
		dbName = dbOverride
	}
	for i := range urls {
		parsed, err := url.Parse(urls[i])
		if err != nil {
			return "", err
		}
		if err := setDBName(parsed, dbName); err != nil {
			return "", err
		}
		if err := setAppName(parsed, genName); err != nil {
			return "", err
		}

		switch parsed.Scheme {
		case "postgres", "postgresql":
			urls[i] = parsed.String()
		default:
			return ``, fmt.Errorf(`unsupported scheme: %s`, parsed.Scheme)
		}
	}
	return dbName, nil
}

func setDBName(url *url.URL, dbName string) error {
	if d := strings.TrimPrefix(url.Path, `/`); d != `` && d != dbName {
		return fmt.Errorf(`%s specifies database %q, but database %q is expected`,
			url, d, dbName)
	}
	url.Path = dbName
	return nil
}

func setAppName(url *url.URL, genName string) error {
	const appNameKey = `application_name`
	const appNamePrefix = `workload_`
	appName := appNamePrefix + genName

	q := url.Query()
	if n := q.Get(appNameKey); n != `` && n != appName {
		return fmt.Errorf(`%s specifies application_name %q, but application_name %q is expected`,
			url, n, appName)
	}
	q.Set(appNameKey, appName)
	url.RawQuery = q.Encode()
	return nil
}
