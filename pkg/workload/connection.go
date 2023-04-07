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
	"time"

	"github.com/spf13/pflag"
)

// ConnFlags is helper of common flags that are relevant to QueryLoads.
type ConnFlags struct {
	*pflag.FlagSet
	DBOverride  string
	Concurrency int
	Method      string // Method for issuing queries; see SQLRunner.

	ConnHealthCheckPeriod time.Duration
	MaxConnIdleTime       time.Duration
	MaxConnLifetime       time.Duration
	MaxConnLifetimeJitter time.Duration
	MinConns              int
	WarmupConns           int
}

// NewConnFlags returns an initialized ConnFlags.
func NewConnFlags(genFlags *Flags) *ConnFlags {
	c := &ConnFlags{}
	c.FlagSet = pflag.NewFlagSet(`conn`, pflag.ContinueOnError)
	c.StringVar(&c.DBOverride, `db`, ``,
		`Override for the SQL database to use. If empty, defaults to the generator name`)
	c.IntVar(&c.Concurrency, `concurrency`, 2*runtime.GOMAXPROCS(0),
		`Number of concurrent workers`)
	c.StringVar(&c.Method, `method`, `cache_statement`, `SQL issue method (cache_statement, cache_describe, describe_exec, exec, simple_protocol)`)
	c.DurationVar(&c.ConnHealthCheckPeriod, `conn-healthcheck-period`, 30*time.Second, `Interval that health checks are run on connections`)
	c.IntVar(&c.MinConns, `min-conns`, 0, `Minimum number of connections to attempt to keep in the pool`)
	c.DurationVar(&c.MaxConnIdleTime, `max-conn-idle-time`, 150*time.Second, `Max time an idle connection will be kept around`)
	c.DurationVar(&c.MaxConnLifetime, `max-conn-lifetime`, 300*time.Second, `Max connection lifetime`)
	c.DurationVar(&c.MaxConnLifetimeJitter, `max-conn-lifetime-jitter`, 150*time.Second, `Jitter max connection lifetime by this amount`)
	c.IntVar(&c.WarmupConns, `warmup-conns`, 0, `Number of connections to warmup in each connection pool`)
	genFlags.AddFlagSet(c.FlagSet)
	if genFlags.Meta == nil {
		genFlags.Meta = make(map[string]FlagMeta)
	}
	for _, k := range []string{
		`concurrency`,
		`conn-healthcheck-period`,
		`db`,
		`max-conn-idle-time`,
		`max-conn-lifetime-jitter`,
		`max-conn-lifetime`,
		`method`,
		`min-conns`,
		`warmup-conns`,
	} {
		v, ok := genFlags.Meta[k]
		if !ok {
			v = FlagMeta{}
		}
		v.RuntimeOnly = true
		genFlags.Meta[k] = v
	}
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

		q := parsed.Query()
		q.Set("application_name", gen.Meta().Name)
		parsed.RawQuery = q.Encode()

		switch parsed.Scheme {
		case "postgres", "postgresql":
			urls[i] = parsed.String()
		default:
			return ``, fmt.Errorf(`unsupported scheme: %s`, parsed.Scheme)
		}
	}
	return dbName, nil
}
