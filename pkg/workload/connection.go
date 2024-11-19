// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package workload

import (
	"fmt"
	"net/url"
	"runtime"
	"sort"
	"strings"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/spf13/pflag"
)

// ConnFlags is helper of common flags that are relevant to QueryLoads.
type ConnFlags struct {
	*pflag.FlagSet
	DBOverride string
	IsoLevel   string
	ConnVars   []string

	Concurrency int
	Method      string // Method for issuing queries; see SQLRunner.

	ConnHealthCheckPeriod time.Duration
	DNSRefreshInterval    time.Duration
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
	c.StringVar(&c.IsoLevel, `isolation-level`, ``,
		`Isolation level to run workload transactions under [serializable, snapshot, read_committed]. `+
			`If unset, the workload will run with the default isolation level of the database.`)
	c.StringSliceVar(&c.ConnVars, `conn-vars`, []string{}, `Session variables to configure on database connections`)
	c.IntVar(&c.Concurrency, `concurrency`, 2*runtime.GOMAXPROCS(0), `Number of concurrent workers`)
	c.StringVar(&c.Method, `method`, `cache_statement`, `SQL issue method (cache_statement, cache_describe, describe_exec, exec, simple_protocol)`)
	c.DurationVar(&c.DNSRefreshInterval, `dns-refresh`, defaultDNSCacheRefresh, `Interval used to refresh cached DNS entries (<0 disables)`)
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
		`conn-vars`,
		`db`,
		`dns-refresh`,
		`isolation-level`,
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
func SanitizeUrls(gen Generator, connFlags *ConnFlags, urls []string) (string, error) {
	dbName := gen.Meta().Name
	if connFlags != nil && connFlags.DBOverride != `` {
		dbName = connFlags.DBOverride
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

// SetUrlConnVars augments the provided URLs with additional query parameters
// which are used by the SQL server during connection establishment to configure
// default session variables.
func SetUrlConnVars(gen Generator, connFlags *ConnFlags, urls []string) error {
	vars := make(map[string]string)
	vars["application_name"] = gen.Meta().Name
	if connFlags != nil {
		if connFlags.IsoLevel != "" {
			// As a convenience, replace underscores with spaces. This allows users of
			// the workload tool to pass --isolation-level=read_committed instead of
			// needing to pass --isolation-level="read committed".
			isoLevel := strings.ReplaceAll(connFlags.IsoLevel, "_", " ")
			// NOTE: validation of the isolation level value is done by the server during
			// connection establishment.
			vars["default_transaction_isolation"] = isoLevel
		}
		for _, v := range connFlags.ConnVars {
			parts := strings.Split(v, "=")
			if len(parts) != 2 {
				return errors.Errorf(`expected "key=value" format for --conn-vars, got %q`, v)
			}
			vars[parts[0]] = parts[1]
		}
	}
	varKeys := make([]string, 0, len(vars))
	for k := range vars {
		varKeys = append(varKeys, k)
	}
	sort.Strings(varKeys)

	for i := range urls {
		parsed, err := url.Parse(urls[i])
		if err != nil {
			return err
		}
		q := parsed.Query()
		for _, k := range varKeys {
			q.Set(k, vars[k])
		}
		parsed.RawQuery = q.Encode()
		urls[i] = parsed.String()
	}
	return nil
}
