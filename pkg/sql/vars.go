// Copyright 2017 The Cockroach Authors.
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

package sql

import (
	"fmt"
	"sort"
	"strings"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
)

const (
	// PgServerVersion is the latest version of postgres that we claim to support.
	PgServerVersion = "9.5.0"
)

// sessionVar provides a unified interface for performing operations on
// variables such as the selected database, or desired syntax.
type sessionVar struct {
	// Set performs mutations (usually on p.session) to effect the change
	// desired by SET commands.
	Set func(ctx context.Context, p *planner, values []parser.TypedExpr) error

	// Get returns a string representation of a given variable to be used
	// either by SHOW or in the pg_catalog table.
	Get func(p *planner) string

	// Reset performs mutations (usually on p.session) to effect the change
	// desired by RESET commands.
	Reset func(*planner) error
}

// nopVar is a placeholder for a number of settings sent by various client
// drivers which we do not support, but should simply ignore rather than
// throwing an error when trying to SET or SHOW them.
var nopVar = sessionVar{
	Set:   func(context.Context, *planner, []parser.TypedExpr) error { return nil },
	Get:   func(*planner) string { return "" },
	Reset: func(*planner) error { return nil },
}

var varGen = map[string]sessionVar{
	`database`: {
		Set: func(ctx context.Context, p *planner, values []parser.TypedExpr) error {
			dbName, err := p.getStringVal(`database`, values)
			if err != nil {
				return err
			}

			if len(dbName) != 0 {
				// Verify database descriptor exists.
				if _, err := MustGetDatabaseDesc(ctx, p.txn, p.getVirtualTabler(), dbName); err != nil {
					return err
				}
			}
			p.session.Database = dbName
			p.evalCtx.Database = dbName

			return nil
		},
		Get: func(p *planner) string { return p.session.Database },
		Reset: func(p *planner) error {
			p.session.Database = p.session.defaults.database
			return nil
		},
	},
	`default_transaction_isolation`: {
		Set: func(_ context.Context, p *planner, values []parser.TypedExpr) error {
			// It's unfortunate that clients want us to support both SET
			// SESSION CHARACTERISTICS AS TRANSACTION ..., which takes the
			// isolation level as keywords/identifiers (e.g. JDBC), and SET
			// DEFAULT_TRANSACTION_ISOLATION TO '...', which takes an
			// expression (e.g. psycopg2). But that's how it is.  Just ensure
			// this code keeps in sync with SetDefaultIsolation() in set.go.
			s, err := p.getStringVal(`default_transaction_isolation`, values)
			if err != nil {
				return err
			}
			switch strings.ToUpper(s) {
			case `READ UNCOMMITTED`, `READ COMMITTED`, `SNAPSHOT`:
				p.session.DefaultIsolationLevel = enginepb.SNAPSHOT
			case `REPEATABLE READ`, `SERIALIZABLE`:
				p.session.DefaultIsolationLevel = enginepb.SERIALIZABLE
			default:
				return fmt.Errorf("set default_transaction_isolation: unknown isolation level: %q", s)
			}

			return nil
		},
		Get: func(p *planner) string { return p.session.DefaultIsolationLevel.String() },
		Reset: func(p *planner) error {
			p.session.DefaultIsolationLevel = enginepb.IsolationType(0)
			return nil
		},
	},
	`distsql`: {
		Set: func(_ context.Context, p *planner, values []parser.TypedExpr) error {
			s, err := p.getStringVal(`distsql`, values)
			if err != nil {
				return err
			}
			switch parser.Name(s).Normalize() {
			case parser.ReNormalizeName("off"):
				p.session.DistSQLMode = DistSQLOff
			case parser.ReNormalizeName("on"):
				p.session.DistSQLMode = DistSQLOn
			case parser.ReNormalizeName("auto"):
				p.session.DistSQLMode = DistSQLAuto
			case parser.ReNormalizeName("always"):
				p.session.DistSQLMode = DistSQLAlways
			default:
				return fmt.Errorf("set distsql: \"%s\" not supported", s)
			}

			return nil
		},
		Get: func(p *planner) string {
			return p.session.DistSQLMode.String()
		},
		Reset: func(p *planner) error {
			p.session.DistSQLMode = DistSQLExecModeFromInt(DistSQLClusterExecMode.Get())
			if p.session.execCfg.TestingKnobs.OverrideDistSQLMode != nil {
				p.session.DistSQLMode = DistSQLExecModeFromInt(p.session.execCfg.TestingKnobs.OverrideDistSQLMode.Get())
			}
			return nil
		},
	},
	`search_path`: {
		Set: func(_ context.Context, p *planner, values []parser.TypedExpr) error {
			// https://www.postgresql.org/docs/9.6/static/runtime-config-client.html
			newSearchPath := make(parser.SearchPath, len(values))
			foundPgCatalog := false
			for i, v := range values {
				val, err := v.Eval(&p.evalCtx)
				if err != nil {
					return err
				}
				s, ok := parser.AsDString(val)
				if !ok {
					return fmt.Errorf("set search_path: requires string values: %s is %s not string",
						v, val.ResolvedType())
				}
				if s == pgCatalogName {
					foundPgCatalog = true
				}
				newSearchPath[i] = parser.Name(s).Normalize()
			}
			if !foundPgCatalog {
				// "The system catalog schema, pg_catalog, is always searched,
				// whether it is mentioned in the path or not. If it is
				// mentioned in the path then it will be searched in the
				// specified order. If pg_catalog is not in the path then it
				// will be searched before searching any of the path items."
				newSearchPath = append([]string{"pg_catalog"}, newSearchPath...)
			}
			p.session.SearchPath = newSearchPath
			return nil
		},
		Get: func(p *planner) string { return strings.Join(p.session.SearchPath, ", ") },
		Reset: func(p *planner) error {
			p.session.SearchPath = sqlbase.DefaultSearchPath
			return nil
		},
	},
	`standard_conforming_strings`: {
		Set: func(_ context.Context, p *planner, values []parser.TypedExpr) error {
			// If true, escape backslash literals in strings. We do this by default,
			// and we do not support the opposite behavior.
			// See https://www.postgresql.org/docs/9.1/static/runtime-config-compatible.html#GUC-STANDARD-CONFORMING-STRINGS
			s, err := p.getStringVal(`standard_conforming_strings`, values)
			if err != nil {
				return err
			}
			if parser.Name(s).Normalize() != parser.ReNormalizeName("on") {
				return fmt.Errorf("set standard_conforming_strings: \"%s\" not supported", s)
			}

			return nil
		},
		Get:   func(*planner) string { return "on" },
		Reset: func(*planner) error { return nil },
	},
	`application_name`: {
		Set: func(_ context.Context, p *planner, values []parser.TypedExpr) error {
			// Set by clients to improve query logging.
			// See https://www.postgresql.org/docs/9.6/static/runtime-config-logging.html#GUC-APPLICATION-NAME
			s, err := p.getStringVal(`application_name`, values)
			if err != nil {
				return err
			}
			p.session.resetApplicationName(s)

			return nil
		},
		Get: func(p *planner) string { return p.session.ApplicationName },
		Reset: func(p *planner) error {
			p.session.resetApplicationName(p.session.defaults.applicationName)
			return nil
		},
	},
	`time zone`: {
		Get: func(p *planner) string {
			// If the time zone is a "fixed offset" one, initialized from an offset
			// and not a standard name, then we use a magic format in the Location's
			// name. We attempt to parse that here and retrieve the original offset
			// specified by the user.
			_, origRepr, parsed := sqlbase.ParseFixedOffsetTimeZone(p.session.Location.String())
			if parsed {
				return origRepr
			}
			return p.session.Location.String()
		},
	},
	`transaction isolation level`: {
		Get: func(p *planner) string { return p.txn.Isolation().String() },
	},
	`transaction priority`: {
		Get: func(p *planner) string { return p.txn.UserPriority().String() },
	},
	`transaction status`: {
		Get: func(p *planner) string { return getTransactionState(&p.session.TxnState, p.autoCommit) },
	},
	`max_index_keys`: {
		Get: func(*planner) string { return "32" },
	},
	`server_version`: {
		Get: func(*planner) string { return PgServerVersion },
	},
	`session_user`: {
		Get: func(p *planner) string { return p.session.User },
	},

	// See https://www.postgresql.org/docs/9.6/static/runtime-config-client.html
	`extra_float_digits`: nopVar,

	// Controls returned message verbosity. We don't support this.
	// See https://www.postgresql.org/docs/9.6/static/runtime-config-compatible.html
	`client_min_messages`: nopVar,

	// See https://www.postgresql.org/docs/9.6/static/multibyte.html
	`client_encoding`: {
		Get: func(*planner) string {
			return "UTF8"
		},
		Set: func(_ context.Context, p *planner, values []parser.TypedExpr) error {
			s, err := p.getStringVal(`client_encoding`, values)
			if err != nil {
				return err
			}
			upper := strings.ToUpper(s)
			if upper != "UTF8" && upper != "UNICODE" {
				return fmt.Errorf("non-UTF8 encoding %s not supported", s)
			}
			return nil
		},
	},
}

var varNames = func() []string {
	res := make([]string, 0, len(varGen))
	for vName := range varGen {
		res = append(res, vName)
	}
	sort.Strings(res)
	return res
}()
