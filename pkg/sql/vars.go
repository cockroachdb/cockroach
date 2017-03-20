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
	Set func(p *planner, ctx context.Context, values []parser.TypedExpr) error

	// Get returns a string representation of a given variable to be used
	// either by SHOW or in the pg_catalog table.
	Get func(p *planner) string
}

// nopVar is a placeholder for a number of settings sent by various client
// drivers which we do not support, but should simply ignore rather than
// throwing an error when trying to SET or SHOW them.
var nopVar = sessionVar{
	Set: func(*planner, context.Context, []parser.TypedExpr) error { return nil },
	Get: func(*planner) string { return "" },
}

var varGen = map[string]sessionVar{
	`database`: {
		Set: func(p *planner, ctx context.Context, values []parser.TypedExpr) error {
			dbName, err := p.getStringVal(`database`, values)
			if err != nil {
				return err
			}

			if len(dbName) != 0 {
				// Verify database descriptor exists.
				if _, err := p.mustGetDatabaseDesc(ctx, dbName); err != nil {
					return err
				}
			}
			p.session.Database = dbName
			p.evalCtx.Database = dbName

			return nil
		},
		Get: func(p *planner) string { return p.session.Database },
	},
	`syntax`: {
		Set: func(p *planner, _ context.Context, values []parser.TypedExpr) error {
			s, err := p.getStringVal(`syntax`, values)
			if err != nil {
				return err
			}
			switch parser.Name(s).Normalize() {
			case parser.ReNormalizeName(parser.Modern.String()):
				p.session.Syntax = parser.Modern
			case parser.ReNormalizeName(parser.Traditional.String()):
				p.session.Syntax = parser.Traditional
			default:
				return fmt.Errorf("set syntax: \"%s\" is not in (%q, %q)", s, parser.Modern, parser.Traditional)
			}

			return nil
		},
		Get: func(p *planner) string { return p.session.Syntax.String() },
	},
	`default_transaction_isolation`: {
		Set: func(p *planner, _ context.Context, values []parser.TypedExpr) error {
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
	},
	`distsql`: {
		Set: func(p *planner, _ context.Context, values []parser.TypedExpr) error {
			s, err := p.getStringVal(`distsql`, values)
			if err != nil {
				return err
			}
			switch parser.Name(s).Normalize() {
			case parser.ReNormalizeName("off"):
				p.session.DistSQLMode = distSQLOff
			case parser.ReNormalizeName("on"):
				p.session.DistSQLMode = distSQLOn
			case parser.ReNormalizeName("always"):
				p.session.DistSQLMode = distSQLAlways
			default:
				return fmt.Errorf("set distsql: \"%s\" not supported", s)
			}

			return nil
		},
		Get: func(p *planner) string {
			switch p.session.DistSQLMode {
			case distSQLOff:
				return "off"
			case distSQLOn:
				return "on"
			case distSQLAlways:
				return "always"
			}

			return "auto"
		},
	},
	`search_path`: {
		Set: func(p *planner, _ context.Context, values []parser.TypedExpr) error {
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
	},
	`standard_conforming_strings`: {
		Set: func(p *planner, _ context.Context, values []parser.TypedExpr) error {
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
		Get: func(*planner) string { return "on" },
	},
	`application_name`: {
		Set: func(p *planner, _ context.Context, values []parser.TypedExpr) error {
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
	},
	`time zone`: {
		Get: func(p *planner) string { return p.session.Location.String() },
	},
	`transaction isolation level`: {
		Get: func(p *planner) string { return p.txn.Isolation().String() },
	},
	`transaction priority`: {
		Get: func(p *planner) string { return p.txn.UserPriority().String() },
	},
	`map_index_keys`: {
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
		Set: func(p *planner, _ context.Context, values []parser.TypedExpr) error {
			s, err := p.getStringVal(`client_encoding`, values)
			if err != nil {
				return err
			}
			if strings.ToUpper(s) != "UTF8" {
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
