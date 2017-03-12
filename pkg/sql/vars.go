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

	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
)

const (
	// PgServerVersion is the latest version of postgres that we claim to support.
	PgServerVersion = "9.5.0"
)

type sessionVar struct {
	Set func(*planner, []parser.TypedExpr) error
	Get func(*planner) string
}

var varGen = map[string]sessionVar{
	`DATABASE`: {
		Set: func(p *planner, values []parser.TypedExpr) error {
			dbName, err := p.getStringVal(`DATABASE`, values)
			if err != nil {
				return err
			}

			if len(dbName) != 0 {
				// Verify database descriptor exists.
				if _, err := p.mustGetDatabaseDesc(dbName); err != nil {
					return err
				}
			}
			p.session.Database = dbName
			p.evalCtx.Database = dbName

			return nil
		},
		Get: func(p *planner) string {
			return p.session.Database
		},
	},
	`SYNTAX`: {
		Set: func(p *planner, values []parser.TypedExpr) error {
			s, err := p.getStringVal(`SYNTAX`, values)
			if err != nil {
				return err
			}
			switch parser.Name(s).Normalize() {
			case parser.ReNormalizeName(parser.Modern.String()):
				p.session.Syntax = parser.Modern
			case parser.ReNormalizeName(parser.Traditional.String()):
				p.session.Syntax = parser.Traditional
			default:
				return fmt.Errorf("SYNTAX: \"%s\" is not in (%q, %q)", s, parser.Modern, parser.Traditional)
			}

			return nil
		},
		Get: func(p *planner) string {
			return p.session.Syntax.String()
		},
	},
	`DEFAULT_TRANSACTION_ISOLATION`: {
		Set: func(p *planner, values []parser.TypedExpr) error {
			// It's unfortunate that clients want us to support both SET
			// SESSION CHARACTERISTICS AS TRANSACTION ..., which takes the
			// isolation level as keywords/identifiers (e.g. JDBC), and SET
			// DEFAULT_TRANSACTION_ISOLATION TO '...', which takes an
			// expression (e.g. psycopg2). But that's how it is.  Just ensure
			// this code keeps in sync with SetDefaultIsolation() in set.go.
			s, err := p.getStringVal(`DEFAULT_TRANSACTION_ISOLATION`, values)
			if err != nil {
				return err
			}
			switch strings.ToUpper(s) {
			case `READ UNCOMMITTED`, `READ COMMITTED`, `SNAPSHOT`:
				p.session.DefaultIsolationLevel = enginepb.SNAPSHOT
			case `REPEATABLE READ`, `SERIALIZABLE`:
				p.session.DefaultIsolationLevel = enginepb.SERIALIZABLE
			default:
				return fmt.Errorf("DEFAULT_TRANSACTION_ISOLATION: unknown isolation level: %q", s)
			}

			return nil
		},
		Get: func(p *planner) string {
			return p.session.DefaultIsolationLevel.String()
		},
	},
	`DISTSQL`: {
		Set: func(p *planner, values []parser.TypedExpr) error {
			s, err := p.getStringVal(`DISTSQL`, values)
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
				return fmt.Errorf("DISTSQL: \"%s\" not supported", s)
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
	`SEARCH_PATH`: {
		Set: func(p *planner, values []parser.TypedExpr) error {
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
					return fmt.Errorf("SEARCH_PATH: requires string values: %s is %s not string",
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
		Get: func(p *planner) string {
			return strings.Join(p.session.SearchPath, ", ")
		},
	},
	`STANDARD_CONFORMING_STRINGS`: {
		Set: func(p *planner, values []parser.TypedExpr) error {
			// If true, escape backslash literals in strings. We do this by default,
			// and we do not support the opposite behavior.
			// See https://www.postgresql.org/docs/9.1/static/runtime-config-compatible.html#GUC-STANDARD-CONFORMING-STRINGS
			s, err := p.getStringVal(`STANDARD_CONFORMING_STRINGS`, values)
			if err != nil {
				return err
			}
			if parser.Name(s).Normalize() != parser.ReNormalizeName("on") {
				return fmt.Errorf("STANDARD_CONFORMING_STRINGS: \"%s\" not supported", s)
			}

			return nil
		},
		Get: func(_ *planner) string {
			return "on"
		},
	},
	`APPLICATION_NAME`: {
		Set: func(p *planner, values []parser.TypedExpr) error {
			// Set by clients to improve query logging.
			// See https://www.postgresql.org/docs/9.6/static/runtime-config-logging.html#GUC-APPLICATION-NAME
			s, err := p.getStringVal(`APPLICATION_NAME`, values)
			if err != nil {
				return err
			}
			p.session.ApplicationName = s

			return nil
		},
		Get: func(p *planner) string {
			return p.session.ApplicationName
		},
	},
	`TIME ZONE`: {
		Get: func(p *planner) string {
			return p.session.Location.String()
		},
	},
	`TRANSACTION ISOLATION LEVEL`: {
		Get: func(p *planner) string {
			return p.txn.Isolation().String()
		},
	},
	`TRANSACTION PRIORITY`: {
		Get: func(p *planner) string {
			return p.txn.UserPriority().String()
		},
	},
	`MAP_INDEX_KEYS`: {
		Get: func(_ *planner) string {
			return "32"
		},
	},
	`SERVER_VERSION`: {
		Get: func(_ *planner) string {
			return PgServerVersion
		},
	},
	`SESSION_USER`: {
		Get: func(p *planner) string {
			return p.session.User
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
