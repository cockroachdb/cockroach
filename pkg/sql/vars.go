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
	"time"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/pkg/errors"
)

const (
	// PgServerVersion is the latest version of postgres that we claim to support.
	PgServerVersion = "9.5.0"
)

// sessionVar provides a unified interface for performing operations on
// variables such as the selected database, or desired syntax.
type sessionVar struct {
	// Set performs mutations (usually on session) to effect the change
	// desired by SET commands.
	Set func(ctx context.Context, session *Session, values []parser.TypedExpr) error

	// Get returns a string representation of a given variable to be used
	// either by SHOW or in the pg_catalog table.
	Get func(*Session) string

	// Reset performs mutations (usually on session) to effect the change
	// desired by RESET commands.
	Reset func(*Session) error
}

// nopVar is a placeholder for a number of settings sent by various client
// drivers which we do not support, but should simply ignore rather than
// throwing an error when trying to SET or SHOW them.
var nopVar = sessionVar{
	Set:   func(context.Context, *Session, []parser.TypedExpr) error { return nil },
	Get:   func(*Session) string { return "" },
	Reset: func(*Session) error { return nil },
}

// varGen is the main definition array for all session variables.
// Note to maintainers: try to keep this sorted in the source code.
var varGen = map[string]sessionVar{
	`application_name`: {
		Set: func(_ context.Context, session *Session, values []parser.TypedExpr) error {
			// Set by clients to improve query logging.
			// See https://www.postgresql.org/docs/9.6/static/runtime-config-logging.html#GUC-APPLICATION-NAME
			s, err := getStringVal(session, `application_name`, values)
			if err != nil {
				return err
			}
			session.resetApplicationName(s)

			return nil
		},
		Get: func(session *Session) string {
			session.mu.RLock()
			defer session.mu.RUnlock()
			return session.mu.ApplicationName
		},
		Reset: func(session *Session) error {
			session.resetApplicationName(session.defaults.applicationName)
			return nil
		},
	},

	// Supported for PG compatibility only.
	// Controls returned message verbosity. We don't support this.
	// See https://www.postgresql.org/docs/9.6/static/runtime-config-compatible.html
	`client_min_messages`: nopVar,

	// Supported for PG compatibility only.
	// See https://www.postgresql.org/docs/9.6/static/multibyte.html
	// Also aliased to SET NAMES.
	`client_encoding`: {
		Get: func(*Session) string {
			return "UTF8"
		},
		Set: func(_ context.Context, session *Session, values []parser.TypedExpr) error {
			s, err := getStringVal(session, `client_encoding`, values)
			if err != nil {
				return err
			}
			upper := strings.ToUpper(s)
			if upper != "UTF8" && upper != "UNICODE" {
				return fmt.Errorf("non-UTF8 encoding %s not supported", s)
			}
			return nil
		},
		Reset: func(*Session) error { return nil },
	},

	`database`: {
		Set: func(ctx context.Context, session *Session, values []parser.TypedExpr) error {
			dbName, err := getStringVal(session, `database`, values)
			if err != nil {
				return err
			}

			if len(dbName) != 0 {
				// Verify database descriptor exists.
				session.TxnState.mu.RLock()
				defer session.TxnState.mu.RUnlock()
				if _, err := MustGetDatabaseDesc(ctx, session.TxnState.mu.txn, &session.virtualSchemas, dbName); err != nil {
					return err
				}
			}
			session.Database = dbName

			return nil
		},
		Get: func(session *Session) string { return session.Database },
		Reset: func(session *Session) error {
			session.Database = session.defaults.database
			return nil
		},
	},

	`datestyle`: {
		// Supported for PG compatibility only.
		Get: func(*Session) string {
			return "ISO"
		},
		Set: func(_ context.Context, session *Session, values []parser.TypedExpr) error {
			s, err := getStringVal(session, `datestyle`, values)
			if err != nil {
				return err
			}
			if strings.ToUpper(s) != "ISO" {
				return fmt.Errorf("non-ISO date style %s not supported", s)
			}
			return nil
		},
		Reset: func(*Session) error { return nil },
	},

	`default_transaction_isolation`: {
		Set: func(_ context.Context, session *Session, values []parser.TypedExpr) error {
			// It's unfortunate that clients want us to support both SET
			// SESSION CHARACTERISTICS AS TRANSACTION ..., which takes the
			// isolation level as keywords/identifiers (e.g. JDBC), and SET
			// DEFAULT_TRANSACTION_ISOLATION TO '...', which takes an
			// expression (e.g. psycopg2). But that's how it is.  Just ensure
			// this code keeps in sync with SetDefaultIsolation() in set.go.
			s, err := getStringVal(session, `default_transaction_isolation`, values)
			if err != nil {
				return err
			}
			switch strings.ToUpper(s) {
			case `READ UNCOMMITTED`, `READ COMMITTED`, `SNAPSHOT`:
				session.DefaultIsolationLevel = enginepb.SNAPSHOT
			case `REPEATABLE READ`, `SERIALIZABLE`:
				session.DefaultIsolationLevel = enginepb.SERIALIZABLE
			default:
				return fmt.Errorf("set default_transaction_isolation: unknown isolation level: %q", s)
			}

			return nil
		},
		Get: func(session *Session) string { return session.DefaultIsolationLevel.String() },
		Reset: func(session *Session) error {
			session.DefaultIsolationLevel = enginepb.IsolationType(0)
			return nil
		},
	},

	`distsql`: {
		Set: func(_ context.Context, session *Session, values []parser.TypedExpr) error {
			s, err := getStringVal(session, `distsql`, values)
			if err != nil {
				return err
			}
			switch strings.ToLower(s) {
			case "off":
				session.DistSQLMode = cluster.DistSQLOff
			case "on":
				session.DistSQLMode = cluster.DistSQLOn
			case "auto":
				session.DistSQLMode = cluster.DistSQLAuto
			case "always":
				session.DistSQLMode = cluster.DistSQLAlways
			default:
				return fmt.Errorf("set distsql: \"%s\" not supported", s)
			}

			return nil
		},
		Get: func(session *Session) string {
			return session.DistSQLMode.String()
		},
		Reset: func(session *Session) error {
			session.DistSQLMode = cluster.DistSQLExecMode(session.execCfg.Settings.DistSQLClusterExecMode.Get())
			return nil
		},
	},

	// Supported for PG compatibility only.
	// See https://www.postgresql.org/docs/9.6/static/runtime-config-client.html
	`extra_float_digits`: nopVar,

	`max_index_keys`: {
		// Supported for PG compatibility only.
		Get: func(*Session) string { return "32" },
	},

	`node_id`: {
		Get: func(session *Session) string { return fmt.Sprintf("%d", session.tables.leaseMgr.nodeID.Get()) },
	},

	`search_path`: {
		Set: func(_ context.Context, session *Session, values []parser.TypedExpr) error {
			// https://www.postgresql.org/docs/9.6/static/runtime-config-client.html
			newSearchPath := make(parser.SearchPath, len(values))
			foundPgCatalog := false
			for i, v := range values {
				s, err := datumAsString(session, "search_path", v)
				if err != nil {
					return err
				}
				if s == pgCatalogName {
					foundPgCatalog = true
				}
				newSearchPath[i] = s
			}
			if !foundPgCatalog {
				// "The system catalog schema, pg_catalog, is always searched,
				// whether it is mentioned in the path or not. If it is
				// mentioned in the path then it will be searched in the
				// specified order. If pg_catalog is not in the path then it
				// will be searched before searching any of the path items."
				newSearchPath = append([]string{"pg_catalog"}, newSearchPath...)
			}
			session.SearchPath = newSearchPath
			return nil
		},
		Get: func(session *Session) string { return strings.Join(session.SearchPath, ", ") },
		Reset: func(session *Session) error {
			session.SearchPath = sqlbase.DefaultSearchPath
			return nil
		},
	},

	`server_version`: {
		Get: func(*Session) string { return PgServerVersion },
	},
	`session_user`: {
		Get: func(session *Session) string { return session.User },
	},

	`standard_conforming_strings`: {
		// Supported for PG compatibility only.
		Set: func(_ context.Context, session *Session, values []parser.TypedExpr) error {
			// If true, escape backslash literals in strings. We do this by default,
			// and we do not support the opposite behavior.
			// See https://www.postgresql.org/docs/9.1/static/runtime-config-compatible.html#GUC-STANDARD-CONFORMING-STRINGS
			s, err := getStringVal(session, `standard_conforming_strings`, values)
			if err != nil {
				return err
			}
			if strings.ToLower(s) != "on" {
				return fmt.Errorf("set standard_conforming_strings: \"%s\" not supported", s)
			}

			return nil
		},
		Get:   func(*Session) string { return "on" },
		Reset: func(*Session) error { return nil },
	},

	`time zone`: {
		Get: func(session *Session) string {
			// If the time zone is a "fixed offset" one, initialized from an offset
			// and not a standard name, then we use a magic format in the Location's
			// name. We attempt to parse that here and retrieve the original offset
			// specified by the user.
			_, origRepr, parsed := sqlbase.ParseFixedOffsetTimeZone(session.Location.String())
			if parsed {
				return origRepr
			}
			return session.Location.String()
		},
		Set: setTimeZone,
		Reset: func(session *Session) error {
			session.Location = time.UTC
			return nil
		},
	},

	`transaction isolation level`: {
		Get: func(session *Session) string {
			session.TxnState.mu.RLock()
			defer session.TxnState.mu.RUnlock()
			return session.TxnState.mu.txn.Isolation().String()
		},
	},

	`transaction priority`: {
		Get: func(session *Session) string {
			session.TxnState.mu.RLock()
			defer session.TxnState.mu.RUnlock()
			return session.TxnState.mu.txn.UserPriority().String()
		},
	},

	`transaction status`: {
		Get: func(session *Session) string { return getTransactionState(&session.TxnState) },
	},

	`tracing`: {
		Get: func(session *Session) string {
			if session.Tracing.Enabled() {
				val := "on"
				if session.Tracing.RecordingType() == tracing.SingleNodeRecording {
					val += ", local"
				}
				if session.Tracing.KVTracingEnabled() {
					val += ", kv"
				}
				return val
			}
			return "off"
		},
		Reset: func(session *Session) error {
			if !session.Tracing.Enabled() {
				// Tracing is not active. Nothing to do.
				return nil
			}
			return stopTracing(session)
		},
		Set: func(_ context.Context, session *Session, values []parser.TypedExpr) error {
			return enableTracing(session, values)
		},
	},
}

func enableTracing(session *Session, values []parser.TypedExpr) error {
	traceKV := false
	recordingType := tracing.SnowballRecording
	enableMode := true

	for _, v := range values {
		s, err := datumAsString(session, "trace", v)
		if err != nil {
			return err
		}

		switch strings.ToLower(s) {
		case "on":
			enableMode = true
		case "off":
			enableMode = false
		case "kv":
			traceKV = true
		case "local":
			recordingType = tracing.SingleNodeRecording
		case "cluster":
			recordingType = tracing.SnowballRecording
		default:
			return errors.Errorf("set tracing: unknown mode %q", s)
		}
	}
	if !enableMode {
		return stopTracing(session)
	}
	return session.Tracing.StartTracing(recordingType, traceKV)
}

func stopTracing(s *Session) error {
	if err := s.Tracing.StopTracing(); err != nil {
		return errors.Wrapf(err, "error stopping tracing")
	}
	return nil
}

var varNames = func() []string {
	res := make([]string, 0, len(varGen))
	for vName := range varGen {
		res = append(res, vName)
	}
	sort.Strings(res)
	return res
}()
