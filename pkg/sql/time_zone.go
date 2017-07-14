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
//

package sql

import (
	"fmt"
	"strings"
	"time"

	"github.com/pkg/errors"
	"golang.org/x/net/context"

	"github.com/cockroachdb/apd"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

// This file contains the functions that manage the "SQL time zone".
//
// We use the term "time zone" for user-facing features: the cluster setting,
// the SET TIME ZONE statement, etc. Internally in Go we use the term "location"
// to align with Go's time package.
//
// The time zone is predominantly used to configure the conversions between
// timestamp values and strings.
//
// Each SQL session can have its own time zone. This is configured as follows:
//
// - by the SET TIME ZONE statement, if issued by the client, otherwise
// - by the executor default time zone, which is initialized once upon startup:
//   - by the value set via the command-line arg --sql-time-zone, if any and valid,
//   - otherwise, UTC.
//
// We don't allow updating the default time zone beyond start-up
// because if a client reconnects and finds a different time zone, any
// timestamp values without time zone will become silently different,
// possibly causing client breakage or data loss. See discussion in
// #16029.

const errFmtSetDefaultLocation = "error while configuring the default SQL time zone %q"

// LoadDefaultLocation is invoked once just after instantiating a new Executor.
func (e *Executor) LoadDefaultLocation(
	ctx context.Context, locStr string, memMetrics *MemoryMetrics,
) error {
	defer func() { log.Infof(ctx, "loaded default SQL time zone: %s", e.defaultLocation) }()
	e.defaultLocation = time.UTC

	if strings.ToLower(locStr) == "utc" {
		// Fast path: don't bother with the complexity below.
		return nil
	}

	// To load the default time zone, we simulate SET TIME ZONE
	// in an encapsulated ("internal") session, then extract
	// the resulting time zone from the encapsulated session
	// as new default value for the Executor.

	p := makeInternalPlanner("default time zone", nil, security.RootUser, memMetrics)
	defer finishInternalPlanner(p)

	// Prepare the fake statement.
	stmt, err := parser.ParseOne("SET TIME ZONE " + locStr)
	if err != nil {
		// Maybe the time zone was a string, try enclosing it.
		stmt, err = parser.ParseOne("SET TIME ZONE " + parser.EscapeSQLString(locStr))
		if err != nil {
			return errors.Wrapf(err, errFmtSetDefaultLocation, locStr)
		}
	}

	// Run it.
	pn, err := p.Set(ctx, stmt.(*parser.Set))
	if err != nil {
		return errors.Wrapf(err, errFmtSetDefaultLocation, locStr)
	}
	defer pn.Close(p.session.Ctx())

	if _, ok := pn.(*emptyNode); !ok {
		if err := p.startPlan(ctx, pn); err != nil {
			return errors.Wrapf(err, errFmtSetDefaultLocation, locStr)
		}
		if _, err := countRowsAffected(ctx, pn); err != nil {
			return errors.Wrapf(err, errFmtSetDefaultLocation, locStr)
		}
	}

	// The fake statement has left its result in the encapsulated
	// session. Use this.
	e.defaultLocation = p.session.Location
	return nil
}

func setTimeZone(_ context.Context, session *Session, values []parser.TypedExpr) error {
	if len(values) != 1 {
		return errors.New("set time zone requires a single argument")
	}
	evalCtx := session.evalCtx()
	d, err := values[0].Eval(&evalCtx)
	if err != nil {
		return err
	}

	var loc *time.Location
	var offset int64
	switch v := parser.UnwrapDatum(d).(type) {
	case *parser.DString:
		location := string(*v)
		loc, err = timeutil.LoadLocation(location)
		if err != nil {
			var err1 error
			loc, err1 = timeutil.LoadLocation(strings.ToUpper(location))
			if err1 != nil {
				loc, err1 = timeutil.LoadLocation(strings.ToTitle(location))
				if err1 != nil {
					return fmt.Errorf("cannot find time zone %q: %v", location, err)
				}
			}
		}

	case *parser.DInterval:
		offset, _, _, err = v.Duration.Div(time.Second.Nanoseconds()).Encode()
		if err != nil {
			return err
		}

	case *parser.DInt:
		offset = int64(*v) * 60 * 60

	case *parser.DFloat:
		offset = int64(float64(*v) * 60.0 * 60.0)

	case *parser.DDecimal:
		sixty := apd.New(60, 0)
		ed := apd.MakeErrDecimal(parser.ExactCtx)
		ed.Mul(sixty, sixty, sixty)
		ed.Mul(sixty, sixty, &v.Decimal)
		offset = ed.Int64(sixty)
		if ed.Err() != nil {
			return fmt.Errorf("time zone value %s would overflow an int64", sixty)
		}

	default:
		return fmt.Errorf("bad time zone value: %s", d.String())
	}
	if loc == nil {
		loc = sqlbase.FixedOffsetTimeZoneToLocation(int(offset), d.String())
	}
	session.Location = loc
	return nil
}
