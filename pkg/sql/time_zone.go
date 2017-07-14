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
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

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
