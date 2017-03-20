// Copyright 2015 The Cockroach Authors.
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
// Author: Peter Mattis (peter@cockroachlabs.com)

package sql

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"golang.org/x/net/context"

	"github.com/cockroachdb/apd"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

// Set sets session variables.
// Privileges: None.
//   Notes: postgres/mysql do not require privileges for session variables (some exceptions).
func (p *planner) Set(ctx context.Context, n *parser.Set) (planNode, error) {
	if n.Name == nil {
		// A client has sent the reserved internal syntax SET ROW ...
		// Reject it.
		return nil, errors.New("invalid statement: SET ROW")
	}

	// By using VarName.String() here any variables that are keywords will
	// be double quoted.
	name := strings.ToLower(n.Name.String())
	typedValues := make([]parser.TypedExpr, len(n.Values))
	for i, expr := range n.Values {
		typedValue, err := parser.TypeCheck(expr, nil, parser.TypeString)
		if err != nil {
			return nil, err
		}
		typedValues[i] = typedValue
	}

	if v, ok := varGen[name]; ok {
		if len(n.Values) == 0 {
			// SET ... TO DEFAULT
			if v.Reset == nil {
				return nil, fmt.Errorf("variable \"%s\" cannot be changed", name)
			} else if err := v.Reset(p); err != nil {
				return nil, err
			}
		} else {
			// SET ... TO ...
			if v.Set == nil {
				return nil, fmt.Errorf("variable \"%s\" cannot be changed", name)
			} else if err := v.Set(p, ctx, typedValues); err != nil {
				return nil, err
			}
		}
	} else {
		return nil, fmt.Errorf("unknown variable: %q", name)
	}

	return &emptyNode{}, nil
}

func (p *planner) getStringVal(name string, values []parser.TypedExpr) (string, error) {
	if len(values) != 1 {
		return "", fmt.Errorf("%s: requires a single string value", name)
	}
	val, err := values[0].Eval(&p.evalCtx)
	if err != nil {
		return "", err
	}
	s, ok := parser.AsDString(val)
	if !ok {
		return "", fmt.Errorf("%s: requires a single string value: %s is a %s",
			name, values[0], val.ResolvedType())
	}
	return string(s), nil
}

func (p *planner) SetDefaultIsolation(n *parser.SetDefaultIsolation) (planNode, error) {
	// Note: We also support SET DEFAULT_TRANSACTION_ISOLATION TO ' .... ' above.
	// Ensure both versions stay in sync.
	switch n.Isolation {
	case parser.SerializableIsolation:
		p.session.DefaultIsolationLevel = enginepb.SERIALIZABLE
	case parser.SnapshotIsolation:
		p.session.DefaultIsolationLevel = enginepb.SNAPSHOT
	default:
		return nil, fmt.Errorf("unsupported default isolation level: %s", n.Isolation)
	}
	return &emptyNode{}, nil
}

func (p *planner) SetTimeZone(n *parser.SetTimeZone) (planNode, error) {
	typedValue, err := parser.TypeCheck(n.Value, nil, parser.TypeInt)
	if err != nil {
		return nil, err
	}
	d, err := typedValue.Eval(&p.evalCtx)
	if err != nil {
		return nil, err
	}

	var loc *time.Location
	var offset int64
	switch v := parser.UnwrapDatum(d).(type) {
	case *parser.DString:
		location := string(*v)
		loc, err = timeutil.LoadLocation(location)
		if err != nil {
			return nil, fmt.Errorf("cannot find time zone %q: %v", location, err)
		}

	case *parser.DInterval:
		offset, _, _, err = v.Duration.Div(time.Second.Nanoseconds()).Encode()
		if err != nil {
			return nil, err
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
			return nil, fmt.Errorf("time zone value %s would overflow an int64", sixty)
		}

	default:
		return nil, fmt.Errorf("bad time zone value: %v", n.Value)
	}
	if loc == nil {
		loc = time.FixedZone(d.String(), int(offset))
	}
	p.session.Location = loc
	return &emptyNode{}, nil
}
