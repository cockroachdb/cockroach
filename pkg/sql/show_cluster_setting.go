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
	"bytes"
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/pkg/errors"

	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/duration"
)

func (p *planner) showStateMachineSetting(
	ctx context.Context, st *cluster.Settings, s *settings.StateMachineSetting, name string,
) (*tree.DString, error) {
	var d *tree.DString
	// For statemachine settings (at the time of writing, this is only the cluster version setting)
	// we show the value from the KV store and additionally wait for the local Gossip instance to
	// have observed the value as well. This makes sure that cluster version bumps become visible
	// immediately while at the same time guaranteeing that a node reporting a certain version has
	// also processed the corresponding Gossip update (which is important as only then does the node
	// update its persisted state; see #22796).
	if err := retry.ForDuration(10*time.Second, func() error {
		datums, err := p.queryRow(ctx, "SELECT value FROM system.settings WHERE name = $1", name)
		if err != nil {
			return err
		}
		var prevRawVal []byte
		if len(datums) != 0 {
			dStr, ok := datums[0].(*tree.DString)
			if !ok {
				return errors.New("the existing value is not a string")
			}
			prevRawVal = []byte(string(*dStr))
		}
		// Note that if no entry is found, we pretend that an entry
		// exists which is the version used for the running binary. This
		// may not be 100.00% correct, but it will do. The input is
		// checked more thoroughly when a user tries to change the
		// value, and the corresponding sql migration that makes sure
		// the above select finds something usually runs pretty quickly
		// when the cluster is bootstrapped.
		kvRawVal, obj, err := s.Validate(&st.SV, prevRawVal, nil)
		if err != nil {
			return errors.Errorf("unable to read existing value: %s", err)
		}

		// NB: if there is no persisted cluster version yet, this will match
		// kvRawVal (which is taken from `st.SV` in this case too).
		gossipRawVal := []byte(s.Get(&st.SV))
		if !bytes.Equal(gossipRawVal, kvRawVal) {
			return errors.Errorf("gossip and KV store disagree about value")
		}

		d = tree.NewDString(obj.(fmt.Stringer).String())
		return nil
	}); err != nil {
		return nil, err
	}

	return d, nil
}

func (p *planner) ShowClusterSetting(
	ctx context.Context, n *tree.ShowClusterSetting,
) (planNode, error) {

	if err := p.RequireSuperUser(ctx, "SHOW CLUSTER SETTINGS"); err != nil {
		return nil, err
	}

	name := strings.ToLower(n.Name)

	if name == "all" {
		return p.delegateQuery(ctx, "SHOW CLUSTER SETTINGS",
			"TABLE crdb_internal.cluster_settings", nil, nil)
	}

	st := p.ExecCfg().Settings
	val, ok := settings.Lookup(name)
	if !ok {
		return nil, errors.Errorf("unknown setting: %q", name)
	}
	var dType types.T
	switch val.(type) {
	case *settings.IntSetting, *settings.EnumSetting:
		dType = types.Int
	case *settings.StringSetting, *settings.ByteSizeSetting, *settings.StateMachineSetting:
		dType = types.String
	case *settings.BoolSetting:
		dType = types.Bool
	case *settings.FloatSetting:
		dType = types.Float
	case *settings.DurationSetting:
		dType = types.Interval
	default:
		return nil, errors.Errorf("unknown setting type for %s: %s", name, val.Typ())
	}

	columns := sqlbase.ResultColumns{{Name: name, Typ: dType}}
	return &delayedNode{
		name:    "SHOW CLUSTER SETTING " + name,
		columns: columns,
		constructor: func(ctx context.Context, p *planner) (planNode, error) {
			d := tree.DNull
			switch s := val.(type) {
			case *settings.IntSetting:
				d = tree.NewDInt(tree.DInt(s.Get(&st.SV)))
			case *settings.StringSetting:
				d = tree.NewDString(s.String(&st.SV))
			case *settings.StateMachineSetting:
				var err error
				d, err = p.showStateMachineSetting(ctx, st, s, name)
				if err != nil {
					return nil, err
				}
			case *settings.BoolSetting:
				d = tree.MakeDBool(tree.DBool(s.Get(&st.SV)))
			case *settings.FloatSetting:
				d = tree.NewDFloat(tree.DFloat(s.Get(&st.SV)))
			case *settings.DurationSetting:
				d = &tree.DInterval{Duration: duration.Duration{Nanos: s.Get(&st.SV).Nanoseconds()}}
			case *settings.EnumSetting:
				d = tree.NewDInt(tree.DInt(s.Get(&st.SV)))
			case *settings.ByteSizeSetting:
				d = tree.NewDString(s.String(&st.SV))
			}

			v := p.newContainerValuesNode(columns, 0)
			if _, err := v.rows.AddRow(ctx, tree.Datums{d}); err != nil {
				v.rows.Close(ctx)
				return nil, err
			}
			return v, nil
		},
	}, nil
}
