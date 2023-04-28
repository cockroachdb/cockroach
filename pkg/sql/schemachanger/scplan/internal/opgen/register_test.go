// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package opgen

import (
	"reflect"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
)

func TestOpGen(t *testing.T) {
	_ = scpb.ForEachElementType(func(e scpb.Element) error {
		field := reflect.TypeOf(e)
		t.Run(field.Name(), func(t *testing.T) {
			var adds, drops, transients []target
			for _, tg := range opRegistry.targets {
				if reflect.ValueOf(tg.e).Type() == field {
					switch tg.status {
					case scpb.Status_PUBLIC:
						adds = append(adds, tg)
					case scpb.Status_ABSENT:
						drops = append(drops, tg)
					case scpb.Status_TRANSIENT_ABSENT:
						transients = append(transients, tg)
					}
				}
			}
			if len(adds) != 1 && len(transients) != 1 {
				t.Errorf("expected one registered adding spec for %s, instead found %d", field.Name(), len(adds))
			}
			if len(drops) != 1 {
				t.Errorf("expected one registered dropping spec for %s, instead found %d", field.Name(), len(drops))
			}
		})
		return nil
	})
}
