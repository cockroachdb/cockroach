// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cdctest

import (
	gojson "encoding/json"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/geo"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/json"
	"github.com/cockroachdb/cockroach/pkg/workload/rand"
)

// convertFieldFromJSON attempts to convert a JSON object to a Go type,
// including CRDB specific types like geometry.
func convertFieldFromJSON(d json.JSON) (interface{}, error) {
	switch d.Type() {
	case json.ArrayJSONType:
		iter, _ := d.AsArray()
		ret := make([]interface{}, 0)
		for i := 0; i < len(iter); i++ {
			if i, err := convertFieldFromJSON(iter[i]); err == nil {
				ret = append(ret, i)
			}
		}
		return ret, nil
	case json.ObjectJSONType:
		if g, err := geo.ParseGeographyFromGeoJSON([]byte(d.String())); err == nil {
			return rand.DatumToGoSQL(tree.NewDGeography(g))
		}
		if g, err := geo.ParseGeometryFromGeoJSON([]byte(d.String())); err == nil {
			return rand.DatumToGoSQL(tree.NewDGeometry(g))
		}
	default:
		break
	}
	var ret interface{}
	err := gojson.Unmarshal([]byte(d.String()), &ret)
	return ret, err
}

// convertJSONToMap converts a JSON object to a map[string]interface{}. For
// primitive types, the value is just a Go primitive type. For arrays and
// special CRDB objects, the value is handled specially.
func convertJSONToMap(d json.JSON) (map[string]interface{}, error) {
	if d == nil {
		return nil, nil
	}
	ret := make(map[string]interface{})
	switch d.Type() {
	case json.ObjectJSONType:
		iter, _ := d.ObjectIter()
		for iter.Next() {
			if i, err := convertFieldFromJSON(iter.Value()); err == nil {
				ret[iter.Key()] = i
			} else {
				return nil, err
			}
		}
		return ret, nil
	case json.NullJSONType:
		var ret map[string]interface{}
		err := gojson.Unmarshal([]byte(d.String()), &ret)
		return ret, err
	default:
		panic(fmt.Sprintf("unexpected JSON type: %s", d.Type()))
	}
}
