// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cdctest

import (
	gojson "encoding/json"
	"fmt"
	"io"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/geo"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/json"
	"github.com/cockroachdb/cockroach/pkg/workload/rand"
	"github.com/cockroachdb/errors"
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

	var result interface{}
	decoder := gojson.NewDecoder(strings.NewReader(d.String()))
	// We want arbitrary size/precision decimals, so we call UseNumber() to tell
	// the decoder to decode numbers into strings instead of float64s (which we
	// later parse using apd).
	decoder.UseNumber()
	err := decoder.Decode(&result)
	if err != nil {
		return nil, err
	}

	// Check to see if input has more data.
	// Note: using decoder.More() is wrong since it allows `]` and '}'
	// characters (i.e. '{}]this is BAD' will be parsed fine, and More will return
	// false -- thus allowing invalid JSON data to be parsed correctly). The
	// decoder is meant to be used in the streaming applications; not in a one-off
	// Unmarshal mode we are using here.  Therefore, to check if input has
	// trailing characters, we have to attempt to decode the next value.
	var more interface{}
	if err := decoder.Decode(&more); err != io.EOF {
		return nil, errors.New("trailing characters after JSON document")
	}
	return result, nil
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
