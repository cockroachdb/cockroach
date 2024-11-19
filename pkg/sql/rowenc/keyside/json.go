// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package keyside

import (
	"github.com/cockroachdb/apd/v3"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/json"
	"github.com/cockroachdb/errors"
)

// encodeJSONKey is responsible for encoding the different JSON
// values.
func encodeJSONKey(b []byte, json *tree.DJSON, dir encoding.Direction) ([]byte, error) {
	return json.JSON.EncodeForwardIndex(b, dir)
}

// decodeJSONKey is responsible for decoding the different JSON
// values.
func decodeJSONKey(buf []byte, dir encoding.Direction) (json.JSON, []byte, error) {
	var err error
	var typ encoding.Type
	var jsonVal json.JSON

	buf, typ, err = encoding.ValidateAndConsumeJSONKeyMarker(buf, dir)
	if err != nil {
		return nil, nil, err
	}

	switch typ {
	case encoding.JSONNull, encoding.JSONNullDesc:
		jsonVal, err = json.MakeJSON(nil)
		if err != nil {
			return nil, nil, errors.NewAssertionErrorWithWrappedErrf(err, "could not decode JSON Null")
		}
	case encoding.JSONFalse, encoding.JSONFalseDesc:
		jsonVal, err = json.MakeJSON(false)
		if err != nil {
			return nil, nil, errors.NewAssertionErrorWithWrappedErrf(err, "could not decode JSON False")
		}
	case encoding.JSONTrue, encoding.JSONTrueDesc:
		jsonVal, err = json.MakeJSON(true)
		if err != nil {
			return nil, nil, errors.NewAssertionErrorWithWrappedErrf(err, "could not decode JSON True")
		}
	case encoding.JSONString, encoding.JSONStringDesc:
		jsonVal, buf, err = decodeJSONString(buf, dir)
		if err != nil {
			return nil, nil, errors.NewAssertionErrorWithWrappedErrf(err, "could not decode JSON String")
		}
	case encoding.JSONNumber:
		var dec apd.Decimal
		buf, dec, err = encoding.DecodeDecimalAscending(buf, nil)
		if err != nil {
			return nil, nil, errors.NewAssertionErrorWithWrappedErrf(err, "could not decode JSON Number")
		}
		if len(buf) == 0 || !encoding.IsJSONKeyDone(buf, dir) {
			return nil, nil, errors.AssertionFailedf("cannot find JSON terminator")
		}
		buf = buf[1:] // removing the terminator
		jsonVal = json.FromDecimal(dec)
	case encoding.JSONNumberDesc:
		var dec apd.Decimal
		buf, dec, err = encoding.DecodeDecimalDescending(buf, nil)
		if err != nil {
			return nil, nil, errors.NewAssertionErrorWithWrappedErrf(err, "could not decode JSON Number")
		}
		if len(buf) == 0 || !encoding.IsJSONKeyDone(buf, dir) {
			return nil, nil, errors.AssertionFailedf("cannot find JSON terminator")
		}
		buf = buf[1:] // removing the terminator
		jsonVal = json.FromDecimal(dec)
	case encoding.JSONArray, encoding.JSONArrayDesc, encoding.JsonEmptyArray, encoding.JsonEmptyArrayDesc:
		jsonVal, buf, err = decodeJSONArray(buf, dir)
		if err != nil {
			return nil, nil, errors.NewAssertionErrorWithWrappedErrf(err, "could not decode JSON Array")
		}
	case encoding.JSONObject, encoding.JSONObjectDesc:
		jsonVal, buf, err = decodeJSONObject(buf, dir)
		if err != nil {
			return nil, nil, errors.NewAssertionErrorWithWrappedErrf(err, "could not decode JSON Object")
		}
	}

	return jsonVal, buf, nil
}

func decodeJSONString(buf []byte, dir encoding.Direction) (json.JSON, []byte, error) {
	var err error
	var str string

	switch dir {
	case encoding.Ascending:
		buf, str, err = encoding.DecodeUnsafeStringAscendingDeepCopy(buf, nil)
	case encoding.Descending:
		buf, str, err = encoding.DecodeUnsafeStringDescending(buf, nil)
	}
	if err != nil {
		return nil, nil, errors.NewAssertionErrorWithWrappedErrf(err, "could not decode"+
			"the JSON String")
	}
	if len(buf) == 0 || !encoding.IsJSONKeyDone(buf, dir) {
		return nil, nil, errors.AssertionFailedf("cannot find JSON terminator")
	}
	buf = buf[1:] // removing the terminator
	jsonVal, err := json.MakeJSON(str)
	if err != nil {
		return nil, nil, errors.NewAssertionErrorWithWrappedErrf(err,
			"could not make a JSON String from the input string")
	}
	return jsonVal, buf, nil
}

func decodeJSONArray(buf []byte, dir encoding.Direction) (json.JSON, []byte, error) {
	// Extracting the total number of elements in the json array.
	var err error
	buf, length, err := encoding.DecodeJSONValueLength(buf, dir)
	if err != nil {
		return nil, nil, errors.AssertionFailedf("could not decode the number of elements in the JSON Array")
	}
	// Pre-allocate the array builder with `length` number
	// of JSON elements.
	jsonArray := json.NewArrayBuilder(int(length))

	var childElem json.JSON
	for {
		if len(buf) == 0 {
			return nil, nil, errors.AssertionFailedf("invalid JSON array encoding (unterminated)")
		}
		if encoding.IsJSONKeyDone(buf, dir) {
			buf = buf[1:]
			return jsonArray.Build(), buf, nil
		}
		childElem, buf, err = decodeJSONKey(buf, dir)
		if err != nil {
			return nil, buf, err
		}
		jsonArray.Add(childElem)
	}
}

func decodeJSONObject(buf []byte, dir encoding.Direction) (json.JSON, []byte, error) {
	// Extracting the total number of elements in the json object.
	var err error
	buf, length, err := encoding.DecodeJSONValueLength(buf, dir)
	if err != nil {
		return nil, nil, errors.AssertionFailedf("could not decode the number of elements in the JSON Object")
	}

	jsonObject := json.NewObjectBuilder(int(length))
	var jsonKey, value json.JSON
	for {
		if len(buf) == 0 {
			return nil, nil, errors.AssertionFailedf("invalid JSON Object encoding (unterminated)")
		}
		if encoding.IsJSONKeyDone(buf, dir) {
			// JSONB Objects will have a terminator byte.
			buf = buf[1:]
			return jsonObject.Build(), buf, nil
		}

		// Assumption: The byte array given to us can be decoded into a
		// valid JSON Object. In other words, for each JSON key there
		// should be a valid JSON value.
		jsonKey, buf, err = decodeJSONKey(buf, dir)
		if err != nil {
			return nil, buf, err
		}

		key, err := jsonKey.AsText()
		if err != nil {
			return nil, buf, err
		}

		value, buf, err = decodeJSONKey(buf, dir)
		if err != nil {
			return nil, buf, err
		}

		jsonObject.Add(*key, value)
	}
}
