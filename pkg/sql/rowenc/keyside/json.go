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
	var err error
	var buf []byte
	jsonVal := json.JSON
	buf, err = jsonVal.EncodeForwardIndex(b, dir)

	if err != nil {
		return buf, err
	}
	return buf, nil
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

	if (typ == encoding.JSONNull) || (typ == encoding.JSONNullDesc) {
		jsonVal, err = json.MakeJSON(nil)
		if err != nil {
			return nil, nil, errors.AssertionFailedf("could not decode JSON Null")
		}
	} else if (typ == encoding.JSONFalse) || (typ == encoding.JSONFalseDesc) {
		jsonVal, err = json.MakeJSON(false)
		if err != nil {
			return nil, nil, errors.AssertionFailedf("could not decode JSON False")
		}
	} else if (typ == encoding.JSONTrue) || (typ == encoding.JSONTrueDesc) {
		jsonVal, err = json.MakeJSON(true)
		if err != nil {
			return nil, nil, errors.AssertionFailedf("could not decode JSON True")
		}
	} else if typ == encoding.JSONString || typ == encoding.JSONStringDesc {
		jsonVal, buf, err = decodeJSONString(buf, dir)
		if err != nil {
			return nil, nil, errors.AssertionFailedf("could not decode JSON String")
		}
	} else if typ == encoding.JSONNumber {
		var dec apd.Decimal
		buf, dec, err = encoding.DecodeDecimalAscending(buf, nil)
		if err != nil {
			return nil, nil, errors.AssertionFailedf("could not decode the JSON Number")
		}
		buf = buf[1:] // removing the terminator
		jsonVal = json.FromDecimal(dec)
	} else if typ == encoding.JSONNumberDesc {
		var dec apd.Decimal
		buf, dec, err = encoding.DecodeDecimalDescending(buf, nil)
		if err != nil {
			return nil, nil, errors.AssertionFailedf("could not decode the JSON Number")
		}
		buf = buf[1:] // removing the terminator
		jsonVal = json.FromDecimal(dec)
	} else if (typ == encoding.JSONArray) || (typ == encoding.JSONArrayDesc) {
		jsonVal, buf, err = decodeJSONArray(buf, dir)
		if err != nil {
			return nil, nil, errors.AssertionFailedf("could not decode the JSON Array")
		}
	} else if (typ == encoding.JSONObject) || (typ == encoding.JSONObjectDesc) {
		jsonVal, buf, err = decodeJSONObject(buf, dir)
		if err != nil {
			return nil, nil, errors.AssertionFailedf("could not decode the JSON Object")
		}
	}

	return jsonVal, buf, nil
}

func decodeJSONString(buf []byte, dir encoding.Direction) (json.JSON, []byte, error) {
	// extracting the total number of elements in the byte array
	var err error
	var str string
	buf, _, err = encoding.DecodeJSONValueLength(buf, dir)

	if err != nil {
		panic("could not decode the length of the JSON String")
	}

	switch dir {
	case encoding.Ascending:
		buf, str, err = encoding.DecodeUnsafeStringAscendingDeepCopy(buf, nil)
	case encoding.Descending:
		buf, str, err = encoding.DecodeUnsafeStringDescending(buf, nil)
	}
	if err != nil {
		panic("could not decode the JSON string")
	}
	buf = buf[1:] // removing the terminator
	jsonVal, err := json.MakeJSON(str)
	if err != nil {
		panic("could not make a JSON String from the input string")
	}
	return jsonVal, buf, nil
}

func decodeJSONArray(buf []byte, dir encoding.Direction) (json.JSON, []byte, error) {
	// extracting the total number of elements in the json array
	var err error
	buf, length, err := encoding.DecodeJSONValueLength(buf, dir)
	if err != nil {
		return nil, nil, errors.AssertionFailedf("could not decode the number" +
			"of elements in the JSON Array")
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
	// extracting the total number of elements in the json object
	var err error
	buf, length, err := encoding.DecodeJSONValueLength(buf, dir)
	if err != nil {
		return nil, nil, errors.AssertionFailedf("could not decode the number" +
			"of elements in the JSON Object")
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
