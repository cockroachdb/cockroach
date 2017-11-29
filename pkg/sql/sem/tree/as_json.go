package tree

import (
	"github.com/cockroachdb/cockroach/pkg/util/json"
	"github.com/cockroachdb/cockroach/pkg/util/timeofday"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

// AsJSON implements conversion from Datum to json.JSON
func AsJSON(d Datum) (json.JSON, error) {
	switch t := d.(type) {
	case *DBool:
		return json.MakeJSONFromBool(bool(*t)), nil
	case *DInt:
		return json.MakeJSONFromInt(int(*t)), nil
	case *DFloat:
		return json.MakeJSONFromFloat64(float64(*t))
	case *DDecimal:
		return json.MakeJSONFromDecimal(t.Decimal), nil
	case *DString:
		return json.MakeJSONFromString(string(*t)), nil
	case *DCollatedString:
		return json.MakeJSONFromString(t.Contents), nil
	case *DBytes:
		return json.MakeJSONFromString(string(*t)), nil
	case *DUuid:
		return json.MakeJSONFromString(""), nil
	case *DIPAddr:
		return json.MakeJSONFromString(t.IPAddr.String()), nil
	case *DDate:
		return json.MakeJSONFromString(timeutil.Unix(int64(*t)*SecondsInDay, 0).Format(dateFormat)), nil
	case *DTime:
		return json.MakeJSONFromString(timeofday.TimeOfDay(*t).String()), nil
	case *DTimestamp:
		return json.MakeJSONFromString(t.UTC().Format(timestampRFC3339WithoutZoneFormat)), nil
	case *DTimestampTZ:
		return json.MakeJSONFromString(t.Time.Format(TimestampOutputFormat)), nil
	case *DInterval:
		return json.MakeJSONFromString(t.Duration.String()), nil
	case *DJSON:
		return t.JSON, nil
	case dNull:
		return json.MakeJSON(nil)
	case *DArray:
		var err error
		jsons := make([]json.JSON, t.Len())
		for i, e := range t.Array {
			jsons[i], err = AsJSON(e)
			if err != nil {
				return nil, err
			}
		}
		return json.MakeJSONFromArrayOfJSON(jsons), nil
	case *DOidWrapper:
		return AsJSON(t.Wrapped)
	}
	return nil, nil
}
