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
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Peter Mattis (peter@cockroachlabs.com)

package sqlwire

import "strconv"

func (d Datum) String() string {
	if d.BoolVal != nil {
		if *d.BoolVal {
			return "true"
		}
		return "false"
	}
	if d.IntVal != nil {
		return strconv.FormatInt(*d.IntVal, 10)
	}
	if d.UintVal != nil {
		return strconv.FormatUint(*d.UintVal, 10)
	}
	if d.FloatVal != nil {
		return strconv.FormatFloat(*d.FloatVal, 'g', -1, 64)
	}
	if d.BytesVal != nil {
		return string(d.BytesVal)
	}
	if d.StringVal != nil {
		return *d.StringVal
	}
	return "NULL"
}

// IsNull returns true iff the datum is NULL.
func (d Datum) IsNull() bool {
	return d.BoolVal == nil && d.IntVal == nil && d.UintVal == nil &&
		d.FloatVal == nil && d.BytesVal == nil && d.StringVal == nil
}

// Bool parses the datum as a bool.
func (d Datum) Bool() (bool, error) {
	if d.BoolVal != nil {
		return *d.BoolVal, nil
	}
	if d.IntVal != nil {
		return *d.IntVal != 0, nil
	}
	if d.UintVal != nil {
		return *d.UintVal != 0, nil
	}
	if d.FloatVal != nil {
		return *d.FloatVal != 0, nil
	}
	// TODO(pmattis): ParseBool is more permissive than the SQL grammar accepting
	// "t" and "f". Is this conversion even necessary? Boolean expressions (where
	// Bool() is used) are composed of boolean literals and other boolean
	// expressions.
	if d.BytesVal != nil {
		v, err := strconv.ParseBool(string(d.BytesVal))
		if err != nil {
			return false, err
		}
		return v, nil
	}
	if d.StringVal != nil {
		v, err := strconv.ParseBool(*d.StringVal)
		if err != nil {
			return false, err
		}
		return v, nil
	}
	return false, nil
}

// TODO(pmattis): Check for loss of significant bits during conversion and
// return error.

// ToInt converts the datum type to int.
func (d Datum) ToInt() (Datum, error) {
	if d.BoolVal != nil {
		var v int64
		if *d.BoolVal {
			v = 1
		}
		return Datum{IntVal: &v}, nil
	}
	if d.IntVal != nil {
		return d, nil
	}
	if d.UintVal != nil {
		v := int64(*d.UintVal)
		return Datum{IntVal: &v}, nil
	}
	if d.FloatVal != nil {
		v := int64(*d.FloatVal)
		return Datum{IntVal: &v}, nil
	}
	if d.BytesVal != nil {
		v, err := strconv.ParseInt(string(d.BytesVal), 0, 64)
		if err != nil {
			return d, err
		}
		return Datum{IntVal: &v}, nil
	}
	if d.StringVal != nil {
		v, err := strconv.ParseInt(*d.StringVal, 0, 64)
		if err != nil {
			return d, err
		}
		return Datum{IntVal: &v}, nil
	}
	return Datum{IntVal: new(int64)}, nil
}

// ToUint converts the datum type to int.
func (d Datum) ToUint() (Datum, error) {
	if d.BoolVal != nil {
		var v uint64
		if *d.BoolVal {
			v = 1
		}
		return Datum{UintVal: &v}, nil
	}
	if d.IntVal != nil {
		v := uint64(*d.IntVal)
		return Datum{UintVal: &v}, nil
	}
	if d.UintVal != nil {
		return d, nil
	}
	if d.FloatVal != nil {
		v := uint64(*d.FloatVal)
		return Datum{UintVal: &v}, nil
	}
	if d.BytesVal != nil {
		v, err := strconv.ParseUint(string(d.BytesVal), 0, 64)
		if err != nil {
			return d, err
		}
		return Datum{UintVal: &v}, nil
	}
	if d.StringVal != nil {
		v, err := strconv.ParseUint(*d.StringVal, 0, 64)
		if err != nil {
			return d, err
		}
		return Datum{UintVal: &v}, nil
	}
	return Datum{UintVal: new(uint64)}, nil
}

// ToFloat converts the datum type to float.
func (d Datum) ToFloat() (Datum, error) {
	if d.BoolVal != nil {
		var v float64
		if *d.BoolVal {
			v = 1
		}
		return Datum{FloatVal: &v}, nil
	}
	if d.IntVal != nil {
		v := float64(*d.IntVal)
		return Datum{FloatVal: &v}, nil
	}
	if d.UintVal != nil {
		v := float64(*d.UintVal)
		return Datum{FloatVal: &v}, nil
	}
	if d.FloatVal != nil {
		return d, nil
	}
	if d.BytesVal != nil {
		v, err := strconv.ParseFloat(string(d.BytesVal), 64)
		if err != nil {
			return d, err
		}
		return Datum{FloatVal: &v}, nil
	}
	if d.StringVal != nil {
		v, err := strconv.ParseFloat(*d.StringVal, 64)
		if err != nil {
			return d, err
		}
		return Datum{FloatVal: &v}, nil
	}
	return Datum{FloatVal: new(float64)}, nil
}

// ToString converts the datum type to a string.
func (d Datum) ToString() Datum {
	if d.StringVal != nil {
		return d
	}
	s := d.String()
	return Datum{StringVal: &s}
}
