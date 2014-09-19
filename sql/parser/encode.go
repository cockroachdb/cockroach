// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file

package parser

import (
	"database/sql/driver"
	"fmt"
	"strconv"
	"time"
)

var (
	dontEscape = byte(255)
	nullstr    = []byte("null")
	// encodeMap specifies how to escape binary data with '\'.
	// Complies to http://dev.mysql.com/doc/refman/5.1/en/string-syntax.html
	encodeMap [256]byte
	// decodeMap is the reverse of encodeMap
	decodeMap [256]byte
	hexMap    [256][]byte
)

// EncodeSQLValue ...
func EncodeSQLValue(buf []byte, arg interface{}) ([]byte, error) {
	// Use sql.driver to convert the arg to a sql.Value which is simply
	// an interface{} with a restricted set of types. This also takes
	// care of using the sql.Valuer interface to convert arbitrary types
	// into sql.Values.
	dv, err := driver.DefaultParameterConverter.ConvertValue(arg)
	if err != nil {
		return nil, fmt.Errorf("converting query argument type: %v", err)
	}

	switch v := dv.(type) {
	case nil:
		return append(buf, nullstr...), nil
	case bool:
		if v {
			return append(buf, '1'), nil
		}
		return append(buf, '0'), nil
	case int64:
		return strconv.AppendInt(buf, v, 10), nil
	case float64:
		return strconv.AppendFloat(buf, v, 'f', -1, 64), nil
	case string:
		return encodeSQLString(buf, []byte(v)), nil
	case []byte:
		return encodeSQLBytes(buf, v), nil
	case time.Time:
		return encodeSQLString(buf, []byte(v.Format("2006-01-02 15:04:05"))), nil
	default:
		return nil, fmt.Errorf("unsupported bind variable type %T: %v", arg, arg)
	}
}

func encodeSQLString(buf []byte, in []byte) []byte {
	buf = append(buf, '\'')
	for _, ch := range in {
		if encodedChar := encodeMap[ch]; encodedChar == dontEscape {
			buf = append(buf, ch)
		} else {
			buf = append(buf, '\\')
			buf = append(buf, encodedChar)
		}
	}
	buf = append(buf, '\'')
	return buf
}

func encodeSQLBytes(buf []byte, v []byte) []byte {
	buf = append(buf, "X'"...)
	for _, d := range v {
		buf = append(buf, hexMap[d]...)
	}
	buf = append(buf, '\'')
	return buf
}

func init() {
	for i := range encodeMap {
		encodeMap[i] = dontEscape
		decodeMap[i] = dontEscape
	}

	encodeRef := []struct {
		from byte
		to   byte
	}{
		{'\x00', '0'},
		{'\'', '\''},
		{'"', '"'},
		{'\b', 'b'},
		{'\n', 'n'},
		{'\r', 'r'},
		{'\t', 't'},
		{26, 'Z'}, // ctl-Z
		{'\\', '\\'},
	}
	for _, r := range encodeRef {
		encodeMap[r.from] = r.to
		decodeMap[r.to] = r.from
	}

	for i := range hexMap {
		hexMap[i] = []byte(fmt.Sprintf("%02x", i))
	}
}
