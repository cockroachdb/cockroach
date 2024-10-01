// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package pgurl

import (
	"fmt"
	"net/url"
	"sort"
	"strings"
	"unicode"

	"github.com/cockroachdb/errors"
)

// ParseExtendedOptions is a parser for the value part of the special
// option "options".
//
// The options must be separated by space and have one of the
// following patterns: '-c key=value', '-ckey=value', '--key=value'
func ParseExtendedOptions(optionsString string) (res url.Values, err error) {
	res = url.Values{}
	lastWasDashC := false
	opts := splitOptions(optionsString)

	for i := 0; i < len(opts); i++ {
		prefix := ""
		if len(opts[i]) > 1 {
			prefix = opts[i][:2]
		}

		switch {
		case opts[i] == "-c":
			lastWasDashC = true
			continue
		case lastWasDashC:
			lastWasDashC = false
			// if the last option was '-c' parse current option with no regard to
			// the prefix
			prefix = ""
		case prefix == "--" || prefix == "-c":
			lastWasDashC = false
		default:
			return nil, errors.Newf(
				"option %q is invalid, must have prefix '-c' or '--'", opts[i])
		}

		key, value, err := splitOption(opts[i], prefix)
		if err != nil {
			return nil, err
		}
		res.Set(key, value)
	}
	return res, nil
}

// splitOptions slices the given string into substrings separated by space
// unless the space is escaped using backslashes '\\'. It also skips multiple
// subsequent spaces.
func splitOptions(options string) []string {
	var res []string
	var sb strings.Builder
	i := 0
	for i < len(options) {
		sb.Reset()
		// skip leading spaces.
		for i < len(options) && unicode.IsSpace(rune(options[i])) {
			i++
		}
		if i == len(options) {
			break
		}

		lastWasEscape := false

		for i < len(options) {
			if unicode.IsSpace(rune(options[i])) && !lastWasEscape {
				break
			}
			if !lastWasEscape && options[i] == '\\' {
				lastWasEscape = true
			} else {
				lastWasEscape = false
				sb.WriteByte(options[i])
			}
			i++
		}

		res = append(res, sb.String())
	}

	return res
}

// splitOption splits the given opt argument into substrings separated by '='.
// It returns an error if the given option does not comply with the pattern
// "key=value" and the number of elements in the result is not two.
// splitOption removes the prefix from the key and replaces '-' with '_' so
// "--option-name=value" becomes [option_name, value].
func splitOption(opt, prefix string) (string, string, error) {
	kv := strings.Split(opt, "=")

	if len(kv) != 2 {
		return "", "", errors.Newf(
			"option %q is invalid, check '='", opt)
	}

	kv[0] = strings.TrimPrefix(kv[0], prefix)

	return strings.ReplaceAll(kv[0], "-", "_"), kv[1], nil
}

// EncodeExtendedOptions encodes the given options into a string that can be
// used as a value for the special option "options".
func EncodeExtendedOptions(options url.Values) string {
	keys := make([]string, 0, len(options))
	for k := range options {
		keys = append(keys, k)
	}
	// sort keys to make the output deterministic.
	sort.Strings(keys)

	var sb strings.Builder
	for _, k := range keys {
		v := options[k]
		if len(v) == 0 {
			continue
		}
		fmt.Fprintf(&sb, "-c%s=%s ", escapeSpaces(k), escapeSpaces(v[0]))
	}
	return sb.String()
}

func escapeSpaces(v string) string {
	var sb strings.Builder
	for _, r := range v {
		if unicode.IsSpace(r) || r == '\\' {
			sb.WriteByte('\\')
		}
		sb.WriteRune(r)
	}
	return sb.String()
}
