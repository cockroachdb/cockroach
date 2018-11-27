// Copyright 2018 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package changefeedccl

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"unicode/utf8"
)

var escapeRE = regexp.MustCompile(`_u[0-9a-fA-F]{2,8}_`)
var kafkaDisallowedRE = regexp.MustCompile(`[^a-zA-Z0-9\._\-]`)
var avroDisallowedRE = regexp.MustCompile(`[^A-Za-z0-9_]`)

func escapeRune(r rune) string {
	if r <= 1<<16 {
		return fmt.Sprintf(`_u%04x_`, r)
	}
	return fmt.Sprintf(`_u%08x_`, r)
}

// SQLNameToKafkaName escapes a sql table name into a valid kafka topic name.
// This is reversible by KafkaNameToSQLName except when the escaped string is
// longer than kafka's length limit.
//
// Kafka allows names matching `[a-zA-Z0-9\._\-]{1,249}` excepting `.` and `..`.
//
// Runes are escaped with _u<hex>_ in an attempt to look like U+0021. For
// example `!` escapes to `_u0021_`.
func SQLNameToKafkaName(s string) string {
	if s == `.` {
		return escapeRune('.')
	} else if s == `..` {
		return escapeRune('.') + escapeRune('.')
	}
	s = escapeSQLName(s, kafkaDisallowedRE)
	if len(s) > 249 {
		// Not going to roundtrip, but not much we can do about that.
		return s[:249]
	}
	return s
}

// KafkaNameToSQLName is the inverse of SQLNameToKafkaName except when
// SQLNameToKafkaName had to truncate.
func KafkaNameToSQLName(s string) string {
	return unescapeSQLName(s)
}

// SQLNameToAvroName escapes a sql table name into a valid avro record or field
// name. This is reversible by AvroNameToSQLName.
//
// Avro allows names matching `[a-zA-Z_][a-zA-Z0-9_]*`.
//
// Runes are escaped with _u<hex>_ in an attempt to look like U+0021. For
// example `!` escapes to `_u0021_`.
func SQLNameToAvroName(s string) string {
	r, firstSize := utf8.DecodeRuneInString(s)
	if r == utf8.RuneError {
		// Invalid or empty string. Not much we can do here.
		return s
	}
	// Avro disallows a leading 0-9, but allows them otherwise.
	if r >= '0' && r <= '9' {
		return escapeRune(r) + escapeSQLName(s[firstSize:], avroDisallowedRE)
	}
	return escapeSQLName(s, avroDisallowedRE)
}

// AvroNameToSQLName is the inverse of SQLNameToAvroName.
func AvroNameToSQLName(s string) string {
	return unescapeSQLName(s)
}

func escapeSQLName(s string, disallowedRE *regexp.Regexp) string {
	// First replace anything that looks like an escape, so we can roundtrip.
	s = escapeRE.ReplaceAllStringFunc(s, func(match string) string {
		var ret strings.Builder
		for _, r := range match {
			ret.WriteString(escapeRune(r))
		}
		return ret.String()
	})
	// Then replace anything disallowed.
	s = disallowedRE.ReplaceAllStringFunc(s, func(match string) string {
		var ret strings.Builder
		for _, r := range match {
			ret.WriteString(escapeRune(r))
		}
		return ret.String()
	})
	return s
}

func unescapeSQLName(s string) string {
	var buf [utf8.UTFMax]byte
	s = escapeRE.ReplaceAllStringFunc(s, func(match string) string {
		// Cut off the `_u` prefix and the `_` suffix.
		hex := match[2 : len(match)-1]
		r, err := strconv.ParseInt(hex, 16, 32)
		if err != nil {
			// Should be unreachable.
			return match
		}
		n := utf8.EncodeRune(buf[:utf8.UTFMax], rune(r))
		return string(buf[:n])
	})
	return s
}
