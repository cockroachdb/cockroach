// Copyright 2018 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package changefeedccl

import (
	"testing"
	"unicode/utf8"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func TestSQLNameToKafkaName(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	tests := []struct {
		sql, kafka string
	}{
		{`foo`, `foo`},
		{`abcdefghijklmnopqrstuvwxyz`, `abcdefghijklmnopqrstuvwxyz`},
		{`ABCDEFGHIJKLMNOPQRSTUVWXYZ`, `ABCDEFGHIJKLMNOPQRSTUVWXYZ`},
		{`0123456789_-.`, `0123456789_-.`},
		{`!`, `_u0021_`},
		{`!@#$%^&*()`, `_u0021__u0040__u0023__u0024__u0025__u005e__u0026__u002a__u0028__u0029_`},
		{`foo!`, `foo_u0021_`},
		{`!bar`, `_u0021_bar`},
		{`foo!bar`, `foo_u0021_bar`},
		{`foo_u0021_bar`, `foo_u005f__u0075__u0030__u0030__u0032__u0031__u005f_bar`},
		{`/`, `_u002f_`},
		{`☃`, `_u2603_`},
		{"\x00", `_u0000_`},
		{string(rune(utf8.RuneSelf)), `_u0080_`},
		{string(rune(utf8.MaxRune)), `_u0010ffff_`},
		// special case: exact match of . and .. are disallowed by kafka
		{`.`, `_u002e_`},
		{`..`, `_u002e__u002e_`},
	}
	for i, test := range tests {
		if k := SQLNameToKafkaName(test.sql); k != test.kafka {
			t.Errorf(`%d: %s did not escape to %s got %s`, i, test.sql, test.kafka, k)
		}
		if s := KafkaNameToSQLName(test.kafka); s != test.sql {
			t.Errorf(`%d: %s did not unescape to %s got %s`, i, test.kafka, test.sql, s)
		}
	}
	// We don't produce capital letters in escapes but check them anyway.
	require.Equal(t, `/`, KafkaNameToSQLName(`_u2F_`))
}

func TestSQLNameToAvroName(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	tests := []struct {
		sql, avro string
	}{
		{`foo`, `foo`},
		{`abcdefghijklmnopqrstuvwxyz`, `abcdefghijklmnopqrstuvwxyz`},
		{`ABCDEFGHIJKLMNOPQRSTUVWXYZ`, `ABCDEFGHIJKLMNOPQRSTUVWXYZ`},
		// special case: avro disallows 0-9 in the first character, but allows them otherwise
		{`0123456789_-.`, `_u0030_123456789__u002d__u002e_`},
		{`99`, `_u0039_9`},
		{`!`, `_u0021_`},
		{`!@#$%^&*()`, `_u0021__u0040__u0023__u0024__u0025__u005e__u0026__u002a__u0028__u0029_`},
		{`foo!`, `foo_u0021_`},
		{`!bar`, `_u0021_bar`},
		{`foo!bar`, `foo_u0021_bar`},
		{`foo_u0021_bar`, `foo_u005f__u0075__u0030__u0030__u0032__u0031__u005f_bar`},
		{`/`, `_u002f_`},
		{`☃`, `_u2603_`},
		{"\x00", `_u0000_`},
		{string(rune(utf8.RuneSelf)), `_u0080_`},
		{string(rune(utf8.MaxRune)), `_u0010ffff_`},
	}
	for i, test := range tests {
		if a := SQLNameToAvroName(test.sql); a != test.avro {
			t.Errorf(`%d: %s did not escape to %s got %s`, i, test.sql, test.avro, a)
		}
		if s := AvroNameToSQLName(test.avro); s != test.sql {
			t.Errorf(`%d: %s did not unescape to %s got %s`, i, test.avro, test.sql, s)
		}
	}
	// We don't produce capital letters in escapes but check them anyway.
	require.Equal(t, `/`, KafkaNameToSQLName(`_u2F_`))
}
