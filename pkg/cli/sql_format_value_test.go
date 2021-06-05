// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package cli

func Example_sql_format() {
	c := NewCLITest(TestCLIParams{})
	defer c.Cleanup()

	c.RunWithArgs([]string{"sql", "-e", "create database t; create table t.times (bare timestamp, withtz timestamptz)"})
	c.RunWithArgs([]string{"sql", "-e", "insert into t.times values ('2016-01-25 10:10:10', '2016-01-25 10:10:10-05:00')"})
	c.RunWithArgs([]string{"sql", "-e", "select bare from t.times; select withtz from t.times"})
	c.RunWithArgs([]string{"sql", "-e",
		"select '2021-03-20'::date; select '01:01'::time; select '01:01'::timetz; select '01:01+02:02'::timetz"})
	c.RunWithArgs([]string{"sql", "-e", "select (1/3.0)::real; select (1/3.0)::double precision; select '-inf'::float8"})
	// Special characters inside arrays used to be represented as escaped bytes.
	c.RunWithArgs([]string{"sql", "-e", "select array['哈哈'::TEXT], array['哈哈'::NAME], array['哈哈'::VARCHAR]"})
	c.RunWithArgs([]string{"sql", "-e", "select array['哈哈'::CHAR(2)], array['哈'::\"char\"]"})
	// Preserve quoting of arrays containing commas or double quotes.
	c.RunWithArgs([]string{"sql", "-e", `select array['a,b', 'a"b', 'a\b']`, "--format=table"})
	// Infinities inside float arrays used to be represented differently from infinities as simpler scalar.
	c.RunWithArgs([]string{"sql", "-e", "select array['Inf'::FLOAT4, '-Inf'::FLOAT4], array['Inf'::FLOAT8]"})
	// Sanity check for other array types.
	c.RunWithArgs([]string{"sql", "-e", "select array[true, false], array['01:01'::time], array['2021-03-20'::date]"})
	c.RunWithArgs([]string{"sql", "-e", "select array[123::int2], array[123::int4], array[123::int8]"})

	// Output:
	// sql -e create database t; create table t.times (bare timestamp, withtz timestamptz)
	// CREATE TABLE
	// sql -e insert into t.times values ('2016-01-25 10:10:10', '2016-01-25 10:10:10-05:00')
	// INSERT 1
	// sql -e select bare from t.times; select withtz from t.times
	// bare
	// 2016-01-25 10:10:10
	// withtz
	// 2016-01-25 15:10:10+00
	// sql -e select '2021-03-20'::date; select '01:01'::time; select '01:01'::timetz; select '01:01+02:02'::timetz
	// date
	// 2021-03-20
	// time
	// 01:01:00
	// timetz
	// 01:01:00+00
	// timetz
	// 01:01:00+02:02
	// sql -e select (1/3.0)::real; select (1/3.0)::double precision; select '-inf'::float8
	// float4
	// 0.33333334
	// float8
	// 0.3333333333333333
	// float8
	// -Infinity
	// sql -e select array['哈哈'::TEXT], array['哈哈'::NAME], array['哈哈'::VARCHAR]
	// array	array	array
	// {哈哈}	{哈哈}	{哈哈}
	// sql -e select array['哈哈'::CHAR(2)], array['哈'::"char"]
	// array	array
	// {哈哈}	{哈}
	// sql -e select array['a,b', 'a"b', 'a\b'] --format=table
	//           array
	// -------------------------
	//   {"a,b","a\"b","a\\b"}
	// (1 row)
	// sql -e select array['Inf'::FLOAT4, '-Inf'::FLOAT4], array['Inf'::FLOAT8]
	// array	array
	// {Infinity,-Infinity}	{Infinity}
	// sql -e select array[true, false], array['01:01'::time], array['2021-03-20'::date]
	// array	array	array
	// {true,false}	{01:01:00}	{2021-03-20}
	// sql -e select array[123::int2], array[123::int4], array[123::int8]
	// array	array	array
	// {123}	{123}	{123}
}
