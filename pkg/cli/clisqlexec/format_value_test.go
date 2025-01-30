// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package clisqlexec_test

import "github.com/cockroachdb/cockroach/pkg/cli"

func Example_sql_format() {
	c := cli.NewCLITest(cli.TestCLIParams{})
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
	// Check that tuples and user-defined types are escaped correctly.
	c.RunWithArgs([]string{"sql", "-e", `create table tup (a text, b int8)`})
	c.RunWithArgs([]string{"sql", "-e", `select row('\n',1), row('\\n',2), '("\n",3)'::tup, '("\\n",4)'::tup`})
	c.RunWithArgs([]string{"sql", "-e", `select '{"(n,1)","(\n,2)","(\\n,3)"}'::tup[]`})
	c.RunWithArgs([]string{"sql", "-e", `create type e as enum('a', '\n')`})
	c.RunWithArgs([]string{"sql", "-e", `select '\n'::e, '{a, "\\n"}'::e[]`})
	// Check that intervals are formatted correctly.
	c.RunWithArgs([]string{"sql", "-e", `select '1 day 2 minutes'::interval`})
	c.RunWithArgs([]string{"sql", "-e", `select '{3 months 4 days 2 hours, 4 years 1 second}'::interval[]`})
	c.RunWithArgs([]string{"sql", "-e", `SET intervalstyle = sql_standard; select '1 day 2 minutes'::interval`})
	c.RunWithArgs([]string{"sql", "-e", `SET intervalstyle = sql_standard; select '{3 months 4 days 2 hours,4 years 1 second}'::interval[]`})
	c.RunWithArgs([]string{"sql", "-e", `SET intervalstyle = iso_8601; select '1 day 2 minutes'::interval`})
	c.RunWithArgs([]string{"sql", "-e", `SET intervalstyle = iso_8601; select '{3 months 4 days 2 hours,4 years 1 second}'::interval[]`})
	// Check that UUIDs are formatted correctly.
	c.RunWithArgs([]string{"sql", "-e", `select 'f2b046d8-dc59-11ec-8b22-cbf9a9dd2f5f'::uuid`})

	// Output:
	// sql -e create database t; create table t.times (bare timestamp, withtz timestamptz)
	// CREATE DATABASE
	// NOTICE: auto-committing transaction before processing DDL due to autocommit_before_ddl setting
	// CREATE TABLE
	// sql -e insert into t.times values ('2016-01-25 10:10:10', '2016-01-25 10:10:10-05:00')
	// INSERT 0 1
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
	// {t,f}	{01:01:00}	{2021-03-20}
	// sql -e select array[123::int2], array[123::int4], array[123::int8]
	// array	array	array
	// {123}	{123}	{123}
	// sql -e create table tup (a text, b int8)
	// CREATE TABLE
	// sql -e select row('\n',1), row('\\n',2), '("\n",3)'::tup, '("\\n",4)'::tup
	// row	row	tup	tup
	// "(""\\n"",1)"	"(""\\\\n"",2)"	(n,3)	"(""\\n"",4)"
	// sql -e select '{"(n,1)","(\n,2)","(\\n,3)"}'::tup[]
	// tup
	// "{""(n,1)"",""(n,2)"",""(n,3)""}"
	// sql -e create type e as enum('a', '\n')
	// CREATE TYPE
	// sql -e select '\n'::e, '{a, "\\n"}'::e[]
	// e	e
	// \n	"{a,""\\n""}"
	// sql -e select '1 day 2 minutes'::interval
	// interval
	// 1 day 00:02:00
	// sql -e select '{3 months 4 days 2 hours, 4 years 1 second}'::interval[]
	// interval
	// "{""3 mons 4 days 02:00:00"",""4 years 00:00:01""}"
	// sql -e SET intervalstyle = sql_standard; select '1 day 2 minutes'::interval
	// SET
	// interval
	// 1 0:02:00
	// sql -e SET intervalstyle = sql_standard; select '{3 months 4 days 2 hours,4 years 1 second}'::interval[]
	// SET
	// interval
	// "{""+0-3 +4 +2:00:00"",""+4-0 +0 +0:00:01""}"
	// sql -e SET intervalstyle = iso_8601; select '1 day 2 minutes'::interval
	// SET
	// interval
	// P1DT2M
	// sql -e SET intervalstyle = iso_8601; select '{3 months 4 days 2 hours,4 years 1 second}'::interval[]
	// SET
	// interval
	// {P3M4DT2H,P4YT1S}
	// sql -e select 'f2b046d8-dc59-11ec-8b22-cbf9a9dd2f5f'::uuid
	// uuid
	// f2b046d8-dc59-11ec-8b22-cbf9a9dd2f5f
}
