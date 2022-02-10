// Copyright 2021 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package multiregionccl_test

import (
	"testing"

	// Blank import partitionccl to install CreatePartitioning hook.
	_ "github.com/cockroachdb/cockroach/pkg/ccl/partitionccl"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltestutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

func TestShowCreateTable(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testCases := []sqltestutils.ShowCreateTableTestCase{
		// Check GLOBAL tables are round trippable.
		{
			CreateStatement: `CREATE TABLE %s (
				a INT
			) LOCALITY GLOBAL`,
			Expect: `CREATE TABLE public.%[1]s (
	a INT8 NULL,
	rowid INT8 NOT VISIBLE NOT NULL DEFAULT unique_rowid(),
	CONSTRAINT %[1]s_pkey PRIMARY KEY (rowid ASC)
) LOCALITY GLOBAL`,
			Database: "mrdb",
		},
		// Check REGIONAL BY TABLE tables are round trippable.
		{
			CreateStatement: `CREATE TABLE %s (
				a INT
			) LOCALITY REGIONAL BY TABLE`,
			Expect: `CREATE TABLE public.%[1]s (
	a INT8 NULL,
	rowid INT8 NOT VISIBLE NOT NULL DEFAULT unique_rowid(),
	CONSTRAINT %[1]s_pkey PRIMARY KEY (rowid ASC)
) LOCALITY REGIONAL BY TABLE IN PRIMARY REGION`,
			Database: "mrdb",
		},
		{
			CreateStatement: `CREATE TABLE %s (
				a INT
			) LOCALITY REGIONAL BY TABLE IN "us-west1"`,
			Expect: `CREATE TABLE public.%[1]s (
	a INT8 NULL,
	rowid INT8 NOT VISIBLE NOT NULL DEFAULT unique_rowid(),
	CONSTRAINT %[1]s_pkey PRIMARY KEY (rowid ASC)
) LOCALITY REGIONAL BY TABLE IN "us-west1"`,
			Database: "mrdb",
		},
		// Check REGIONAL BY ROW tests are round trippable.
		{
			CreateStatement: `SET experimental_enable_implicit_column_partitioning = true; CREATE TABLE %s (
				a INT,
				INDEX a_idx (a)
			) LOCALITY REGIONAL BY ROW`,
			Expect: `CREATE TABLE public.%[1]s (
	a INT8 NULL,
	crdb_region public.crdb_internal_region NOT VISIBLE NOT NULL DEFAULT default_to_database_primary_region(gateway_region())::public.crdb_internal_region,
	rowid INT8 NOT VISIBLE NOT NULL DEFAULT unique_rowid(),
	CONSTRAINT %[1]s_pkey PRIMARY KEY (rowid ASC),
	INDEX a_idx (a ASC)
) LOCALITY REGIONAL BY ROW`,
			Database: "mrdb",
		},
		{
			CreateStatement: `SET experimental_enable_implicit_column_partitioning = true; CREATE TABLE %s (
				a INT,
				crdb_region_col crdb_internal_region,
				INDEX a_idx (a)
			) LOCALITY REGIONAL BY ROW AS crdb_region_col`,
			Expect: `CREATE TABLE public.%[1]s (
	a INT8 NULL,
	crdb_region_col public.crdb_internal_region NOT NULL,
	rowid INT8 NOT VISIBLE NOT NULL DEFAULT unique_rowid(),
	CONSTRAINT %[1]s_pkey PRIMARY KEY (rowid ASC),
	INDEX a_idx (a ASC)
) LOCALITY REGIONAL BY ROW AS crdb_region_col`,
			Database: "mrdb",
		},
		{
			CreateStatement: `SET experimental_enable_implicit_column_partitioning = true; CREATE TABLE %s (
				a INT,
				crdb_region_col crdb_internal_region,
				INDEX a_idx (a) WHERE a > 0
			) LOCALITY REGIONAL BY ROW AS crdb_region_col`,
			Expect: `CREATE TABLE public.%[1]s (
	a INT8 NULL,
	crdb_region_col public.crdb_internal_region NOT NULL,
	rowid INT8 NOT VISIBLE NOT NULL DEFAULT unique_rowid(),
	CONSTRAINT %[1]s_pkey PRIMARY KEY (rowid ASC),
	INDEX a_idx (a ASC) WHERE a > 0:::INT8
) LOCALITY REGIONAL BY ROW AS crdb_region_col`,
			Database: "mrdb",
		},
	}
	sqltestutils.ShowCreateTableTest(
		t,
		`CREATE DATABASE mrdb PRIMARY REGION = "us-west1"`,
		testCases,
	)
}
