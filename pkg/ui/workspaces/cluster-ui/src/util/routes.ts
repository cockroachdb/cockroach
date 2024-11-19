// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

export const DB_PAGE_PATH = "/databases";
export const databaseDetailsPagePath = (dbId: number) => `/databases/${dbId}`;
export const tableDetailsPagePath = (tableId: number) => `/table/${tableId}`;
