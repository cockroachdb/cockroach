// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import React from "react";
import { Link } from "react-router-dom";

import { Tooltip } from "src/components/tooltip";
import { TableColumnProps } from "src/sharedFromCloud/table";

import { FingerprintRow } from "./types";

export const COLUMNS: TableColumnProps<FingerprintRow>[] = [
  {
    title: (
      <Tooltip title={"The unique identifier for this fingerprint."}>
        Fingerprint ID
      </Tooltip>
    ),
    render: (fp: FingerprintRow) => {
      return (
        <Link to={`/sql-activity?q=${encodeURIComponent(fp.fingerprint)}`}>
          <span style={{ fontFamily: "monospace" }}>{fp.fingerprint}</span>
        </Link>
      );
    },
  },
  {
    title: (
      <Tooltip title={"The SQL statement fingerprint text."}>
        Fingerprint
      </Tooltip>
    ),
    render: (fp: FingerprintRow) => {
      return <span style={{ fontFamily: "monospace" }}>{fp.query}</span>;
    },
    width: "50%",
  },
  {
    title: (
      <Tooltip title={"A short summary of the fingerprint."}>Summary</Tooltip>
    ),
    render: (fp: FingerprintRow) => {
      return fp.summary;
    },
  },
  {
    title: (
      <Tooltip
        title={"The database where this statement fingerprint was recorded."}
      >
        Database
      </Tooltip>
    ),
    render: (fp: FingerprintRow) => {
      return fp.database;
    },
  },
  {
    title: (
      <Tooltip
        title={"Whether the statement was executed in an implicit transaction."}
      >
        Implicit Txn
      </Tooltip>
    ),
    render: (fp: FingerprintRow) => {
      return fp.implicitTxn ? "Yes" : "No";
    },
    align: "center",
  },
  {
    title: (
      <Tooltip title={"When this fingerprint was first recorded."}>
        Created At
      </Tooltip>
    ),
    render: (fp: FingerprintRow) => {
      return fp.createdAt.format("YYYY-MM-DD HH:mm:ss");
    },
    align: "right",
  },
];
