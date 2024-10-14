// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import React from "react";

import { Anchor } from "src/anchor";
import {
  distSql,
  vectorizedExecution,
  mergeJoin,
  lookupJoin,
  hashJoin,
  invertedJoin,
  indexJoin,
  fullScan,
  secondaryIndex,
  lockingStrength,
  transactionLayerOverview,
} from "src/util/docs";

type TooltipMap = Record<string, JSX.Element>;

const attributeTooltips: TooltipMap = {
  autoCommit: (
    <span>
      All statements are handled as transactions.{" "}
      <Anchor href={transactionLayerOverview}>Autocommit</Anchor> indicates that
      the single statement was implicitly committed.
    </span>
  ),
  distribution: (
    <span>
      CockroachDB supports{" "}
      <Anchor href={distSql}>two modes of execution </Anchor>
      for SQL queries: local and distributed. In local execution, data is pulled
      from where it is towards the one node that does the processing. In
      distributed execution, the processing is shipped to run close to where the
      data is stored, usually on multiple nodes simultaneously.
    </span>
  ),
  lockingStrength: (
    <span>
      <Anchor href={lockingStrength}>Locking strength</Anchor> indicates that
      additional locks were applied to rows during execution to improve
      performance for workloads having high contention.
    </span>
  ),
  vectorized: (
    <span>
      CockroachDB supports a{" "}
      <Anchor href={vectorizedExecution}>vectorized execution engine.</Anchor>
    </span>
  ),
};

const operatorTooltips: TooltipMap = {
  filter: (
    <span>
      The filter operator indicates the statement contains a WHERE clause.
    </span>
  ),
  group: (
    <span>
      The group operator indicates the statement contains a GROUP BY clause.
    </span>
  ),
  hashJoin: (
    <span>
      <Anchor href={hashJoin}>Hash join</Anchor> is used when merge join cannot
      be used. CockroachDB will create a hash table based on the smaller table
      and then scan the larger table, looking up each row in the hash table.
    </span>
  ),
  indexJoin: (
    <span>
      <Anchor href={indexJoin}>Index join</Anchor> is suboptimal and indicates a
      non-covering index where not all columns are present in the index to
      satisfy the query. Index join requires columns to additionally be read
      from the primary index.
    </span>
  ),
  insertFastPath: (
    <span>The insert operator indicates an INSERT statement.</span>
  ),
  invertedJoin: (
    <span>
      <Anchor href={invertedJoin}>Inverted join</Anchor> forces the optimizer to
      use a join using an inverted index on the right side of the join. Inverted
      joins can only be used with INNER and LEFT joins.
    </span>
  ),
  lookupJoin: (
    <span>
      <Anchor href={lookupJoin}>Lookup join</Anchor> is used where there is a
      large imbalance in size between the tables. The larger table must be
      indexed on the equality column. Each row in the small table is read where
      the rows are &apos;looked up&apos; in the larger table for matches.
    </span>
  ),
  mergeJoin: (
    <span>
      <Anchor href={mergeJoin}>Merge join</Anchor> is selected when both tables
      are indexed on the equality columns where indexes must have the same
      ordering.
    </span>
  ),
  scan: (
    <span>
      The scan (or reverse scan) operator indicates which and how the table(s)
      in the statement&apos;s FROM clause and its index is read.
    </span>
  ),
  sort: (
    <span>
      The sort operator indicates the statement contains an ORDER BY clause.
    </span>
  ),
  update: <span>The update operator indicates an UPDATE statement.</span>,
  window: (
    <span>
      The window operator indicates the statement contains a OVER or WINDOW
      clause for window functions.
    </span>
  ),
};

const attributeValueTooltips: TooltipMap = {
  fullScan: (
    <span>
      A <Anchor href={fullScan}>FULL SCAN</Anchor> indicates that{" "}
      <strong>all</strong> rows were read from the table index.
      <br />
      <strong>Recommendation:</strong> Consider{" "}
      <Anchor href={secondaryIndex}>adding a secondary index</Anchor> to the
      table if the FULL SCAN is retrieving a relatively large number of rows.
    </span>
  ),
};

export function getOperatorTooltip(key: string): React.ReactElement {
  return operatorTooltips[key] || null;
}

export function getAttributeTooltip(key: string): JSX.Element {
  return attributeTooltips[key] || null;
}

export function getAttributeValueTooltip(key: string): JSX.Element {
  return attributeValueTooltips[key] || null;
}
