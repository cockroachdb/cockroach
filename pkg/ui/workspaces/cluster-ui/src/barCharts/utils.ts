// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import * as protos from "@cockroachlabs/crdb-protobuf-client";
import { format as d3Format } from "d3-format";

import { TransactionInfo } from "../transactionsTable";

type StatementStatistics =
  protos.cockroach.server.serverpb.StatementsResponse.ICollectedStatementStatistics;
type Transaction =
  protos.cockroach.server.serverpb.StatementsResponse.IExtendedCollectedTransactionStatistics;

export const clamp = (i: number) => (i < 0 ? 0 : i);

export const formatTwoPlaces = d3Format(".2f");

export function bar<
  T extends StatementStatistics | Transaction | TransactionInfo,
>(name: string, value: (d: T) => number) {
  return { name, value };
}

export const SCALE_FACTORS: { factor: number; key: string }[] = [
  { factor: 1000000000, key: "b" },
  { factor: 1000000, key: "m" },
  { factor: 1000, key: "k" },
];

export function approximify(value: number): string {
  for (let i = 0; i < SCALE_FACTORS.length; i++) {
    const scale = SCALE_FACTORS[i];
    if (value > scale.factor) {
      return "" + Math.round(value / scale.factor) + scale.key;
    }
  }

  return "" + Math.round(value);
}

/**
 * normalizeClosedDomain increases collapsed domain when start and end range are equal.
 * @description
 * This function preserves behavior introduced by following issue in d3-scale library (starting from 2.2 version)
 * https://github.com/d3/d3-scale/issues/117
 * It is expected for scaling within closed domain range, the start value is returned.
 * @example
 * scaleLinear().domain([0, 0])(0) // --> 0.5
 * scaleLinear().domain(normalizeClosedDomain([0, 0]))(0) // --> 0
 */
export function normalizeClosedDomain([d0, d1]: Tuple<number>): Tuple<number> {
  if (d0 === d1) {
    return [d0, d1 + 1];
  }
  return [d0, d1];
}
