// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import * as protos from "@cockroachlabs/crdb-protobuf-client";
import { scaleLinear } from "d3-scale";

import { stdDevLong } from "src/util";

import { formatTwoPlaces, normalizeClosedDomain } from "./utils";

type StatementStatistics =
  protos.cockroach.server.serverpb.StatementsResponse.ICollectedStatementStatistics;

export function rowsBreakdown(s: StatementStatistics) {
  const mean = s.stats.num_rows.mean;
  const sd = stdDevLong(s.stats.num_rows, s.stats.count);
  const domain = normalizeClosedDomain([0, mean + sd]);

  const scale = scaleLinear().domain(domain).range([0, 100]);

  return {
    rowsBarChart(meanRow?: boolean) {
      const spread = scale(sd + (sd > mean ? mean : sd));
      if (meanRow) {
        return formatTwoPlaces(mean);
      } else {
        return spread;
      }
    },
  };
}
