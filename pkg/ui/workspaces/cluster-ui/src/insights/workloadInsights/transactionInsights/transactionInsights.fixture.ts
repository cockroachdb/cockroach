// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { TransactionInsightsViewProps } from "./transactionInsightsView";
import moment from "moment";
import { InsightExecEnum } from "../../types";

export const transactionInsightsPropsFixture: TransactionInsightsViewProps = {
  transactions: [
    {
      transactionID: "f72f37ea-b3a0-451f-80b8-dfb27d0bc2a5",
      fingerprintID: "\\x76245b7acd82d39d",
      queries: [
        "SELECT IFNULL(a, b) FROM (SELECT (SELECT code FROM promo_codes WHERE code > $1 ORDER BY code LIMIT _) AS a, (SELECT code FROM promo_codes ORDER BY code LIMIT _) AS b)",
      ],
      insightName: "HighContention",
      startTime: moment.utc("2022.08.10"),
      contentionDuration: moment.duration("00:00:00.25"),
      application: "demo",
      execType: InsightExecEnum.TRANSACTION,
      contentionThreshold: moment.duration("00:00:00.1").asMilliseconds(),
    },
    {
      transactionID: "e72f37ea-b3a0-451f-80b8-dfb27d0bc2a5",
      fingerprintID: "\\x76245b7acd82d39e",
      queries: [
        "INSERT INTO vehicles VALUES ($1, $2, __more1_10__)",
        "INSERT INTO vehicles VALUES ($1, $2, __more1_10__)",
      ],
      insightName: "HighContention",
      startTime: moment.utc("2022.08.10"),
      contentionDuration: moment.duration("00:00:00.25"),
      application: "demo",
      execType: InsightExecEnum.TRANSACTION,
      contentionThreshold: moment.duration("00:00:00.1").asMilliseconds(),
    },
    {
      transactionID: "f72f37ea-b3a0-451f-80b8-dfb27d0bc2a0",
      fingerprintID: "\\x76245b7acd82d39f",
      queries: [
        "UPSERT INTO vehicle_location_histories VALUES ($1, $2, now(), $3, $4)",
      ],
      insightName: "HighContention",
      startTime: moment.utc("2022.08.10"),
      contentionDuration: moment.duration("00:00:00.25"),
      application: "demo",
      execType: InsightExecEnum.TRANSACTION,
      contentionThreshold: moment.duration("00:00:00.1").asMilliseconds(),
    },
  ],
  transactionsError: null,
  sortSetting: {
    ascending: false,
    columnTitle: "startTime",
  },
  filters: {
    app: "",
  },
  refreshTransactionInsights: () => {},
  onSortChange: () => {},
  onFiltersChange: () => {},
  setTimeScale: () => {},
};
