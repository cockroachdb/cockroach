// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { StatementInsightsViewProps } from "./statementInsightsView";
import moment from "moment";

export const statementInsightsPropsFixture: StatementInsightsViewProps = {
  onColumnsChange: x => {},
  selectedColumnNames: [],
  statements: [
    {
      statementID: "f72f37ea-b3a0-451f-80b8-dfb27d0bc2a9",
      statementFingerprintID: "abc",
      transactionFingerprintID: "defg",
      transactionID: "f72f37ea-b3a0-451f-80b8-dfb27d0bc2a5",
      implicitTxn: true,
      query:
        "SELECT IFNULL(a, b) FROM (SELECT (SELECT code FROM promo_codes WHERE code > $1 ORDER BY code LIMIT _) AS a, (SELECT code FROM promo_codes ORDER BY code LIMIT _) AS b)",
      startTime: moment.utc("2022.08.10"),
      endTime: moment.utc("2022.08.10 00:00:00.25"),
      elapsedTimeMillis: moment.duration("00:00:00.25").asMilliseconds(),
      application: "demo",
      databaseName: "test",
      username: "username test",
      lastRetryReason: null,
      isFullScan: false,
      retries: 0,
      problem: "SlowExecution",
      causes: ["HighContention"],
      priority: "high",
      sessionID: "103",
      timeSpentWaiting: null,
      rowsWritten: 0,
      rowsRead: 100,
      insights: null,
      indexRecommendations: ["make this index"],
      planGist: "abc",
    },
    {
      statementID: "f72f37ea-b3a0-451f-80b8-dfb27d0bc2a9",
      statementFingerprintID: "938x3",
      transactionFingerprintID: "1971x3",
      transactionID: "e72f37ea-b3a0-451f-80b8-dfb27d0bc2a5",
      implicitTxn: true,
      query: "INSERT INTO vehicles VALUES ($1, $2, __more1_10__)",
      startTime: moment.utc("2022.08.10"),
      endTime: moment.utc("2022.08.10 00:00:00.25"),
      elapsedTimeMillis: moment.duration("00:00:00.25").asMilliseconds(),
      application: "demo",
      databaseName: "test",
      username: "username test",
      lastRetryReason: null,
      isFullScan: false,
      retries: 0,
      problem: "SlowExecution",
      causes: ["HighContention"],
      priority: "high",
      sessionID: "103",
      timeSpentWaiting: null,
      rowsWritten: 0,
      rowsRead: 100,
      insights: null,
      indexRecommendations: ["make that index"],
      planGist: "abc",
    },
    {
      statementID: "f72f37ea-b3a0-451f-80b8-dfb27d0bc2a9",
      statementFingerprintID: "hisas",
      transactionFingerprintID: "3anc",
      transactionID: "f72f37ea-b3a0-451f-80b8-dfb27d0bc2a0",
      implicitTxn: true,
      query:
        "UPSERT INTO vehicle_location_histories VALUES ($1, $2, now(), $3, $4)",
      startTime: moment.utc("2022.08.10"),
      endTime: moment.utc("2022.08.10 00:00:00.25"),
      elapsedTimeMillis: moment.duration("00:00:00.25").asMilliseconds(),
      application: "demo",
      databaseName: "test",
      username: "username test",
      lastRetryReason: null,
      isFullScan: false,
      retries: 0,
      problem: "SlowExecution",
      causes: ["HighContention"],
      priority: "high",
      sessionID: "103",
      timeSpentWaiting: null,
      rowsWritten: 0,
      rowsRead: 100,
      insights: null,
      indexRecommendations: ["make these indices"],
      planGist: "abc",
    },
  ],
  statementsError: null,
  sortSetting: {
    ascending: false,
    columnTitle: "startTime",
  },
  filters: {
    app: "",
  },
  refreshStatementInsights: () => {},
  onSortChange: () => {},
  onFiltersChange: () => {},
  setTimeScale: () => {},
};
