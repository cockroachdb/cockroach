// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import moment from "moment-timezone";

import { ClusterIndexUsageStatistic } from "../../api";

import {
  formatMomentDuration,
  indexNeverUsedReason,
  recommendDropUnusedIndex,
} from "./indexUsageStatsRec";

describe("recommendDropUnusedIndex", () => {
  const mockCurrentTime = moment();
  const oneHourAgo: moment.Moment = moment(mockCurrentTime).subtract(1, "hour");

  describe("Never Used Index", () => {
    const neverUsedIndex: ClusterIndexUsageStatistic = {
      table_id: 1,
      index_id: 1,
      last_read: null,
      created_at: null,
      index_name: "recent_index",
      table_name: "test_table",
      database_id: 1,
      database_name: "test_db",
      schema_name: "public",
      unused_threshold: "10h0m0s",
    };
    it("should recommend index to be dropped with the reason that the index is never used", () => {
      expect(recommendDropUnusedIndex(neverUsedIndex)).toEqual({
        recommend: true,
        reason: indexNeverUsedReason,
      });
    });
  });
  describe("Index Last Use Exceeds Duration Threshold", () => {
    const exceedsDurationIndex: ClusterIndexUsageStatistic = {
      table_id: 1,
      index_id: 1,
      last_read: moment.utc(oneHourAgo, "X").format(),
      created_at: null,
      index_name: "recent_index",
      table_name: "test_table",
      database_id: 1,
      database_name: "test_db",
      schema_name: "public",
      unused_threshold: "0h30m0s",
    };
    it("should recommend index to be dropped with the reason that it has exceeded the configured index unuse duration", () => {
      expect(recommendDropUnusedIndex(exceedsDurationIndex)).toEqual({
        recommend: true,
        reason: `This index has not been used in over ${formatMomentDuration(
          moment.duration(
            "PT" + exceedsDurationIndex.unused_threshold.toUpperCase(),
          ),
        )} and can be removed for better write performance.`,
      });
    });
  });
  describe("Index Created But Never Read", () => {
    describe("creation date exceeds unuse duration", () => {
      const createdNeverReadIndexExceed: ClusterIndexUsageStatistic = {
        table_id: 1,
        index_id: 1,
        last_read: null,
        created_at: moment.utc(oneHourAgo, "X").format(),
        index_name: "recent_index",
        table_name: "test_table",
        database_id: 1,
        database_name: "test_db",
        schema_name: "public",
        unused_threshold: "0h30m0s",
      };
      it("should recommend index to be dropped with the reason that it has exceeded the configured index unuse duration", () => {
        expect(recommendDropUnusedIndex(createdNeverReadIndexExceed)).toEqual({
          recommend: true,
          reason: `This index has not been used in over ${formatMomentDuration(
            moment.duration(
              "PT" + createdNeverReadIndexExceed.unused_threshold.toUpperCase(),
            ),
          )} and can be removed for better write performance.`,
        });
      });
    });
  });
});
