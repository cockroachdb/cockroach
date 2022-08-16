// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { ClusterIndexUsageStatistic } from "../../api/schemaInsightsApi";
import moment from "moment";

// defaultUnusedIndexDuration is a week.
const defaultUnusedIndexDuration = moment.duration(7, "days");

const indexNeverUsedReason =
  "This index has not been used and can be removed for better write performance.";

const minDate = moment.utc("0001-01-01"); // minimum value as per UTC.

type dropIndexRecommendation = {
  recommend: boolean;
  reason: string;
};

export function recommendDropUnusedIndex(
  clusterIndexUsageStat: ClusterIndexUsageStatistic,
): dropIndexRecommendation {
  console.log("created at", clusterIndexUsageStat.created_at);
  console.log("last read", clusterIndexUsageStat.last_read);
  const createdAt = clusterIndexUsageStat.created_at
    ? moment.utc(clusterIndexUsageStat.created_at)
    : minDate;
  const lastRead = clusterIndexUsageStat.last_read
    ? moment.utc(clusterIndexUsageStat.last_read)
    : minDate;
  let lastActive = createdAt;
  if (lastActive.isSame(minDate) && !lastRead.isSame(minDate)) {
    lastActive = lastRead;
  }

  if (lastActive.isSame(minDate)) {
    return { recommend: true, reason: indexNeverUsedReason };
  }

  const duration = moment.duration(moment().diff(lastActive));
  const unusedThreshold = moment.duration(
    "PT" + clusterIndexUsageStat.unused_threshold.toUpperCase(),
  );
  console.log("parsed duration", unusedThreshold);
  console.log("formatted duration", formatDuration(unusedThreshold));
  if (duration >= unusedThreshold) {
    return {
      recommend: true,
      reason: `This index has not been used in over ${formatDuration(
        unusedThreshold,
      )} and can be removed for better write performance.`,
    };
  }
  return { recommend: false, reason: "" };
}

function formatDuration(duration: moment.Duration): string {
  // Get days and subtract from duration.
  const days = duration.asDays();
  duration.subtract(moment.duration(days, "days"));

  // Get hours and subtract from duration.
  const hours = duration.hours();
  duration.subtract(moment.duration(hours, "hours"));

  const minutes = duration.minutes();
  return `${days} days, ${hours} hours, and ${minutes} minutes`;
}
