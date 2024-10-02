// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import moment from "moment-timezone";

export const indexNeverUsedReason =
  "This index has not been used and can be removed for better write performance.";

const minDate = moment.utc("0001-01-01"); // minimum value as per UTC.

type DropIndexRecommendation = {
  recommend: boolean;
  reason: string;
};

export interface IndexUsageStatistic {
  created_at?: string;
  last_read?: string;
  unused_threshold: string;
}

export function recommendDropUnusedIndex(
  indexUsageStat: IndexUsageStatistic,
): DropIndexRecommendation {
  const createdAt = indexUsageStat.created_at
    ? moment.utc(indexUsageStat.created_at)
    : minDate;
  const lastRead = indexUsageStat.last_read
    ? moment.utc(indexUsageStat.last_read)
    : minDate;
  let lastActive = createdAt;
  if (lastActive.isSame(minDate) && !lastRead.isSame(minDate)) {
    lastActive = lastRead;
  }

  if (lastActive.isSame(minDate)) {
    return { recommend: true, reason: indexNeverUsedReason };
  }

  const unusedThreshold = moment.duration(
    "PT" + indexUsageStat.unused_threshold.toUpperCase(),
  );
  return {
    recommend: true,
    reason: `This index has not been used in over ${formatMomentDuration(
      unusedThreshold,
    )} and can be removed for better write performance.`,
  };
}

export function formatMomentDuration(duration: moment.Duration): string {
  const numSecondsInMinute = 60;
  const numMinutesInHour = 60;
  const numHoursInDay = 24;

  const seconds = Math.floor(duration.as("s")) % numSecondsInMinute;
  const minutes = Math.floor(duration.as("m")) % numMinutesInHour;
  const hours = Math.floor(duration.as("h")) % numHoursInDay;
  const days = Math.floor(duration.as("d"));

  const daysSubstring = days > 0 ? `${days} days, ` : "";
  const hoursSubstring = hours > 0 ? `${hours} hours, ` : "";
  const minutesSubstring = minutes > 0 ? `${minutes} minutes, ` : "";
  const secondsSubstring = seconds > 0 ? `${seconds} seconds, ` : "";

  return `${daysSubstring}${hoursSubstring}${minutesSubstring}${secondsSubstring}`;
}
