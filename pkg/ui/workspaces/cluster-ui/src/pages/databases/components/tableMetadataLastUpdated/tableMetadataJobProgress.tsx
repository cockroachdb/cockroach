// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import moment from "moment-timezone";
import React from "react";

import { Timestamp } from "src/timestamp";
import { DATE_WITH_SECONDS_FORMAT_24_TZ } from "src/util";

import styles from "./tableMetadataJobProgress.module.scss";

type Props = {
  jobStartedTime: moment.Moment;
  jobProgressFraction: number; // Between 0 and 1.
};

// This message is meant to be displayed when the job is running.
export const TableMetadataJobProgress: React.FC<Props> = ({
  jobStartedTime,
  jobProgressFraction,
}) => {
  const percentDone = Math.round(jobProgressFraction * 100);
  return (
    <div>
      Refreshing data
      <ul className={styles["progress-list"]}>
        <li>{percentDone}% done</li>
        <li>
          Started at{" "}
          <Timestamp
            time={jobStartedTime}
            format={DATE_WITH_SECONDS_FORMAT_24_TZ}
          />
        </li>
      </ul>
    </div>
  );
};
