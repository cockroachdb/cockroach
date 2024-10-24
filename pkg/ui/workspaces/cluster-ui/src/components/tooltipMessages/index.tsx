// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import Link from "antd/es/typography/Link";
import React from "react";

import { tableStatsClusterSetting } from "../../util";

export const AUTO_STATS_COLLECTION_HELP = (
  <span>
    Automatic statistics can help improve query performance. Learn how to{" "}
    <Link strong underline href={tableStatsClusterSetting} target="_blank">
      <span>manage statistics collection</span>
    </Link>{" "}
    .
  </span>
);

export * from "./tableMetadataLastUpdatedTooltip";
