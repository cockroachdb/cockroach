// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.
import { Icon } from "@cockroachlabs/ui-components";
import { Row, Skeleton } from "antd";
import moment from "moment-timezone";
import React from "react";

import { Timestamp } from "../../timestamp";
import { DATE_WITH_SECONDS_FORMAT_24_TZ } from "../../util";
import { Tooltip } from "../tooltip";

import styles from "./tableMetadataLastUpdatedTooltip.module.scss";

const TABLE_METADATA_LAST_UPDATED_HELP =
  "Data was last refreshed automatically (per cluster setting) or manually.";

const formatErrorMessage = (lastUpdatedTime: moment.Moment | null) => {
  return (
    <>
      Last refresh failed to retrieve data about this table. The data shown is
      as of{" "}
      <Timestamp
        format={DATE_WITH_SECONDS_FORMAT_24_TZ}
        time={lastUpdatedTime}
        fallback={"Never"}
      />
      .
    </>
  );
};

type Props = {
  timestamp?: moment.Moment | null;
  children: (
    formattedRelativeTime: React.ReactNode,
    icon?: JSX.Element,
  ) => React.ReactNode;
  hasError?: boolean;
  loading?: boolean;
};

export const TableMetadataLastUpdatedTooltip = ({
  timestamp,
  hasError,
  children,
  loading,
}: Props) => {
  const duration = (
    <span>
      <Skeleton
        paragraph={false}
        title={{ width: 80 }}
        active
        loading={loading}
      >
        {timestamp?.fromNow() ?? "Never"}
      </Skeleton>
    </span>
  );

  const icon = hasError ? (
    <Icon fill={"warning"} iconName={"Caution"} />
  ) : (
    <Icon fill="info" iconName={"InfoCircle"} />
  );

  return (
    <Tooltip
      title={
        <div>
          {hasError ? (
            formatErrorMessage(timestamp)
          ) : (
            <>
              {timestamp && (
                <Timestamp
                  format={DATE_WITH_SECONDS_FORMAT_24_TZ}
                  time={timestamp}
                  fallback={"Never"}
                />
              )}
              <br />
              {TABLE_METADATA_LAST_UPDATED_HELP}
            </>
          )}
        </div>
      }
    >
      <Row className={styles["table-metadata-tooltip-content"]} align="middle">
        {children(duration, icon)}
      </Row>
    </Tooltip>
  );
};
