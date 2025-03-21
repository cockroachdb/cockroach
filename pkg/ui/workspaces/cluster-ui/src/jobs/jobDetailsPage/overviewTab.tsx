// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.
//
import { Col, Row } from "antd";
import classNames from "classnames/bind";
import React from "react";

import { JobResponse } from "src/api/jobsApi";
import { EmptyTable } from "src/empty";
import jobStyles from "src/jobs/jobs.module.scss";
import { SortedTable } from "src/sortedtable";
import { SummaryCard, SummaryCardItem } from "src/summaryCard";
import summaryCardStyles from "src/summaryCard/summaryCard.module.scss";
import { Text, TextTypes } from "src/text";
import {
  DATE_WITH_SECONDS_FORMAT,
  DATE_WITH_SECONDS_FORMAT_24_TZ,
  TimestampToMoment,
} from "src/util";

import { Timestamp } from "../../timestamp";
import { HighwaterTimestamp, JobStatusCell } from "../util";

type JobMessage = JobResponse["messages"][number];
const cardCx = classNames.bind(summaryCardStyles);
const jobCx = classNames.bind(jobStyles);

interface OverviewTabContentProps {
  job: JobResponse | null;
}

export default function OverviewTabContent({
  job,
}: OverviewTabContentProps): React.ReactElement {
  if (!job) {
    return null;
  }

  return (
    <Row gutter={24}>
      <Col className="gutter-row" span={8}>
        <Text textType={TextTypes.Heading5} className={jobCx("details-header")}>
          Details
        </Text>
        <SummaryCard className={cardCx("summary-card")}>
          <SummaryCardItem
            label="Status"
            value={
              <JobStatusCell job={job} lineWidth={1.5} hideDuration={true} />
            }
          />
          <SummaryCardItem
            label="Creation Time"
            value={
              <Timestamp
                time={TimestampToMoment(job.created, null)}
                format={DATE_WITH_SECONDS_FORMAT_24_TZ}
              />
            }
          />
          {job.modified && (
            <SummaryCardItem
              label="Last Modified Time"
              value={
                <Timestamp
                  time={TimestampToMoment(job.modified, null)}
                  format={DATE_WITH_SECONDS_FORMAT_24_TZ}
                />
              }
            />
          )}
          {job.finished && (
            <SummaryCardItem
              label="Completed Time"
              value={
                <Timestamp
                  time={TimestampToMoment(job.finished, null)}
                  format={DATE_WITH_SECONDS_FORMAT_24_TZ}
                />
              }
            />
          )}
          <SummaryCardItem label="User Name" value={job.username} />
          {job.highwater_timestamp && (
            <SummaryCardItem
              label="High-water Timestamp"
              value={
                <HighwaterTimestamp
                  timestamp={job.highwater_timestamp}
                  decimalString={job.highwater_decimal}
                />
              }
            />
          )}
          <SummaryCardItem
            label="Coordinator Node"
            value={
              job.coordinator_id.isZero() ? "-" : job.coordinator_id.toString()
            }
          />
        </SummaryCard>
      </Col>
      <Col className="gutter-row" span={16}>
        <Text textType={TextTypes.Heading5} className={jobCx("details-header")}>
          Events
        </Text>
        <SummaryCard className={jobCx("messages-card")}>
          <SortedTable
            data={job.messages}
            columns={messageColumns}
            tableWrapperClassName={jobCx("job-messages", "sorted-table")}
            renderNoResult={<EmptyTable title="No messages recorded." />}
          />
        </SummaryCard>
      </Col>
    </Row>
  );
}

const messageColumns = [
  {
    name: "timestamp",
    title: "When",
    hideTitleUnderline: true,
    cell: (x: JobMessage) => (
      <Timestamp
        time={TimestampToMoment(x.timestamp, null)}
        format={DATE_WITH_SECONDS_FORMAT}
      />
    ),
  },
  {
    name: "kind",
    title: "Kind",
    hideTitleUnderline: true,
    cell: (x: JobMessage) => x.kind,
  },
  {
    name: "message",
    title: "Message",
    hideTitleUnderline: true,
    cell: (x: JobMessage) => <p className={jobCx("message")}>{x.message}</p>,
  },
];
