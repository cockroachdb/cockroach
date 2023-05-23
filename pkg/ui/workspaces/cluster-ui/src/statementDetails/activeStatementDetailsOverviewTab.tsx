// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import React from "react";
import classNames from "classnames/bind";
import { Link } from "react-router-dom";
import { Col, Row } from "antd";
import { SummaryCard, SummaryCardItem } from "src/summaryCard";
import {
  ActiveStatement,
  ExecutionContentionDetails,
} from "src/activeExecutions";
import { WaitTimeInsightsPanel } from "src/detailsPanels/waitTimeInsightsPanel";
import { StatusIcon } from "src/activeExecutions/statusIcon";
import { DATE_FORMAT_24_TZ, Duration } from "src/util";

import "antd/lib/col/style";
import "antd/lib/row/style";
import summaryCardStyles from "src/summaryCard/summaryCard.module.scss";

const summaryCardStylesCx = classNames.bind(summaryCardStyles);

import styles from "./statementDetails.module.scss";
import { Timestamp } from "../timestamp";
const cx = classNames.bind(styles);

type Props = {
  statement?: ActiveStatement;
  contentionDetails?: ExecutionContentionDetails;
};

export const ActiveStatementDetailsOverviewTab = ({
  statement,
  contentionDetails,
}: Props): React.ReactElement => {
  if (!statement) return null;

  return (
    <>
      <section className={cx("section", "section--container")}>
        <Row gutter={24} type="flex">
          <Col className="gutter-row" span={12}>
            <SummaryCard className={cx("summary-card")}>
              <SummaryCardItem
                label="Start Time"
                value={
                  <Timestamp
                    time={statement.start}
                    format={DATE_FORMAT_24_TZ}
                  />
                }
              />
              <SummaryCardItem
                label="Elapsed Time"
                value={Duration(statement.elapsedTime.asMilliseconds() * 1e6)}
              />
              <SummaryCardItem
                label="Status"
                value={
                  <>
                    <StatusIcon status={statement.status} />
                    {statement.status}
                  </>
                }
              />
              <SummaryCardItem
                label="Full Scan"
                value={statement.isFullScan.toString()}
              />
            </SummaryCard>
          </Col>
          <Col className="gutter-row" span={12}>
            <SummaryCard className={cx("summary-card")}>
              <SummaryCardItem
                label="Application Name"
                value={statement.application}
              />
              <SummaryCardItem label="User Name" value={statement.user} />
              <SummaryCardItem
                label="Client Address"
                value={statement.clientAddress}
              />
              <p className={summaryCardStylesCx("summary--card__divider")} />
              <SummaryCardItem
                label="Session ID"
                value={
                  <Link
                    className={cx("text-link")}
                    to={`/session/${statement.sessionID}`}
                  >
                    {statement.sessionID}
                  </Link>
                }
              />
              <SummaryCardItem
                label="Transaction Execution ID"
                value={
                  <Link
                    className={cx("text-link")}
                    to={`/execution/transaction/${statement.transactionID}`}
                  >
                    {statement.transactionID}
                  </Link>
                }
              />
            </SummaryCard>
          </Col>
        </Row>
      </section>
      {contentionDetails && (
        <WaitTimeInsightsPanel
          execType="statement"
          executionID={statement.statementID}
          schemaName={contentionDetails.waitInsights?.schemaName}
          tableName={contentionDetails.waitInsights?.tableName}
          indexName={contentionDetails.waitInsights?.indexName}
          databaseName={contentionDetails.waitInsights?.databaseName}
          waitTime={contentionDetails.waitInsights?.waitTime}
          waitingExecutions={contentionDetails.waitingExecutions}
          blockingExecutions={contentionDetails.blockingExecutions}
        />
      )}
    </>
  );
};
