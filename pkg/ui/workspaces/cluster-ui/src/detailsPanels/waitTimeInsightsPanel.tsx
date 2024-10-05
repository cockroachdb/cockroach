// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { Col, Row } from "antd";
import "antd/lib/col/style";
import "antd/lib/row/style";
import classNames from "classnames/bind";
import React from "react";
import { SummaryCard, SummaryCardItem } from "src/summaryCard";

import { ContendedExecution, ExecutionType } from "src/activeExecutions";
import { capitalize, Duration, NO_SAMPLES_FOUND } from "../util";

import { Heading } from "@cockroachlabs/ui-components";
import { ExecutionContentionTable } from "../activeExecutions/activeTransactionsTable/execContentionTable";
import styles from "../statementDetails/statementDetails.module.scss";

const cx = classNames.bind(styles);

export const WaitTimeInsightsLabels = {
  SECTION_HEADING: "Contention Insights",
  BLOCKED_SCHEMA: "Blocked Schema",
  BLOCKED_DATABASE: "Blocked Database",
  BLOCKED_TABLE: "Blocked Table",
  BLOCKED_INDEX: "Blocked Index",
  BLOCKED_ROW: "Blocked Row",
  CONTENDED_KEY: "Contended Key",
  WAIT_TIME: "Time Spent Waiting",
  BLOCKING_TXNS_TABLE_TITLE: (id: string, execType: ExecutionType): string =>
    `${capitalize(execType)} ID: ${id} waiting on`,
  WAITING_TXNS_TABLE_TITLE: (id: string, execType: ExecutionType): string =>
    `${capitalize(execType)}s waiting for ID: ${id}`,
  BLOCKED_TXNS_TABLE_TITLE: (id: string, execType: ExecutionType): string =>
    `${capitalize(execType)} with ID ${id} waited on`,
  WAITED_TXNS_TABLE_TITLE: (id: string, execType: ExecutionType): string =>
    `${capitalize(execType)}s that waited for ${capitalize(
      execType,
    )}s with ID ${id}`,
};

type WaitTimeInsightsPanelProps = {
  executionID: string;
  execType: ExecutionType;
  databaseName?: string;
  schemaName?: string;
  tableName?: string;
  indexName?: string;
  waitTime?: moment.Duration;
  waitingExecutions: ContendedExecution[];
  blockingExecutions: ContendedExecution[];
};

export const WaitTimeInsightsPanel: React.FC<WaitTimeInsightsPanelProps> = ({
  executionID,
  execType,
  databaseName,
  schemaName,
  tableName,
  indexName,
  waitTime,
  waitingExecutions,
  blockingExecutions,
}) => {
  const showWaitTimeInsightsDetails = waitTime != null;

  return (
    <section
      className={cx("section", "section--container", "margin-bottom-large")}
    >
      <Row gutter={24}>
        <Col>
          <Heading type="h5" className={cx("margin-header")}>
            {WaitTimeInsightsLabels.SECTION_HEADING}
          </Heading>
          {showWaitTimeInsightsDetails && (
            <Row gutter={24}>
              {" "}
              <Col className="gutter-row" span={12}>
                <SummaryCard className={cx("summary-card")}>
                  <SummaryCardItem
                    label={WaitTimeInsightsLabels.WAIT_TIME}
                    value={
                      waitTime
                        ? Duration(waitTime.asMilliseconds() * 1e6)
                        : NO_SAMPLES_FOUND
                    }
                  />
                  {schemaName && (
                    <SummaryCardItem
                      label={WaitTimeInsightsLabels.BLOCKED_SCHEMA}
                      value={schemaName}
                    />
                  )}
                  {databaseName && (
                    <SummaryCardItem
                      label={WaitTimeInsightsLabels.BLOCKED_DATABASE}
                      value={databaseName}
                    />
                  )}
                </SummaryCard>
              </Col>
              {tableName && (
                <Col className="gutter-row" span={12}>
                  <SummaryCard className={cx("summary-card")}>
                    <SummaryCardItem
                      label={WaitTimeInsightsLabels.BLOCKED_TABLE}
                      value={tableName}
                    />
                    {indexName && (
                      <SummaryCardItem
                        label={WaitTimeInsightsLabels.BLOCKED_INDEX}
                        value={indexName}
                      />
                    )}
                  </SummaryCard>
                </Col>
              )}
            </Row>
          )}
          {blockingExecutions.length > 0 && (
            <Row>
              <Heading type="h5" className={cx("margin-header")}>
                {WaitTimeInsightsLabels.BLOCKING_TXNS_TABLE_TITLE(
                  executionID,
                  execType,
                )}
              </Heading>
              <div>
                <ExecutionContentionTable
                  execType={execType}
                  data={blockingExecutions}
                />
              </div>
            </Row>
          )}
          {waitingExecutions.length > 0 && (
            <Row>
              <Heading type="h5" className={cx("margin-header")}>
                {WaitTimeInsightsLabels.WAITING_TXNS_TABLE_TITLE(
                  executionID,
                  execType,
                )}
              </Heading>
              <div>
                <ExecutionContentionTable
                  execType={execType}
                  data={waitingExecutions}
                />
              </div>
            </Row>
          )}
        </Col>
      </Row>
    </section>
  );
};
