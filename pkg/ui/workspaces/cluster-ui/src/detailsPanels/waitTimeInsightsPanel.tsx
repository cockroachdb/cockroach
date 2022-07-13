// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { Col, Row } from "antd";
import "antd/lib/col/style";
import "antd/lib/row/style";
import classNames from "classnames/bind";
import React from "react";
import { SummaryCard, SummaryCardItem } from "src/summaryCard";

import { ContendedExecution, ExecutionType } from "src/activeExecutions";
import { Duration } from "../util";

import { Heading, Text } from "@cockroachlabs/ui-components";
import { ExecutionContentionTable } from "../activeExecutions/activeTransactionsTable/execContentionTable";
import styles from "../statementDetails/statementDetails.module.scss";
import { capitalize } from "../activeExecutions/execTableCommon";
const cx = classNames.bind(styles);

export const WaitTimeInsightsLabels = {
  SECTION_HEADING: "Wait Time Insights",
  BLOCKED_SCHEMA: "Blocked Schema",
  BLOCKED_DATABASE: "Blocked Database",
  BLOCKED_TABLE: "Blocked Table",
  BLOCKED_INDEX: "Blocked Index",
  BLOCKED_ROW: "Blocked Row",
  WAIT_TIME: "Time Spent Waiting",
  BLOCKING_TXNS_TABLE_TITLE: (id: string, execType: ExecutionType): string =>
    `${capitalize(execType)} ID: ${id} waiting on`,
  WAITING_TXNS_TABLE_TITLE: (id: string, execType: ExecutionType): string =>
    `${capitalize(execType)}s waiting for ID: ${id}`,
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
    <section className={cx("section", "section--container")}>
      <Row gutter={24}>
        <Col>
          <Heading type="h5">{WaitTimeInsightsLabels.SECTION_HEADING}</Heading>
          {showWaitTimeInsightsDetails && (
            <Row gutter={24}>
              {" "}
              <Col className="gutter-row" span={12}>
                <SummaryCard className={cx("summary-card")}>
                  <SummaryCardItem
                    label={WaitTimeInsightsLabels.WAIT_TIME}
                    value={
                      waitTime ? Duration(waitTime.milliseconds() * 1e6) : "N/A"
                    }
                  />
                  <SummaryCardItem
                    label={WaitTimeInsightsLabels.BLOCKED_SCHEMA}
                    value={schemaName}
                  />
                  <SummaryCardItem
                    label={WaitTimeInsightsLabels.BLOCKED_DATABASE}
                    value={databaseName}
                  />
                </SummaryCard>
              </Col>
              <Col className="gutter-row" span={12}>
                <SummaryCard className={cx("summary-card")}>
                  <SummaryCardItem
                    label={WaitTimeInsightsLabels.BLOCKED_TABLE}
                    value={tableName}
                  />
                  <SummaryCardItem
                    label={WaitTimeInsightsLabels.BLOCKED_INDEX}
                    value={indexName}
                  />
                </SummaryCard>
              </Col>
            </Row>
          )}
          {blockingExecutions.length > 0 && (
            <Row>
              <Text>
                {WaitTimeInsightsLabels.BLOCKING_TXNS_TABLE_TITLE(
                  executionID,
                  execType,
                )}
              </Text>
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
              <Text>
                {WaitTimeInsightsLabels.WAITING_TXNS_TABLE_TITLE(
                  executionID,
                  execType,
                )}
              </Text>
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
