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

import { TransactionContentionDetails } from "src/activeExecutions";
import { Duration } from "../util";

import { Heading, Text } from "@cockroachlabs/ui-components";
import { TransactionContentionTable } from "../activeExecutions/activeTransactionsTable/transactionContentionTable";
import styles from "../statementDetails/statementDetails.module.scss";
const cx = classNames.bind(styles);

export const WaitTimeInsightsLabels = {
  SECTION_HEADING: "Wait Time Insights",
  WAIT_TIME: "Time Spent Waiting",
  BLOCKED_SCHEMA: "Blocked Schema",
  BLOCKED_DATABASE: "Blocked Database",
  BLOCKED_TABLE: "Blocked Table",
  BLOCKED_INDEX: "Blocked Index",
  BLOCKED_ROW: "Blocked Row",
  BLOCKING_TXNS_TABLE_TITLE: (id: string) => `Transaction ID: ${id} waiting on`,
  WAITING_TXNS_TABLE_TITLE: (id: string) =>
    `Transactions waiting for ID: ${id}`,
};

type TransactionContentionDetailsSectionProps = {
  transactionExecutionID: string;
  contentionDetails: TransactionContentionDetails | null;
};

export const TransactionContentionDetailsSection: React.FC<
  TransactionContentionDetailsSectionProps
> = ({ contentionDetails, transactionExecutionID }) => {
  if (!contentionDetails) return null;

  const waitTimeInsights = contentionDetails.waitInsights;
  console.log(contentionDetails);

  return (
    <section className={cx("section", "section--container")}>
      <Row gutter={24}>
        <Col>
          <Heading type="h5">{WaitTimeInsightsLabels.SECTION_HEADING}</Heading>
          {waitTimeInsights && (
            <Row gutter={24}>
              {" "}
              <Col className="gutter-row" span={12}>
                <SummaryCard className={cx("summary-card")}>
                  <SummaryCardItem
                    label={WaitTimeInsightsLabels.WAIT_TIME}
                    value={
                      waitTimeInsights.waitTime
                        ? Duration(waitTimeInsights.waitTime.seconds() * 1e9)
                        : "N/A"
                    }
                  />
                  <SummaryCardItem
                    label={WaitTimeInsightsLabels.BLOCKED_SCHEMA}
                    value={waitTimeInsights.schemaName}
                  />
                  <SummaryCardItem
                    label={WaitTimeInsightsLabels.BLOCKED_DATABASE}
                    value={waitTimeInsights.databaseName}
                  />
                </SummaryCard>
              </Col>
              <Col className="gutter-row" span={12}>
                <SummaryCard className={cx("summary-card")}>
                  <SummaryCardItem
                    label={WaitTimeInsightsLabels.BLOCKED_TABLE}
                    value={waitTimeInsights.tableName}
                  />
                  <SummaryCardItem
                    label={WaitTimeInsightsLabels.BLOCKED_INDEX}
                    value={waitTimeInsights.indexName}
                  />
                </SummaryCard>
              </Col>
            </Row>
          )}
          {contentionDetails.blockingTxns.length > 0 && (
            <Row>
              <Text>
                {WaitTimeInsightsLabels.BLOCKING_TXNS_TABLE_TITLE(
                  transactionExecutionID,
                )}
              </Text>
              <div>
                <TransactionContentionTable
                  data={contentionDetails.blockingTxns}
                />
              </div>
            </Row>
          )}
          {contentionDetails.waitingTxns.length > 0 && (
            <Row>
              <Text>
                {WaitTimeInsightsLabels.WAITING_TXNS_TABLE_TITLE(
                  transactionExecutionID,
                )}
              </Text>
              <div>
                <TransactionContentionTable
                  data={contentionDetails.waitingTxns}
                />
              </div>
            </Row>
          )}
        </Col>
      </Row>
    </section>
  );
};
