// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { ArrowLeft } from "@cockroachlabs/icons";
import { Col, Row } from "antd";
import classNames from "classnames/bind";
import React, { useEffect } from "react";
import Helmet from "react-helmet";
import { Link, match, useHistory } from "react-router-dom";

import {
  ActiveTransaction,
  ExecutionContentionDetails,
} from "src/activeExecutions";
import { StatusIcon } from "src/activeExecutions/statusIcon";
import { Button } from "src/button";
import { commonStyles } from "src/common";
import { WaitTimeInsightsPanel } from "src/detailsPanels/waitTimeInsightsPanel";
import { SqlBox, SqlBoxSize } from "src/sql/box";
import { SummaryCard, SummaryCardItem } from "src/summaryCard";
import summaryCardStyles from "src/summaryCard/summaryCard.module.scss";
import { executionIdAttr, DATE_FORMAT_24_TZ } from "src/util";
import { getMatchParamByName } from "src/util/query";

import styles from "../statementDetails/statementDetails.module.scss";
import { Timestamp } from "../timestamp";
import { capitalize, Duration } from "../util/format";
const cx = classNames.bind(styles);
const summaryCardStylesCx = classNames.bind(summaryCardStyles);

export type ActiveTransactionDetailsStateProps = {
  transaction: ActiveTransaction;
  contentionDetails?: ExecutionContentionDetails;
  match: match;
};

export type ActiveTransactionDetailsDispatchProps = {
  refreshLiveWorkload: () => void;
};

const BACK_TO_ACTIVE_TXNS_BUTTON_LABEL = "Active Transactions";
const TXN_EXECUTION_ID_LABEL = "Transaction Execution ID";

export const ActiveTxnInsightsLabels = {
  START_TIME: "Start Time",
  ELAPSED_TIME: "Elapsed Time",
  STATUS: "Status",
  RETRY_COUNT: "Internal Retries",
  RETRY_REASON: "Last Retry Reason",
  STATEMENT_COUNT: "Number of Statements",
  APPLICATION_NAME: "Application Name",
  LAST_STATEMENT_EXEC_ID: "Most Recent Statement Execution ID",
  SESSION_ID: "Session ID",
  PRIORITY: "Priority",
};

export const RECENT_STATEMENT_NOT_FOUND_MESSAGE =
  "Most recent statement not found.";

export type ActiveTransactionDetailsProps = ActiveTransactionDetailsStateProps &
  ActiveTransactionDetailsDispatchProps;

export const ActiveTransactionDetails: React.FC<
  ActiveTransactionDetailsProps
> = ({ transaction, contentionDetails, match, refreshLiveWorkload }) => {
  const history = useHistory();
  const executionID = getMatchParamByName(match, executionIdAttr);

  useEffect(() => {
    if (transaction == null) {
      // Refresh sessions and cluster lock info  if the transaction was not found initially.
      refreshLiveWorkload();
    }
  }, [refreshLiveWorkload, transaction]);

  const returnToActiveTransactions = () => {
    history.push("/sql-activity?tab=Transactions&view=active");
  };

  return (
    <div className={cx("root")}>
      <Helmet title={`Details`} />
      <div className={cx("section", "page--header")}>
        <Button
          onClick={returnToActiveTransactions}
          type="unstyled-link"
          size="small"
          icon={<ArrowLeft fontSize={"10px"} />}
          iconPosition="left"
          className="small-margin"
        >
          {BACK_TO_ACTIVE_TXNS_BUTTON_LABEL}
        </Button>
        <h3 className={commonStyles("base-heading", "no-margin-bottom")}>
          {TXN_EXECUTION_ID_LABEL}:{" "}
          <span className={cx("heading-execution-id")}>{executionID}</span>
        </h3>
      </div>
      <section className={cx("section", "section--container")}>
        <Row gutter={24}>
          <Col className="gutter-row" span={24}>
            <SqlBox
              value={transaction?.query || RECENT_STATEMENT_NOT_FOUND_MESSAGE}
              size={SqlBoxSize.CUSTOM}
            />
          </Col>
        </Row>
        {transaction && (
          <Row gutter={24}>
            <Col className="gutter-row" span={12}>
              <SummaryCard className={cx("summary-card")}>
                <SummaryCardItem
                  label={ActiveTxnInsightsLabels.START_TIME}
                  value={
                    <Timestamp
                      time={transaction.start}
                      format={DATE_FORMAT_24_TZ}
                    />
                  }
                />
                <SummaryCardItem
                  label={ActiveTxnInsightsLabels.ELAPSED_TIME}
                  value={Duration(
                    transaction.elapsedTime.asMilliseconds() * 1e6,
                  )}
                />
                <SummaryCardItem
                  label={ActiveTxnInsightsLabels.STATUS}
                  value={
                    <>
                      <StatusIcon status={transaction.status} />
                      {transaction.status}
                    </>
                  }
                />
                <SummaryCardItem
                  label={ActiveTxnInsightsLabels.PRIORITY}
                  value={capitalize(transaction.priority)}
                />
              </SummaryCard>
            </Col>
            <Col className="gutter-row" span={12}>
              <SummaryCard className={cx("summary-card")}>
                <SummaryCardItem
                  label={ActiveTxnInsightsLabels.RETRY_COUNT}
                  value={transaction.retries}
                />
                <SummaryCardItem
                  label={ActiveTxnInsightsLabels.RETRY_REASON}
                  value={transaction.lastAutoRetryReason || "N/A"}
                />
                <SummaryCardItem
                  label={ActiveTxnInsightsLabels.STATEMENT_COUNT}
                  value={transaction.statementCount}
                />
                <SummaryCardItem
                  label={ActiveTxnInsightsLabels.APPLICATION_NAME}
                  value={transaction.application}
                />
                <p className={summaryCardStylesCx("summary--card__divider")} />
                {transaction.statementID && (
                  <SummaryCardItem
                    label={ActiveTxnInsightsLabels.LAST_STATEMENT_EXEC_ID}
                    value={
                      <Link
                        className={cx("text-link")}
                        to={`/execution/statement/${transaction.statementID}`}
                      >
                        {transaction.statementID}
                      </Link>
                    }
                  />
                )}

                <SummaryCardItem
                  label={ActiveTxnInsightsLabels.SESSION_ID}
                  value={
                    <Link
                      className={cx("text-link")}
                      to={`/session/${transaction.sessionID}`}
                    >
                      {transaction.sessionID}
                    </Link>
                  }
                />
              </SummaryCard>
            </Col>
          </Row>
        )}
      </section>
      {transaction && contentionDetails && (
        <WaitTimeInsightsPanel
          execType="transaction"
          executionID={transaction.transactionID}
          schemaName={contentionDetails.waitInsights?.schemaName}
          tableName={contentionDetails.waitInsights?.tableName}
          indexName={contentionDetails.waitInsights?.indexName}
          databaseName={contentionDetails.waitInsights?.databaseName}
          waitTime={contentionDetails.waitInsights?.waitTime}
          waitingExecutions={contentionDetails.waitingExecutions}
          blockingExecutions={contentionDetails.blockingExecutions}
        />
      )}
    </div>
  );
};
