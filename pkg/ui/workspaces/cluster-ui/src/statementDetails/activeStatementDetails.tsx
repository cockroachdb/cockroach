// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import React, { useEffect } from "react";
import { ArrowLeft } from "@cockroachlabs/icons";
import { Button } from "src/button";
import Helmet from "react-helmet";
import { commonStyles } from "src/common";
import classNames from "classnames/bind";
import { Link, useHistory, match } from "react-router-dom";
import { Col, Row } from "antd";
import "antd/lib/col/style";
import "antd/lib/row/style";
import { SummaryCard, SummaryCardItem } from "src/summaryCard";

import summaryCardStyles from "src/summaryCard/summaryCard.module.scss";
import { getMatchParamByName } from "src/util/query";
import { executionIdAttr, DATE_FORMAT_24_UTC, Duration } from "../util";
import {
  ActiveStatement,
  ExecutionContentionDetails,
} from "src/activeExecutions";
import { StatusIcon } from "src/activeExecutions/statusIcon";

import styles from "./statementDetails.module.scss";
import { SqlBox, SqlBoxSize } from "src/sql/box";
import { WaitTimeInsightsPanel } from "src/detailsPanels/waitTimeInsightsPanel";
const cx = classNames.bind(styles);
const summaryCardStylesCx = classNames.bind(summaryCardStyles);

export type ActiveStatementDetailsStateProps = {
  contentionDetails?: ExecutionContentionDetails;
  statement: ActiveStatement;
  match: match;
};

export type ActiveStatementDetailsDispatchProps = {
  refreshLiveWorkload: () => void;
};

export type ActiveStatementDetailsProps = ActiveStatementDetailsStateProps &
  ActiveStatementDetailsDispatchProps;

export const ActiveStatementDetails: React.FC<ActiveStatementDetailsProps> = ({
  contentionDetails,
  statement,
  match,
  refreshLiveWorkload,
}) => {
  const history = useHistory();
  const executionID = getMatchParamByName(match, executionIdAttr);

  useEffect(() => {
    if (statement == null) {
      // Refresh sessions if the statement was not found initially.
      refreshLiveWorkload();
    }
  }, [refreshLiveWorkload, statement]);

  const returnToActiveStatements = () => {
    history.push("/sql-activity?tab=Statements&view=active");
  };

  return (
    <div className={cx("root")}>
      <Helmet title={`Details`} />
      <div className={cx("section", "page--header")}>
        <Button
          onClick={returnToActiveStatements}
          type="unstyled-link"
          size="small"
          icon={<ArrowLeft fontSize={"10px"} />}
          iconPosition="left"
          className="small-margin"
        >
          Active Statements
        </Button>
        <h3 className={commonStyles("base-heading", "no-margin-bottom")}>
          Statement Execution ID:{" "}
          <span className={cx("heading-execution-id")}>{executionID}</span>
        </h3>
      </div>
      <section className={cx("section", "section--container")}>
        <Row gutter={24}>
          <Col className="gutter-row" span={24}>
            <SqlBox
              value={statement?.query || "SQL Execution not found."}
              size={SqlBoxSize.custom}
            />
          </Col>
        </Row>
        {statement && (
          <Row gutter={24}>
            <Col className="gutter-row" span={12}>
              <SummaryCard className={cx("summary-card")}>
                <Row>
                  <Col>
                    <SummaryCardItem
                      label="Start Time (UTC)"
                      value={statement.start.format(DATE_FORMAT_24_UTC)}
                    />
                    <SummaryCardItem
                      label="Elapsed Time"
                      value={Duration(statement.elapsedTimeMillis * 1e6)}
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
                  </Col>
                </Row>
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
        )}
      </section>
      {statement && contentionDetails && (
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
    </div>
  );
};
