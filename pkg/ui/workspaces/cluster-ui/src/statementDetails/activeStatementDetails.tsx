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
import { Text } from "@cockroachlabs/ui-components";
import { commonStyles } from "src/common";
import classNames from "classnames/bind";
import { Link, useHistory, match } from "react-router-dom";
import { Col, Row } from "antd";
import "antd/lib/col/style";
import "antd/lib/row/style";
import { SummaryCard } from "src/summaryCard";

import summaryCardStyles from "src/summaryCard/summaryCard.module.scss";
import { getMatchParamByName } from "src/util/query";
import { executionIdAttr } from "../util/constants";
import { ActiveStatement } from "src/activeExecutions";
import { StatusIcon } from "src/activeExecutions/statusIcon";

import styles from "./statementDetails.module.scss";
import { SqlBox } from "src/sql/box";
const cx = classNames.bind(styles);
const summaryCardStylesCx = classNames.bind(summaryCardStyles);

export type ActiveStatementDetailsStateProps = {
  statement: ActiveStatement;
  match: match;
};

export type ActiveStatementDetailsDispatchProps = {
  refreshSessions: () => void;
};

export type ActiveStatementDetailsProps = ActiveStatementDetailsStateProps &
  ActiveStatementDetailsDispatchProps;

export const ActiveStatementDetails: React.FC<ActiveStatementDetailsProps> = ({
  statement,
  match,
  refreshSessions,
}) => {
  const history = useHistory();
  const executionID = getMatchParamByName(match, executionIdAttr);

  useEffect(() => {
    if (statement == null) {
      // Refresh sessions if the statement was not found initially.
      refreshSessions();
    }
  }, [refreshSessions, statement]);

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
            <SqlBox value={statement?.query || "SQL Execution not found."} />
          </Col>
        </Row>
        {statement && (
          <Row gutter={24}>
            <Col className="gutter-row" span={12}>
              <SummaryCard className={cx("summary-card")}>
                <Row>
                  <Col>
                    <div className={summaryCardStylesCx("summary--card__item")}>
                      <Text>Start Time (UTC)</Text>
                      <Text>
                        {statement.start.format("MMM D, YYYY [at] H:mm (UTC)")}
                      </Text>
                    </div>
                    <div className={summaryCardStylesCx("summary--card__item")}>
                      <Text>Elapsed Time</Text>
                      <Text>{statement.elapsedTimeSeconds} s</Text>
                    </div>
                    <div className={summaryCardStylesCx("summary--card__item")}>
                      <Text>Status</Text>
                      <Text>
                        <StatusIcon status={statement.status} />
                        {statement.status}
                      </Text>
                    </div>
                  </Col>
                </Row>
              </SummaryCard>
            </Col>
            <Col className="gutter-row" span={12}>
              <SummaryCard className={cx("summary-card")}>
                <div className={summaryCardStylesCx("summary--card__item")}>
                  <Text>Application Name</Text>
                  {statement.application}
                </div>
                <div className={summaryCardStylesCx("summary--card__item")}>
                  <Text>User Name</Text>
                  {statement.user}
                </div>
                <div className={summaryCardStylesCx("summary--card__item")}>
                  <Text>Client Address</Text>
                  {statement.clientAddress}
                </div>
                <p className={summaryCardStylesCx("summary--card__divider")} />
                <div className={summaryCardStylesCx("summary--card__item")}>
                  <Text>Session ID</Text>
                  <Link
                    className={cx("text-link")}
                    to={`/session/${statement.sessionID}`}
                  >
                    {statement.sessionID}
                  </Link>
                </div>
                <div className={summaryCardStylesCx("summary--card__item")}>
                  <Text>Transaction Execution ID</Text>
                  <Link
                    className={cx("text-link")}
                    to={`/execution/transaction/${statement.transactionID}`}
                  >
                    {statement.transactionID}
                  </Link>
                </div>
              </SummaryCard>
            </Col>
          </Row>
        )}
      </section>
    </div>
  );
};
