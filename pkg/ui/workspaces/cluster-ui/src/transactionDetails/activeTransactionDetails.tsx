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
import { SqlBox } from "src/sql/box";
import classNames from "classnames/bind";
import { Link, useHistory, match } from "react-router-dom";
import { Col, Row } from "antd";
import "antd/lib/col/style";
import "antd/lib/row/style";
import { SummaryCard, SummaryCardItem } from "src/summaryCard";

import summaryCardStyles from "src/summaryCard/summaryCard.module.scss";
import { getMatchParamByName } from "src/util/query";
import { executionIdAttr } from "../util";
import { ActiveTransaction } from "src/activeExecutions";
import { StatusIcon } from "src/activeExecutions/statusIcon";

import styles from "../statementDetails/statementDetails.module.scss";
const cx = classNames.bind(styles);
const summaryCardStylesCx = classNames.bind(summaryCardStyles);

export type ActiveTransactionDetailsStateProps = {
  transaction: ActiveTransaction;
  match: match;
};

export type ActiveTransactionDetailsDispatchProps = {
  refreshSessions: () => void;
};

export type ActiveTransactionDetailsProps = ActiveTransactionDetailsStateProps &
  ActiveTransactionDetailsDispatchProps;

export const ActiveTransactionDetails: React.FC<
  ActiveTransactionDetailsProps
> = ({ transaction, match, refreshSessions }) => {
  const history = useHistory();
  const executionID = getMatchParamByName(match, executionIdAttr);

  useEffect(() => {
    if (transaction == null) {
      // Refresh sessions if the transaction was not found initially.
      refreshSessions();
    }
  }, [refreshSessions, transaction]);

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
          Active Transactions
        </Button>
        <h3 className={commonStyles("base-heading", "no-margin-bottom")}>
          Transaction Execution ID:{" "}
          <span className={cx("heading-execution-id")}>{executionID}</span>
        </h3>
      </div>
      <section className={cx("section", "section--container")}>
        <Row gutter={24}>
          <Col className="gutter-row" span={24}>
            <SqlBox
              value={
                transaction?.mostRecentStatement?.query ||
                "Most recent statement not found."
              }
            />
          </Col>
        </Row>
        {transaction && (
          <Row gutter={24}>
            <Col className="gutter-row" span={12}>
              <SummaryCard className={cx("summary-card")}>
                <Row>
                  <Col>
                    <SummaryCardItem
                      label="Start Time (UTC)"
                      value={transaction.start.format(
                        "MMM D, YYYY [at] H:mm (UTC)",
                      )}
                    />
                    <SummaryCardItem
                      label="Elapsed Time"
                      value={`${transaction.elapsedTimeSeconds} s`}
                    />
                    <SummaryCardItem
                      label="Status"
                      value={
                        <>
                          <StatusIcon status={transaction.status} />
                          {transaction.status}
                        </>
                      }
                    />
                  </Col>
                </Row>
              </SummaryCard>
            </Col>
            <Col className="gutter-row" span={12}>
              <SummaryCard className={cx("summary-card")}>
                <SummaryCardItem
                  label="Number of Retries"
                  value={transaction.retries}
                />
                <SummaryCardItem
                  label="Number of Statements"
                  value={transaction.statementCount}
                />
                <SummaryCardItem
                  label="Application Name"
                  value={transaction.application}
                />
                <p className={summaryCardStylesCx("summary--card__divider")} />
                {transaction.mostRecentStatement && (
                  <SummaryCardItem
                    label="Most Recent Statement Execution ID"
                    value={
                      <Link
                        className={cx("text-link")}
                        to={`/execution/statement/${transaction.mostRecentStatement.executionID}`}
                      >
                        {transaction.mostRecentStatement.executionID}
                      </Link>
                    }
                  />
                )}

                <SummaryCardItem
                  label="Session ID"
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
    </div>
  );
};
