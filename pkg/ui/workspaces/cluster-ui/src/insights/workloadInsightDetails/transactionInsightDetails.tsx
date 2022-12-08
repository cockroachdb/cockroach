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
import Helmet from "react-helmet";
import { RouteComponentProps } from "react-router-dom";
import { ArrowLeft } from "@cockroachlabs/icons";
import { Tabs } from "antd";
import "antd/lib/col/style";
import "antd/lib/row/style";
import { Button } from "src/button";
import { Loading } from "src/loading";
import { getMatchParamByName } from "src/util/query";
import { TxnInsightDetailsRequest } from "src/api";
import { InsightNameEnum, TxnInsightDetails } from "../types";

import { commonStyles } from "src/common";
import { InsightsError } from "../insightsErrorComponent";
import { TimeScale } from "../../timeScaleDropdown";
import { idAttr } from "src/util";
import { TransactionInsightDetailsOverviewTab } from "./transactionInsightDetailsOverviewTab";
import { TransactionInsightsDetailsStmtsTab } from "./transactionInsightDetailsStmtsTab";

import "antd/lib/tabs/style";
import { timeScaleRangeToObject } from "../utils";
export interface TransactionInsightDetailsStateProps {
  insightDetails: TxnInsightDetails;
  insightError: Error[] | null;
  timeScale?: TimeScale;
}

export interface TransactionInsightDetailsDispatchProps {
  refreshTransactionInsightDetails: (req: TxnInsightDetailsRequest) => void;
  setTimeScale: (ts: TimeScale) => void;
}

export type TransactionInsightDetailsProps =
  TransactionInsightDetailsStateProps &
    TransactionInsightDetailsDispatchProps &
    RouteComponentProps<unknown>;

enum TabKeysEnum {
  OVERVIEW = "overview",
  STATEMENTS = "statements",
}

export const TransactionInsightDetails: React.FC<
  TransactionInsightDetailsProps
> = ({
  refreshTransactionInsightDetails,
  setTimeScale,
  history,
  insightDetails,
  insightError,
  timeScale,
  match,
}) => {
  const executionID = getMatchParamByName(match, idAttr);
  const txnDetails = insightDetails.txnDetails;
  const stmts = insightDetails.statements;
  const contentionInfo = insightDetails.blockingContentionDetails;

  useEffect(() => {
    const stmtsComplete =
      stmts != null && stmts.length === txnDetails?.stmtExecutionIDs?.length;

    const contentionComplete =
      contentionInfo != null ||
      (txnDetails != null &&
        txnDetails.insights.find(
          i => i.name === InsightNameEnum.highContention,
        ) == null);

    if (!stmtsComplete || !contentionComplete || txnDetails == null) {
      // Only fetch if we are missing some information.
      const execReq = timeScaleRangeToObject(timeScale);
      const req = {
        mergeResultWith: {
          txnDetails,
          blockingContentionDetails: contentionInfo,
          statements: stmts,
          start: execReq.start,
          end: execReq.end,
        },
        txnExecutionID: executionID,
        excludeTxn: txnDetails != null,
        excludeStmts: stmtsComplete,
        excludeContention: contentionComplete,
      };
      refreshTransactionInsightDetails(req);
    }
  }, [
    timeScale,
    executionID,
    refreshTransactionInsightDetails,
    stmts,
    txnDetails,
    contentionInfo,
  ]);

  const prevPage = (): void => history.goBack();

  return (
    <div>
      <Helmet title={"Details | Insight"} />
      <div>
        <Button
          onClick={prevPage}
          type="unstyled-link"
          size="small"
          icon={<ArrowLeft fontSize={"10px"} />}
          iconPosition="left"
          className={commonStyles("small-margin")}
        >
          Insights
        </Button>
        <h3
          className={commonStyles("base-heading", "no-margin-bottom")}
        >{`Transaction Execution ID: ${String(
          getMatchParamByName(match, idAttr),
        )}`}</h3>
      </div>
      <section>
        <Loading
          loading={txnDetails == null}
          page={"Transaction Insight details"}
          error={
            txnDetails == null && insightError?.length ? insightError[0] : null
          }
          renderError={() => InsightsError()}
        >
          <Tabs
            className={commonStyles("cockroach--tabs")}
            defaultActiveKey={TabKeysEnum.OVERVIEW}
          >
            <Tabs.TabPane tab="Overview" key={TabKeysEnum.OVERVIEW}>
              <TransactionInsightDetailsOverviewTab
                statements={insightDetails.statements}
                txnDetails={insightDetails.txnDetails}
                contentionDetails={insightDetails.blockingContentionDetails}
                setTimeScale={setTimeScale}
              />
            </Tabs.TabPane>
            {txnDetails?.stmtExecutionIDs?.length && (
              <Tabs.TabPane
                tab="Statement Executions"
                key={TabKeysEnum.STATEMENTS}
              >
                <TransactionInsightsDetailsStmtsTab
                  insightDetails={insightDetails}
                />
              </Tabs.TabPane>
            )}
          </Tabs>
        </Loading>
      </section>
    </div>
  );
};
