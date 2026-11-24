// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.
import { ArrowLeft } from "@cockroachlabs/icons";
import { InlineAlert } from "@cockroachlabs/ui-components";
import { Tabs } from "antd";
import React from "react";
import Helmet from "react-helmet";
import { RouteComponentProps } from "react-router-dom";

import { Anchor } from "src/anchor";
import { useTxnInsightDetails } from "src/api/txnInsightDetailsApi";
import { useUserSQLRoles } from "src/api/userApi";
import { Button } from "src/button";
import { commonStyles } from "src/common";
import { idAttr, insights } from "src/util";
import { getMatchParamByName } from "src/util/query";

import { TimeScale } from "../../timeScaleDropdown";
import { InsightNameEnum, StmtFailureCodesStr } from "../types";

import { TransactionInsightDetailsOverviewTab } from "./transactionInsightDetailsOverviewTab";
import { TransactionInsightsDetailsStmtsTab } from "./transactionInsightDetailsStmtsTab";

export interface TransactionInsightDetailsStateProps {
  timeScale?: TimeScale;
}

export interface TransactionInsightDetailsDispatchProps {
  setTimeScale: (ts: TimeScale) => void;
}

export type TransactionInsightDetailsProps =
  TransactionInsightDetailsStateProps &
    TransactionInsightDetailsDispatchProps &
    RouteComponentProps;

enum TabKeysEnum {
  OVERVIEW = "overview",
  STATEMENTS = "statements",
}

export const TransactionInsightDetails: React.FC<
  TransactionInsightDetailsProps
> = ({ setTimeScale, history, timeScale, match }) => {
  const executionID = getMatchParamByName(match, idAttr);

  const { data: txnInsightDetailsResp } = useTxnInsightDetails(
    executionID,
    timeScale,
  );
  const { data: userRoles } = useUserSQLRoles();
  const hasAdminRole = userRoles?.roles?.includes("ADMIN") ?? false;

  const insightDetails = txnInsightDetailsResp?.results.result;
  const insightError = txnInsightDetailsResp?.results.errors ?? null;
  const maxSizeApiReached = txnInsightDetailsResp?.maxSizeReached;

  // Determine if we have loaded data but some sub-queries are still
  // missing information (partial failure). Once data has arrived, any
  // missing pieces won't be retried.
  const hasData = txnInsightDetailsResp != null;
  const stmtsComplete =
    insightDetails?.statements != null &&
    insightDetails.statements.length ===
      insightDetails?.txnDetails?.stmtExecutionIDs.length;
  const contentionComplete =
    insightDetails?.blockingContentionDetails != null ||
    (insightDetails?.txnDetails != null &&
      insightDetails.txnDetails.insights.find(
        i => i.name === InsightNameEnum.HIGH_CONTENTION,
      ) == null &&
      insightDetails.txnDetails.errorCode !==
        StmtFailureCodesStr.RETRY_SERIALIZABLE);
  const maxRequestsReached =
    hasData &&
    insightDetails != null &&
    (!stmtsComplete || !contentionComplete);

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
          Previous page
        </Button>
        <h3
          className={commonStyles("base-heading", "no-margin-bottom")}
        >{`Transaction Execution ID: ${String(
          getMatchParamByName(match, idAttr),
        )}`}</h3>
      </div>
      <section>
        <Tabs
          className={commonStyles("cockroach--tabs")}
          defaultActiveKey={TabKeysEnum.OVERVIEW}
        >
          <Tabs.TabPane tab="Overview" key={TabKeysEnum.OVERVIEW}>
            <TransactionInsightDetailsOverviewTab
              maxRequestsReached={maxRequestsReached}
              errors={insightError}
              statements={insightDetails?.statements}
              txnDetails={insightDetails?.txnDetails}
              contentionDetails={insightDetails?.blockingContentionDetails}
              setTimeScale={setTimeScale}
              hasAdminRole={hasAdminRole}
              maxApiSizeReached={maxSizeApiReached}
            />
          </Tabs.TabPane>
          {(insightDetails?.txnDetails?.stmtExecutionIDs.length ||
            insightDetails?.statements?.length) && (
            <Tabs.TabPane
              tab="Statement Executions"
              key={TabKeysEnum.STATEMENTS}
            >
              <TransactionInsightsDetailsStmtsTab
                isLoading={insightDetails?.statements == null && !hasData}
                error={insightError?.statementsErr}
                statements={insightDetails?.statements}
              />
              {maxSizeApiReached && (
                <InlineAlert
                  intent="info"
                  title={
                    <>
                      Not all statements are displayed because the maximum
                      number of statements was reached in the console.&nbsp;
                      <Anchor href={insights} target="_blank">
                        Learn more
                      </Anchor>
                    </>
                  }
                />
              )}
            </Tabs.TabPane>
          )}
        </Tabs>
      </section>
    </div>
  );
};
