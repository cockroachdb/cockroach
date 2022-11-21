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
import { TxnContentionInsightDetailsRequest } from "src/api";
import { TxnInsightDetails } from "../types";

import { commonStyles } from "src/common";
import { InsightsError } from "../insightsErrorComponent";
import { TimeScale } from "../../timeScaleDropdown";
import { idAttr } from "src/util";
import { TransactionInsightDetailsOverviewTab } from "./transactionInsightDetailsOverviewTab";
import { TransactionInsightsDetailsStmtsTab } from "./transactionInsightDetailsStmtsTab";

import "antd/lib/tabs/style";
import { executionInsightsRequestFromTimeScale } from "../utils";
export interface TransactionInsightDetailsStateProps {
  insightDetails: TxnInsightDetails;
  insightError: Error | null;
  timeScale?: TimeScale;
}

export interface TransactionInsightDetailsDispatchProps {
  refreshTransactionInsightDetails: (
    req: TxnContentionInsightDetailsRequest,
  ) => void;
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
  const noInsights = !insightDetails;
  useEffect(() => {
    const execReq = executionInsightsRequestFromTimeScale(timeScale);
    if (noInsights) {
      // Only refresh if we have no data (e.g. refresh the page)
      refreshTransactionInsightDetails({
        id: executionID,
        start: execReq.start,
        end: execReq.end,
      });
    }
  }, [executionID, refreshTransactionInsightDetails, noInsights, timeScale]);

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
          loading={!insightDetails || insightDetails === null}
          page={"Transaction Insight details"}
          error={insightError}
          renderError={() => InsightsError()}
        >
          <Tabs
            className={commonStyles("cockroach--tabs")}
            defaultActiveKey={TabKeysEnum.OVERVIEW}
          >
            <Tabs.TabPane tab="Overview" key={TabKeysEnum.OVERVIEW}>
              <TransactionInsightDetailsOverviewTab
                insightDetails={insightDetails}
                setTimeScale={setTimeScale}
              />
            </Tabs.TabPane>
            {insightDetails?.statementInsights?.length && (
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
