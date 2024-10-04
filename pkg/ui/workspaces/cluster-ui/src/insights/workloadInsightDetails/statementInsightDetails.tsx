// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.
import React, { useEffect, useState } from "react";
import Helmet from "react-helmet";
import { RouteComponentProps } from "react-router-dom";
import { ArrowLeft } from "@cockroachlabs/icons";
import { Row, Col, Tabs } from "antd";
import "antd/lib/tabs/style";
import "antd/lib/col/style";
import "antd/lib/row/style";
import { Button } from "src/button";
import { Loading } from "src/loading";
import { SqlBox, SqlBoxSize } from "src/sql";
import { getMatchParamByName, idAttr } from "src/util";
import { StmtInsightEvent } from "../types";
import { getExplainPlanFromGist } from "src/api/decodePlanGistApi";
import { StatementInsightDetailsOverviewTab } from "./statementInsightDetailsOverviewTab";
import { TimeScale, toDateRange } from "../../timeScaleDropdown";
import { getStmtInsightsApi } from "src/api";
import { InsightsError } from "../insightsErrorComponent";

// Styles
import classNames from "classnames/bind";
import { commonStyles } from "src/common";
import insightsDetailsStyles from "src/insights/workloadInsightDetails/insightsDetails.module.scss";

const cx = classNames.bind(insightsDetailsStyles);

enum TabKeysEnum {
  OVERVIEW = "overview",
  EXPLAIN = "explain",
}
export interface StatementInsightDetailsStateProps {
  insightEventDetails: StmtInsightEvent;
  insightError: Error | null;
  timeScale?: TimeScale;
  hasAdminRole: boolean;
}

export interface StatementInsightDetailsDispatchProps {
  setTimeScale: (ts: TimeScale) => void;
  refreshUserSQLRoles: () => void;
}

export type StatementInsightDetailsProps = StatementInsightDetailsStateProps &
  StatementInsightDetailsDispatchProps &
  RouteComponentProps<unknown>;

type ExplainPlanState = {
  explainPlan: string;
  loaded: boolean;
  error?: Error;
};

type StmtInsightsState = {
  details: StmtInsightEvent;
  loaded: boolean;
  error?: Error;
};

export const StatementInsightDetails: React.FC<
  StatementInsightDetailsProps
> = ({
  history,
  insightEventDetails,
  insightError,
  match,
  timeScale,
  hasAdminRole,
  refreshUserSQLRoles,
}) => {
  const [explainPlanState, setExplainPlanState] = useState<ExplainPlanState>({
    explainPlan: null,
    loaded: false,
    error: null,
  });
  const [insightDetails, setInsightDetails] =
    useState<StmtInsightsState | null>({
      details: insightEventDetails,
      loaded: insightEventDetails != null,
      error: insightError,
    });

  const details = insightDetails.details;

  const prevPage = (): void => history.goBack();

  const onTabClick = (key: TabKeysEnum) => {
    if (
      key === TabKeysEnum.EXPLAIN &&
      details?.planGist &&
      !explainPlanState.loaded
    ) {
      // Get the explain plan.
      getExplainPlanFromGist({ planGist: details.planGist }).then(res => {
        setExplainPlanState({
          explainPlan: res.explainPlan,
          loaded: true,
          error: res.error,
        });
      });
    }
  };

  const executionID = getMatchParamByName(match, idAttr);

  useEffect(() => {
    refreshUserSQLRoles();
    if (details != null) {
      return;
    }
    const [start, end] = toDateRange(timeScale);
    getStmtInsightsApi({ stmtExecutionID: executionID, start, end })
      .then(res => {
        setInsightDetails({
          details: res?.results?.length ? res.results[0] : null,
          loaded: true,
        });
      })
      .catch(e => {
        setInsightDetails({ details: null, error: e, loaded: true });
      });
  }, [details, executionID, timeScale, refreshUserSQLRoles]);

  return (
    <div>
      <Helmet title={"Details | Insight"} />
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
      <h3 className={commonStyles("base-heading", "no-margin-bottom")}>
        Statement Execution ID: {executionID}
      </h3>
      <div>
        <Loading
          loading={!insightDetails.loaded}
          page="Statement Insight details"
          error={insightDetails.error}
          renderError={() => InsightsError(insightDetails.error?.message)}
        >
          <section className={cx("section")}>
            <Row>
              <Col span={24}>
                <SqlBox
                  size={SqlBoxSize.custom}
                  value={details?.query}
                  format={true}
                />
              </Col>
            </Row>
          </section>
          <Tabs
            className={commonStyles("cockroach--tabs")}
            defaultActiveKey={TabKeysEnum.OVERVIEW}
            onTabClick={onTabClick}
          >
            <Tabs.TabPane tab="Overview" key={TabKeysEnum.OVERVIEW}>
              <StatementInsightDetailsOverviewTab
                insightEventDetails={details}
                hasAdminRole={hasAdminRole}
              />
            </Tabs.TabPane>
            <Tabs.TabPane tab="Explain Plan" key={TabKeysEnum.EXPLAIN}>
              <section className={cx("section")}>
                <Row gutter={24}>
                  <Col span={24}>
                    <Loading
                      loading={
                        !explainPlanState.loaded &&
                        details?.planGist?.length > 0
                      }
                      page={"stmt_insight_details"}
                      error={explainPlanState.error}
                      renderError={() =>
                        InsightsError(explainPlanState.error?.message)
                      }
                    >
                      <SqlBox
                        value={explainPlanState.explainPlan || "Not available."}
                        size={SqlBoxSize.custom}
                      />
                    </Loading>
                  </Col>
                </Row>
              </section>
            </Tabs.TabPane>
          </Tabs>
        </Loading>
      </div>
    </div>
  );
};
