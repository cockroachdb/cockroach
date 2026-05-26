// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.
import { ArrowLeft } from "@cockroachlabs/icons";
import { Row, Col, Tabs } from "antd";
import classNames from "classnames/bind";
import React, { useState } from "react";
import Helmet from "react-helmet";
import { RouteComponentProps } from "react-router-dom";

import { getExplainPlanFromGist } from "src/api/decodePlanGistApi";
import { useStmtInsightDetails } from "src/api/stmtInsightsApi";
import { useUserSQLRoles } from "src/api/userApi";
import { Button } from "src/button";
import { commonStyles } from "src/common";
import insightsDetailsStyles from "src/insights/workloadInsightDetails/insightsDetails.module.scss";
import { Loading } from "src/loading";
import { SqlBox, SqlBoxSize } from "src/sql";
import { getMatchParamByName, idAttr } from "src/util";
// Styles

import { TimeScale } from "../../timeScaleDropdown";
import { InsightsError } from "../insightsErrorComponent";

import { StatementInsightDetailsOverviewTab } from "./statementInsightDetailsOverviewTab";

const cx = classNames.bind(insightsDetailsStyles);

enum TabKeysEnum {
  OVERVIEW = "overview",
  EXPLAIN = "explain",
}
export interface StatementInsightDetailsStateProps {
  timeScale?: TimeScale;
}

export interface StatementInsightDetailsDispatchProps {
  setTimeScale: (ts: TimeScale) => void;
}

export type StatementInsightDetailsProps = StatementInsightDetailsStateProps &
  StatementInsightDetailsDispatchProps &
  RouteComponentProps;

type ExplainPlanState = {
  explainPlan: string;
  loaded: boolean;
  error?: Error;
};

export const StatementInsightDetails: React.FC<
  StatementInsightDetailsProps
> = ({ history, match, timeScale }) => {
  const [explainPlanState, setExplainPlanState] = useState<ExplainPlanState>({
    explainPlan: null,
    loaded: false,
    error: null,
  });

  const executionID = getMatchParamByName(match, idAttr);
  const { data: stmtInsightsResp, error: stmtInsightsErr } =
    useStmtInsightDetails(executionID, timeScale);
  const { data: userRoles } = useUserSQLRoles();
  const hasAdminRole = userRoles?.roles?.includes("ADMIN") ?? false;

  const details = stmtInsightsResp?.results.length
    ? stmtInsightsResp.results[0]
    : null;

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

  const isLoading = !stmtInsightsResp && !stmtInsightsErr;

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
          loading={isLoading}
          page="Statement Insight details"
          error={stmtInsightsErr}
          renderError={() => InsightsError(stmtInsightsErr?.message)}
        >
          <section className={cx("section")}>
            <Row>
              <Col span={24}>
                <SqlBox
                  size={SqlBoxSize.CUSTOM}
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
                        size={SqlBoxSize.CUSTOM}
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
