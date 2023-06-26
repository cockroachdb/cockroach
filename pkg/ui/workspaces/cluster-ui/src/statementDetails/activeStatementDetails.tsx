// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import React, { useEffect, useState } from "react";
import { ArrowLeft } from "@cockroachlabs/icons";
import { Button } from "src/button";
import Helmet from "react-helmet";
import { commonStyles } from "src/common";
import classNames from "classnames/bind";
import { useHistory, match } from "react-router-dom";
import { Col, Row, Tabs } from "antd";
import { ActiveStatementDetailsOverviewTab } from "./activeStatementDetailsOverviewTab";
import { SqlBox, SqlBoxSize } from "src/sql/box";
import { getExplainPlanFromGist } from "../api/decodePlanGistApi";
import { getMatchParamByName } from "src/util/query";
import { executionIdAttr } from "../util";
import {
  ActiveStatement,
  ExecutionContentionDetails,
} from "src/activeExecutions";

import "antd/lib/tabs/style";
import "antd/lib/col/style";
import "antd/lib/row/style";
import styles from "./statementDetails.module.scss";
import LoadingError from "../sqlActivity/errorComponent";
import { Loading } from "../loading";
import { Insights } from "./planDetails";
import { getIdxRecommendationsFromExecution } from "../api/idxRecForStatementApi";
import { SortSetting } from "../sortedtable";
const cx = classNames.bind(styles);

export type ActiveStatementDetailsStateProps = {
  isTenant?: boolean;
  contentionDetails?: ExecutionContentionDetails;
  statement: ActiveStatement;
  match: match;
  hasAdminRole: boolean;
};

export type ActiveStatementDetailsDispatchProps = {
  refreshLiveWorkload: () => void;
};

enum TabKeysEnum {
  OVERVIEW = "overview",
  EXPLAIN = "explain",
}

type ExplainPlanState = {
  explainPlan: string;
  loaded: boolean;
  error: Error;
};

export type ActiveStatementDetailsProps = ActiveStatementDetailsStateProps &
  ActiveStatementDetailsDispatchProps;

export const ActiveStatementDetails: React.FC<ActiveStatementDetailsProps> = ({
  isTenant,
  contentionDetails,
  statement,
  match,
  refreshLiveWorkload,
  hasAdminRole,
}) => {
  const history = useHistory();
  const executionID = getMatchParamByName(match, executionIdAttr);
  const [explainPlanState, setExplainPlanState] = useState<ExplainPlanState>({
    explainPlan: null,
    loaded: false,
    error: null,
  });
  const [indexRecommendations, setIndexRecommendations] = useState<string[]>();
  const [insightsSortSetting, setInsightsSortSetting] = useState<SortSetting>({
    ascending: false,
    columnTitle: "insights",
  });

  useEffect(() => {
    if (statement == null) {
      // Refresh sessions if the statement was not found initially.
      refreshLiveWorkload();
    }
  }, [refreshLiveWorkload, statement]);

  const onTabClick = (key: TabKeysEnum) => {
    if (
      !isTenant &&
      key === TabKeysEnum.EXPLAIN &&
      statement?.planGist &&
      !explainPlanState.loaded
    ) {
      // Get the explain plan.
      getExplainPlanFromGist({ planGist: statement.planGist }).then(res => {
        setExplainPlanState({
          explainPlan: res.explainPlan,
          loaded: true,
          error: res.error,
        });
      });
      getIdxRecommendationsFromExecution({
        planGist: statement.planGist,
        query: statement.stmtNoConstants,
        appName: statement.application,
      }).then(res => {
        setIndexRecommendations(res.recommendations);
      });
    }
  };

  const returnToActiveStatements = () => {
    history.push("/sql-activity?tab=Statements&view=active");
  };

  const hasInsights = indexRecommendations?.length > 0;
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
      </section>
      <Tabs
        className={commonStyles("cockroach--tabs")}
        defaultActiveKey={TabKeysEnum.OVERVIEW}
        onTabClick={onTabClick}
      >
        <Tabs.TabPane tab="Overview" key={TabKeysEnum.OVERVIEW}>
          <ActiveStatementDetailsOverviewTab
            statement={statement}
            contentionDetails={contentionDetails}
          />
        </Tabs.TabPane>
        {!isTenant && (
          <Tabs.TabPane tab="Explain Plan" key={TabKeysEnum.EXPLAIN}>
            <Row gutter={24} className={cx("margin-right")}>
              <Col className="gutter-row" span={24}>
                <Loading
                  loading={
                    !explainPlanState.loaded && statement?.planGist?.length > 0
                  }
                  page={"stmt_insight_details"}
                  error={explainPlanState.error}
                  renderError={() =>
                    LoadingError({
                      statsType: "explain plan",
                      error: explainPlanState.error,
                    })
                  }
                >
                  <SqlBox
                    value={explainPlanState.explainPlan || "Not available."}
                    size={SqlBoxSize.custom}
                  />
                  {hasInsights && (
                    <Insights
                      idxRecommendations={indexRecommendations}
                      query={statement.stmtNoConstants}
                      database={statement.database}
                      sortSetting={insightsSortSetting}
                      onChangeSortSetting={setInsightsSortSetting}
                      hasAdminRole={hasAdminRole}
                    />
                  )}
                </Loading>
              </Col>
            </Row>
          </Tabs.TabPane>
        )}
      </Tabs>
    </div>
  );
};
