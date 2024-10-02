// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { ArrowLeft } from "@cockroachlabs/icons";
import { Col, Row } from "antd";
import classNames from "classnames/bind";
import React, { useContext, useState } from "react";
import { Helmet } from "react-helmet";

import { Button } from "../../button";
import { CockroachCloudContext } from "../../contexts";
import { InsightRecommendation, InsightType } from "../../insights";
import {
  InsightsSortedTable,
  makeInsightsColumns,
} from "../../insightsTable/insightsTable";
import { SortSetting } from "../../sortedtable";
import { SqlBox, SqlBoxSize } from "../../sql";
import { SummaryCard, SummaryCardItem } from "../../summaryCard";
import { Timestamp } from "../../timestamp";
import {
  Count,
  DATE_FORMAT_24_TZ,
  Duration,
  formatNumberForDisplay,
  longToInt,
  RenderCount,
  TimestampToMoment,
} from "../../util";
import styles from "../statementDetails.module.scss";

import {
  formatIndexes,
  PlansSortedTable,
  makeExplainPlanColumns,
  PlanHashStats,
} from "./plansTable";

const cx = classNames.bind(styles);

interface PlanDetailsProps {
  plans: PlanHashStats[];
  statementFingerprintID: string;
  hasAdminRole: boolean;
}

export function PlanDetails({
  plans,
  statementFingerprintID,
  hasAdminRole,
}: PlanDetailsProps): React.ReactElement {
  const [plan, setPlan] = useState<PlanHashStats | null>(null);
  const [plansSortSetting, setPlansSortSetting] = useState<SortSetting>({
    ascending: false,
    columnTitle: "lastExecTime",
  });
  const [insightsSortSetting, setInsightsSortSetting] = useState<SortSetting>({
    ascending: false,
    columnTitle: "insights",
  });
  const handleDetails = (plan: PlanHashStats): void => {
    setPlan(plan);
  };
  const backToPlanTable = (): void => {
    setPlan(null);
  };

  if (plan) {
    return (
      <ExplainPlan
        plan={plan}
        statementFingerprintID={statementFingerprintID}
        backToPlanTable={backToPlanTable}
        sortSetting={insightsSortSetting}
        onChangeSortSetting={setInsightsSortSetting}
        hasAdminRole={hasAdminRole}
      />
    );
  } else {
    return (
      <div className={cx("table-area")}>
        <PlanTable
          plans={plans}
          handleDetails={handleDetails}
          sortSetting={plansSortSetting}
          onChangeSortSetting={setPlansSortSetting}
        />
      </div>
    );
  }
}

interface PlanTableProps {
  plans: PlanHashStats[];
  handleDetails: (plan: PlanHashStats) => void;
  sortSetting: SortSetting;
  onChangeSortSetting: (ss: SortSetting) => void;
}

function PlanTable({
  plans,
  handleDetails,
  sortSetting,
  onChangeSortSetting,
}: PlanTableProps): React.ReactElement {
  const columns = makeExplainPlanColumns(handleDetails);
  return (
    <PlansSortedTable
      columns={columns}
      data={plans}
      className="statements-table"
      sortSetting={sortSetting}
      onChangeSortSetting={onChangeSortSetting}
    />
  );
}

interface ExplainPlanProps {
  plan: PlanHashStats;
  statementFingerprintID: string;
  backToPlanTable: () => void;
  sortSetting: SortSetting;
  onChangeSortSetting: (ss: SortSetting) => void;
  hasAdminRole: boolean;
}

function ExplainPlan({
  plan,
  statementFingerprintID,
  backToPlanTable,
  sortSetting,
  onChangeSortSetting,
  hasAdminRole,
}: ExplainPlanProps): React.ReactElement {
  const explainPlan =
    `Plan Gist: ${plan.stats.plan_gists[0]} \n\n` +
    (plan.explain_plan === "" ? "unavailable" : plan.explain_plan);
  const hasInsights = plan.stats.index_recommendations?.length > 0;
  const duration = (v: number) => Duration(v * 1e9);
  const count = (v: number) => v.toFixed(1);
  return (
    <div>
      <Helmet title="Plan Details" />
      <Button
        onClick={backToPlanTable}
        type="unstyled-link"
        size="small"
        icon={<ArrowLeft fontSize={"10px"} />}
        iconPosition="left"
        className="small-margin"
      >
        All Plans
      </Button>
      <SqlBox value={explainPlan} size={SqlBoxSize.CUSTOM} />
      <Row gutter={24} className={cx("margin-left-neg", "margin-bottom")}>
        <Col className="gutter-row" span={12}>
          <SummaryCard className={cx("summary-card")}>
            <SummaryCardItem
              label="Last Execution Time"
              value={
                <Timestamp
                  time={TimestampToMoment(plan.stats.last_exec_timestamp)}
                  format={DATE_FORMAT_24_TZ}
                />
              }
            />
            <SummaryCardItem
              label="Average Execution Time"
              value={formatNumberForDisplay(plan.stats.run_lat?.mean, duration)}
            />
            <SummaryCardItem
              label="Execution Count"
              value={Count(longToInt(plan.stats.count))}
            />
            <SummaryCardItem
              label="Average Rows Read"
              value={formatNumberForDisplay(plan.stats.rows_read?.mean, count)}
            />
          </SummaryCard>
        </Col>
        <Col className="gutter-row" span={12}>
          <SummaryCard className={cx("summary-card")}>
            <SummaryCardItem
              label="Full Scan"
              value={RenderCount(
                plan.metadata.full_scan_count,
                plan.metadata.total_count,
              )}
            />
            <SummaryCardItem
              label="Distributed"
              value={RenderCount(
                plan.metadata.dist_sql_count,
                plan.metadata.total_count,
              )}
            />
            <SummaryCardItem
              label="Vectorized"
              value={RenderCount(
                plan.metadata.vec_count,
                plan.metadata.total_count,
              )}
            />
            <SummaryCardItem
              label="Used Indexes"
              value={formatIndexes(
                plan.stats.indexes,
                plan.metadata.databases[0],
              )}
            />
          </SummaryCard>
        </Col>
      </Row>
      {hasInsights && (
        <Insights
          idxRecommendations={plan.stats.index_recommendations}
          database={plan.metadata.databases[0]}
          query={plan.metadata.query}
          implicitTxn={plan.metadata.implicit_txn}
          statementFingerprintID={statementFingerprintID}
          sortSetting={sortSetting}
          onChangeSortSetting={onChangeSortSetting}
          hasAdminRole={hasAdminRole}
        />
      )}
    </div>
  );
}

function formatIdxRecommendations(
  idxRecs: string[],
  database: string,
  query: string,
  implicitTxn?: boolean,
  statementFingerprintID?: string,
): InsightRecommendation[] {
  const recs = [];
  for (let i = 0; i < idxRecs.length; i++) {
    const rec = idxRecs[i];
    let idxType: InsightType;
    if (!rec?.includes(" : ")) {
      continue;
    }
    const t = rec.split(" : ")[0];
    switch (t) {
      case "creation":
        idxType = "CreateIndex";
        break;
      case "replacement":
        idxType = "ReplaceIndex";
        break;
      case "drop":
        idxType = "DropIndex";
        break;
      case "alteration":
        idxType = "AlterIndex";
        break;
    }
    const idxRec: InsightRecommendation = {
      type: idxType,
      database: database,
      query: rec.split(" : ")[1],
      execution: {
        statement: query,
        summary: query.length > 120 ? query.slice(0, 120) + "..." : query,
        fingerprintID: statementFingerprintID,
        implicit: implicitTxn,
      },
    };
    recs.push(idxRec);
  }

  return recs;
}

interface InsightsProps {
  idxRecommendations: string[];
  database: string;
  query: string;
  implicitTxn?: boolean;
  statementFingerprintID?: string;
  sortSetting?: SortSetting;
  onChangeSortSetting?: (ss: SortSetting) => void;
  hasAdminRole: boolean;
}

export function Insights({
  idxRecommendations,
  database,
  query,
  implicitTxn,
  statementFingerprintID,
  sortSetting,
  onChangeSortSetting,
  hasAdminRole,
}: InsightsProps): React.ReactElement {
  const hideAction =
    useContext(CockroachCloudContext) || database?.length === 0;
  const insightsColumns = makeInsightsColumns(hideAction, hasAdminRole, true);
  const data = formatIdxRecommendations(
    idxRecommendations,
    database,
    query,
    implicitTxn,
    statementFingerprintID,
  );
  return (
    <Row gutter={24} className={cx("margin-bottom")}>
      <InsightsSortedTable
        columns={insightsColumns}
        data={data}
        sortSetting={sortSetting}
        onChangeSortSetting={onChangeSortSetting}
      />
    </Row>
  );
}
