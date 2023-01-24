// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import React, { useContext, useState } from "react";
import { Helmet } from "react-helmet";
import { ArrowLeft } from "@cockroachlabs/icons";
import {
  PlansSortedTable,
  makeExplainPlanColumns,
  PlanHashStats,
} from "./plansTable";
import { Button } from "../../button";
import { SqlBox, SqlBoxSize } from "../../sql";
import { SortSetting } from "../../sortedtable";
import { Row } from "antd";
import "antd/lib/row/style";
import {
  InsightsSortedTable,
  makeInsightsColumns,
} from "../../insightsTable/insightsTable";
import classNames from "classnames/bind";
import styles from "../statementDetails.module.scss";
import { CockroachCloudContext } from "../../contexts";
import { InsightRecommendation, InsightType } from "../../insights";

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
      <SqlBox value={explainPlan} size={SqlBoxSize.custom} />
      {hasInsights && (
        <Insights
          idxRecommendations={plan.stats.index_recommendations}
          plan={plan}
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
  plan: PlanHashStats,
  statementFingerprintID: string,
): InsightRecommendation[] {
  const recs = [];
  for (let i = 0; i < idxRecs.length; i++) {
    const rec = idxRecs[i];
    let idxType: InsightType;
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
      database: plan.metadata.databases[0],
      query: rec.split(" : ")[1],
      execution: {
        statement: plan.metadata.query,
        summary:
          plan.metadata.query.length > 120
            ? plan.metadata.query.slice(0, 120) + "..."
            : plan.metadata.query,
        fingerprintID: statementFingerprintID,
        implicit: plan.metadata.implicit_txn,
      },
    };
    recs.push(idxRec);
  }

  return recs;
}

interface InsightsProps {
  idxRecommendations: string[];
  plan: PlanHashStats;
  statementFingerprintID: string;
  sortSetting: SortSetting;
  onChangeSortSetting: (ss: SortSetting) => void;
  hasAdminRole: boolean;
}

function Insights({
  idxRecommendations,
  plan,
  statementFingerprintID,
  sortSetting,
  onChangeSortSetting,
  hasAdminRole,
}: InsightsProps): React.ReactElement {
  const isCockroachCloud = useContext(CockroachCloudContext);
  const insightsColumns = makeInsightsColumns(
    isCockroachCloud,
    hasAdminRole,
    true,
  );
  const data = formatIdxRecommendations(
    idxRecommendations,
    plan,
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
