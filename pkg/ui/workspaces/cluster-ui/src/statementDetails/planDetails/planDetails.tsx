// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import React, { useState } from "react";
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

interface PlanDetailsProps {
  plans: PlanHashStats[];
  sortSetting: SortSetting;
  onChangeSortSetting: (ss: SortSetting) => void;
}

export function PlanDetails({
  plans,
  sortSetting,
  onChangeSortSetting,
}: PlanDetailsProps): React.ReactElement {
  const [plan, setPlan] = useState<PlanHashStats | null>(null);
  const handleDetails = (plan: PlanHashStats): void => {
    setPlan(plan);
  };
  const backToPlanTable = (): void => {
    setPlan(null);
  };

  if (plan) {
    return renderExplainPlan(plan, backToPlanTable);
  } else {
    return renderPlanTable(
      plans,
      handleDetails,
      sortSetting,
      onChangeSortSetting,
    );
  }
}

function renderPlanTable(
  plans: PlanHashStats[],
  handleDetails: (plan: PlanHashStats) => void,
  sortSetting: SortSetting,
  onChangeSortSetting: (ss: SortSetting) => void,
): React.ReactElement {
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

function renderExplainPlan(
  plan: PlanHashStats,
  backToPlanTable: () => void,
): React.ReactElement {
  const explainPlan =
    plan.explain_plan === "" ? "unavailable" : plan.explain_plan;
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
      <SqlBox value={explainPlan} size={SqlBoxSize.large} />
    </div>
  );
}
