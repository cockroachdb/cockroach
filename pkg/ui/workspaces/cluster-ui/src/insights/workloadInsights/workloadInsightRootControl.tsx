// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import React, { useState } from "react";
import { viewAttr } from "src/util";
import { useHistory, useLocation } from "react-router-dom";
import { queryByName } from "src/util/query";
import { InsightExecEnum, InsightExecOptions } from "../types";
import { DropDownSelect } from "./util";
import {
  TransactionInsightsView,
  TransactionInsightsViewProps,
} from "./transactionInsights";
import {
  StatementInsightsView,
  StatementInsightsViewProps,
} from "./statementInsights";

export type WorkloadInsightsViewProps = {
  transactionInsightsViewProps: TransactionInsightsViewProps;
  statementInsightsViewProps: StatementInsightsViewProps;
};

// WorkloadInsightsRootControl is used by the Workload insight overview page
// to determine to show the transaction or statement overview component.
export const WorkloadInsightsRootControl = ({
  transactionInsightsViewProps,
  statementInsightsViewProps,
}: WorkloadInsightsViewProps): React.ReactElement => {
  const location = useLocation();
  const history = useHistory();
  let viewValue = queryByName(location, viewAttr) || InsightExecEnum.STATEMENT;
  // Use the default Statement page if an
  // unrecognized string was passed in from the URL
  if (!InsightExecOptions.has(viewValue)) {
    viewValue = InsightExecEnum.STATEMENT;
  }

  const [selectedInsightView, setSelectedInsightView] = useState(viewValue);

  const onViewChange = (view: string): void => {
    setSelectedInsightView(view);
    const searchParams = new URLSearchParams({
      [viewAttr]: view,
    });

    searchParams.set(viewAttr, view);
    history.push({
      search: searchParams.toString(),
    });
  };

  const dropDown = (
    <DropDownSelect
      selectedLabel={InsightExecOptions.get(selectedInsightView)}
      onViewChange={onViewChange}
      options={InsightExecOptions}
    />
  );

  if (selectedInsightView === InsightExecEnum.TRANSACTION) {
    return (
      <div>
        <TransactionInsightsView
          {...transactionInsightsViewProps}
          dropDownSelect={dropDown}
        />
      </div>
    );
  } else {
    return (
      <div>
        <StatementInsightsView
          {...statementInsightsViewProps}
          dropDownSelect={dropDown}
        />
      </div>
    );
  }
};
