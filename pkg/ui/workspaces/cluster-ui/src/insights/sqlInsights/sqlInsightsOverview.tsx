// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import React from "react";
import { Option } from "src/selectWithDescription/selectWithDescription";
import { SQLActivityRootControls } from "src/sqlActivityRootControls/sqlActivityRootControls";
import { TransactionInsightsView, TransactionInsightsViewProps } from ".";
import { transactionContention } from "src/util";
import { Anchor } from "src/anchor";
import { InsightExecEnum } from "../types";

export type SqlInsightsOverviewProps = {
  transactionInsightsProps: TransactionInsightsViewProps;
};

export const SqlInsightsOverview = ({
  transactionInsightsProps,
}: SqlInsightsOverviewProps): React.ReactElement => {
  const sqlInsightsOptions: Option[] = [
    {
      value: InsightExecEnum.TRANSACTION,
      label: "Transactions",
      description: (
        <span>
          Transaction Insights provide a more detailed look into slow
          transaction execution.{" "}
          <Anchor href={transactionContention}>
            Learn more about transaction contention
          </Anchor>
        </span>
      ),
      component: <TransactionInsightsView {...transactionInsightsProps} />,
    },
  ];

  return <SQLActivityRootControls options={sqlInsightsOptions} />;
};
