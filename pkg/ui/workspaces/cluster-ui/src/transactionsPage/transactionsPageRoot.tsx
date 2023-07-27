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
import { TransactionViewType } from "./transactionsPageTypes";
import { Option } from "src/selectWithDescription/selectWithDescription";
import { SQLActivityRootControls } from "src/sqlActivityRootControls/sqlActivityRootControls";
import { TransactionsPageProps } from "./transactionsPage";
import { TransactionsPage } from ".";
import { statementsSql } from "../util/docs";
import {
  RecentTransactionsView,
  RecentTransactionsViewProps,
} from "./recentTransactionsView";
import { Anchor } from "src/anchor";

export type TransactionsPageRootProps = {
  fingerprintsPageProps: TransactionsPageProps;
  activePageProps: RecentTransactionsViewProps;
};

export const TransactionsPageRoot = ({
  fingerprintsPageProps,
  activePageProps,
}: TransactionsPageRootProps): React.ReactElement => {
  const transactionOptions: Option[] = [
    {
      value: TransactionViewType.FINGERPRINTS,
      label: "Transaction Fingerprints",
      description: (
        <span>
          A transaction fingerprint represents statement fingerprints grouped by
          transaction. Statement fingerprints represent one or more SQL
          statements by replacing literal values (e.g., numbers and strings)
          with underscores (_). This can help you quickly identify frequently
          executed SQL transactions and their latencies.{" "}
          <Anchor href={statementsSql}>Learn more</Anchor>
        </span>
      ),
      component: <TransactionsPage {...fingerprintsPageProps} />,
    },
    {
      value: TransactionViewType.ACTIVE,
      label: "Active Executions",
      description: (
        <span>
          Active executions represent individual transactions executions in
          progress. Use active transaction execution details, such as the
          application or elapsed time, to understand and tune workload
          performance.
          {/* TODO (xinhaoz) #78379 add 'Learn More' link to documentation page*/}
        </span>
      ),
      component: <RecentTransactionsView {...activePageProps} />,
    },
  ];

  return <SQLActivityRootControls options={transactionOptions} />;
};
