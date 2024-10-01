// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import React from "react";
import { Link } from "react-router-dom";

import { StatementLinkTarget } from "../../../statementsTable";
import { TransactionLinkTarget } from "../../../transactionsTable";
import { HexStringToInt64String } from "../../../util";
import { StmtInsightEvent } from "../../types";

export function TransactionDetailsLink(
  transactionFingerprintID: string,
  application?: string,
): React.ReactElement {
  const txnID = HexStringToInt64String(transactionFingerprintID);
  return (
    <Link
      to={TransactionLinkTarget({
        transactionFingerprintId: txnID,
        application,
      })}
    >
      <div>{String(transactionFingerprintID)}</div>
    </Link>
  );
}

export function StatementDetailsLink(
  insightDetails: StmtInsightEvent,
): React.ReactElement {
  const linkProps = {
    statementFingerprintID: HexStringToInt64String(
      insightDetails?.statementFingerprintID,
    ),
    appNames: [insightDetails?.application],
    implicitTxn: insightDetails?.implicitTxn,
  };

  return (
    <Link to={StatementLinkTarget(linkProps)}>
      <div>{String(insightDetails?.statementFingerprintID)}</div>
    </Link>
  );
}
