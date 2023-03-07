// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { StmtInsightEvent } from "../../types";
import React from "react";
import { HexStringToInt64String } from "../../../util";
import { Link } from "react-router-dom";
import { StatementLinkTarget } from "../../../statementsTable";

export function TransactionDetailsLink(
  transactionFingerprintID: string,
): React.ReactElement {
  const txnID = HexStringToInt64String(transactionFingerprintID);
  const path = `/transaction/${txnID}`;
  return (
    <Link to={path}>
      <div>{String(transactionFingerprintID)}</div>
    </Link>
  );
}

export function StatementDetailsLink(
  insightDetails: StmtInsightEvent,
): React.ReactElement {
  const linkProps = {
    statementFingerprintID: HexStringToInt64String(
      insightDetails.statementFingerprintID,
    ),
    appNames: [insightDetails.application],
    implicitTxn: insightDetails.implicitTxn,
  };

  return (
    <Link to={StatementLinkTarget(linkProps)}>
      <div>{String(insightDetails.statementFingerprintID)}</div>
    </Link>
  );
}
