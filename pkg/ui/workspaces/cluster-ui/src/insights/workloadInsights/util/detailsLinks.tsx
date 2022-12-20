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
import moment from "moment/moment";
import { TimeScale } from "../../../timeScaleDropdown";
import { Moment } from "moment";

export function TransactionDetailsLink(
  transactionFingerprintID: string,
  startTime: Moment | null,
  setTimeScale: (tw: TimeScale) => void,
): React.ReactElement {
  const txnID = HexStringToInt64String(transactionFingerprintID);
  const path = `/transaction/${txnID}`;
  const timeScale: TimeScale = startTime
    ? {
        windowSize: moment.duration(65, "minutes"),
        fixedWindowEnd: moment(startTime).add(1, "hour"),
        sampleSize: moment.duration(1, "hour"),
        key: "Custom",
      }
    : null;
  return (
    <Link to={path} onClick={() => timeScale && setTimeScale(timeScale)}>
      <div>{String(transactionFingerprintID)}</div>
    </Link>
  );
}

export function StatementDetailsLink(
  insightDetails: StmtInsightEvent,
  setTimeScale: (tw: TimeScale) => void,
): React.ReactElement {
  const linkProps = {
    statementFingerprintID: HexStringToInt64String(
      insightDetails.statementFingerprintID,
    ),
    appNames: [insightDetails.application],
    implicitTxn: insightDetails.implicitTxn,
  };
  const timeScale: TimeScale = {
    windowSize: moment.duration(insightDetails.elapsedTimeMillis),
    fixedWindowEnd: insightDetails.endTime,
    sampleSize: moment.duration(1, "hour"),
    key: "Custom",
  };

  return (
    <Link
      to={StatementLinkTarget(linkProps)}
      onClick={() => setTimeScale(timeScale)}
    >
      <div>{String(insightDetails.statementFingerprintID)}</div>
    </Link>
  );
}
