// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { StatementInsightEvent } from "../../types";
import React from "react";
import { HexStringToInt64String } from "../../../util";
import { Link } from "react-router-dom";
import { StatementLinkTarget } from "../../../statementsTable";
import moment from "moment/moment";
import { TimeScale } from "../../../timeScaleDropdown";
import { Moment } from "moment";

export function TransactionDetailsLink(
  transactionFingerprintID: string,
  startTime: Moment,
  setTimeScale: (tw: TimeScale) => void,
): React.ReactElement {
  const txnID = HexStringToInt64String(transactionFingerprintID);
  const path = `/transaction/${txnID}`;
  const timeScale: TimeScale = {
    windowSize: moment.duration(65, "minutes"),
    fixedWindowEnd: startTime.add(1, "hour"),
    sampleSize: moment.duration(1, "hour"),
    key: "Custom",
  };
  return (
    <Link to={path} onClick={() => setTimeScale(timeScale)}>
      <div>{String(transactionFingerprintID)}</div>
    </Link>
  );
}

export function StatementDetailsLink(
  insightDetails: StatementInsightEvent,
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
