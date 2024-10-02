// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { StatementLinkTarget } from "@cockroachlabs/cluster-ui";
import React from "react";
import { Redirect, match as Match } from "react-router-dom";

import {
  appAttr,
  databaseAttr,
  implicitTxnAttr,
  statementAttr,
} from "src/util/constants";
import { getMatchParamByName } from "src/util/query";

type Props = {
  match: Match;
};

// RedirectToStatementDetails is designed to route old versions of StatementDetails routes
// where app and database are route params, to the new StatementDetails route.
export function RedirectToStatementDetails({ match }: Props) {
  const linkProps = {
    statementFingerprintID: getMatchParamByName(match, statementAttr),
    app: getMatchParamByName(match, appAttr),
    implicitTxn: getMatchParamByName(match, implicitTxnAttr) === "true",
    database: getMatchParamByName(match, databaseAttr),
  };

  return <Redirect to={StatementLinkTarget(linkProps)} />;
}
