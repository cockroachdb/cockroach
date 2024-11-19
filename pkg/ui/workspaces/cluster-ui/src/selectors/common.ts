// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { RouteComponentProps } from "react-router";

import {
  getMatchParamByName,
  executionIdAttr,
  idAttr,
  statementAttr,
  txnFingerprintIdAttr,
} from "src/util";

import { StmtInsightEvent } from "../insights";
import { AppState } from "../store";

// The functions in this file are agnostic to the different shape of each
// state in db-console and cluster-ui. This file contains selector functions
// and combiners that can be reused across both packages.
// This is to avoid unnecessary repeated logic in the creation of selectors
// between db-console and cluster-ui.

export const selectExecutionID = (
  _state: unknown,
  props: RouteComponentProps,
): string | null => {
  return getMatchParamByName(props.match, executionIdAttr);
};

export const selectID = (
  _state: unknown,
  props: RouteComponentProps,
): string | null => {
  return getMatchParamByName(props.match, idAttr);
};

export const selectStatementFingerprintID = (
  _state: unknown,
  props: RouteComponentProps,
): string | null => getMatchParamByName(props.match, statementAttr);

export const selectTransactionFingerprintID = (
  _state: unknown,
  props: RouteComponentProps,
): string | null => getMatchParamByName(props.match, txnFingerprintIdAttr);

export const selectStmtInsights = (state: AppState): StmtInsightEvent[] =>
  state.adminUI?.stmtInsights?.data?.results;
