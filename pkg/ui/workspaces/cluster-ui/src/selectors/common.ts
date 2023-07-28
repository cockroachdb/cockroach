// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { RouteComponentProps } from "react-router";
import {
  getMatchParamByName,
  executionIdAttr,
  idAttr,
  statementAttr,
  txnFingerprintIdAttr,
} from "src/util";

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
