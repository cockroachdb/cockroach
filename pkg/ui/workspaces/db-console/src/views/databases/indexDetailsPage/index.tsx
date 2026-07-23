// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { IndexDetailsPage as IndexDetailsPageComponent } from "@cockroachlabs/cluster-ui";
import React from "react";
import { useDispatch, useSelector } from "react-redux";
import { useParams } from "react-router-dom";

import { setGlobalTimeScaleAction } from "src/redux/statements";
import { selectTimeScale } from "src/redux/timeScale";
import {
  databaseNameAttr,
  indexNameAttr,
  tableNameAttr,
} from "src/util/constants";

export const IndexDetailsPage: React.FC = () => {
  const params = useParams<Record<string, string>>();
  const databaseName = params[databaseNameAttr] ?? "";
  const tableName = params[tableNameAttr] ?? "";
  const indexName = params[indexNameAttr] ?? "";

  const timeScale = useSelector(selectTimeScale);
  const dispatch = useDispatch();

  const onTimeScaleChange = (
    ts: Parameters<typeof setGlobalTimeScaleAction>[0],
  ) => {
    dispatch(setGlobalTimeScaleAction(ts));
  };

  return (
    <IndexDetailsPageComponent
      databaseName={databaseName}
      tableName={tableName}
      indexName={indexName}
      timeScale={timeScale}
      onTimeScaleChange={onTimeScaleChange}
    />
  );
};
