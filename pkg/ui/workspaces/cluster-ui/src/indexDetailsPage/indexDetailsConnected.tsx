// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import React from "react";
import { useDispatch, useSelector } from "react-redux";
import { useParams } from "react-router-dom";

import { actions as sqlStatsActions } from "../store/sqlStats";
import { selectTimeScale } from "../store/utils/selectors";
import { TimeScale } from "../timeScaleDropdown";
import { databaseNameAttr, indexNameAttr, tableNameAttr } from "../util";

import { IndexDetailsPage } from "./indexDetailsPage";

export const ConnectedIndexDetailsPage: React.FC = () => {
  const params = useParams<Record<string, string>>();
  const databaseName = params[databaseNameAttr] ?? "";
  const tableName = params[tableNameAttr] ?? "";
  const indexName = params[indexNameAttr] ?? "";

  const timeScale: TimeScale = useSelector(selectTimeScale);
  const dispatch = useDispatch();

  const onTimeScaleChange = (ts: TimeScale) => {
    dispatch(sqlStatsActions.updateTimeScale({ ts }));
  };

  return (
    <IndexDetailsPage
      databaseName={databaseName}
      tableName={tableName}
      indexName={indexName}
      timeScale={timeScale}
      onTimeScaleChange={onTimeScaleChange}
    />
  );
};
