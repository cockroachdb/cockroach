// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { Moment } from "moment-timezone";
import React from "react";

import {
  IndexRecommendation,
  IndexRecTypeEnum,
} from "../api/databases/tableIndexesApi";
import { QuoteIdentifier } from "../api/safesql";
import IdxRecAction from "../insights/indexActionBtn";
import { Timestamp } from "../timestamp";
import { DATE_FORMAT_24_TZ, minDate } from "../util";

export const LastReset = ({
  lastReset,
}: {
  lastReset: Moment;
}): JSX.Element => {
  return (
    <span>
      Last reset:{" "}
      {lastReset.isSame(minDate) ? (
        "Never"
      ) : (
        <Timestamp time={lastReset} format={DATE_FORMAT_24_TZ} />
      )}
    </span>
  );
};

export interface IndexStat {
  indexName: string;
  totalReads: number;
  lastUsed: Moment;
  lastUsedType: string;
  indexRecommendations: IndexRecommendation[];
}

export const ActionCell = ({
  indexStat,
  tableName,
  databaseName,
}: {
  indexStat: Pick<IndexStat, "indexName" | "indexRecommendations">;
  tableName: string;
  databaseName: string;
}): JSX.Element => {
  const query = indexStat.indexRecommendations.map(recommendation => {
    switch (recommendation.type) {
      case IndexRecTypeEnum.DROP_UNUSED:
        // Here, `tableName` is a fully qualified name whose identifiers have already been quoted.
        // See the QuoteIdentifier unit tests for more details.
        return `DROP INDEX ${tableName}@${QuoteIdentifier(
          indexStat.indexName,
        )};`;
    }
  });
  if (query.length === 0) {
    return <></>;
  }

  return (
    <IdxRecAction
      actionQuery={query.join(" ")}
      actionType={"DropIndex"}
      database={databaseName}
    />
  );
};
