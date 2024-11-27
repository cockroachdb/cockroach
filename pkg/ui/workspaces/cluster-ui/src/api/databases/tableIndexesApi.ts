// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { cockroach } from "@cockroachlabs/crdb-protobuf-client";
import moment from "moment-timezone";
import { useMemo } from "react";

import { TimestampToMoment, useSwrWithClusterId } from "src/util";

import { getIndexStats, resetIndexStats } from "../indexDetailsApi";
import { QualifiedIdentifier } from "../safesql";

import ResetIndexUsageStatsResponse = cockroach.server.serverpb.ResetIndexUsageStatsResponse;

type GetTableIndexesRequest = {
  dbName: string;
  schemaName: string;
  tableName: string;
};

type IndexRecType = cockroach.sql.IndexRecommendation.RecommendationType;
export const IndexRecTypeEnum =
  cockroach.sql.IndexRecommendation.RecommendationType;

export type IndexRecommendation = {
  id: number;
  type: IndexRecType;
  reason: string;
};

export type TableIndex = {
  dbName: string;
  indexName: string;
  tableName: string;
  escSchemaQualifiedTableName: string;
  indexType: string;
  lastRead: moment.Moment | null;
  totalReads: number;
  totalRowsRead: number;
  indexRecs: IndexRecommendation[];
};

export const useTableIndexStats = ({
  dbName,
  schemaName,
  tableName,
}: GetTableIndexesRequest) => {
  const makeRequest = dbName && tableName;
  const { data, isLoading, error, mutate } = useSwrWithClusterId(
    { name: "tableIndexes", dbName, tableName },
    makeRequest
      ? () =>
          getIndexStats(
            new cockroach.server.serverpb.TableIndexStatsRequest({
              database: dbName,
              table: new QualifiedIdentifier([
                schemaName,
                tableName,
              ]).sqlString(),
            }),
          )
      : null,
  );

  const tableIndexes: TableIndex[] = useMemo(() => {
    if (!data) {
      return [];
    }
    return data.statistics.map(stat => {
      const lastRead = TimestampToMoment(
        stat.statistics.stats.last_read,
        moment.unix(0),
      );
      return {
        indexName: stat.index_name,
        dbName: dbName,
        escSchemaQualifiedTableName: new QualifiedIdentifier([
          schemaName,
          tableName,
        ]).sqlString(),
        tableName: tableName,
        indexType: stat.index_type?.toLowerCase() ?? "",
        lastRead: lastRead.unix() <= 0 ? null : lastRead,
        totalReads: stat.statistics?.stats?.total_read_count?.toNumber() ?? 0,
        totalRowsRead:
          stat?.statistics?.stats?.total_rows_read?.toNumber() ?? 0,
        indexRecs:
          data?.index_recommendations
            ?.filter(rec => rec?.type != null)
            .map(formatIndexRecsProto) ?? [],
      };
    });
  }, [data, dbName, schemaName, tableName]);

  const lastReset = TimestampToMoment(data?.last_reset, moment.unix(0));

  return {
    indexStats: {
      tableIndexes,
      lastReset: lastReset.unix() <= 0 ? null : lastReset,
    },
    isLoading,
    error,
    refreshIndexStats: mutate,
  };
};

const formatIndexRecsProto = (
  indexRecs: cockroach.sql.IIndexRecommendation,
): IndexRecommendation => {
  return {
    id: indexRecs.index_id,
    type: indexRecs.type as IndexRecType,
    reason: indexRecs.reason ?? "",
  };
};

// This is a more user-friendly wrapper around resetIndexStats in indexDetailsApi.ts

export const resetIndexStatsApi =
  async (): Promise<ResetIndexUsageStatsResponse> => {
    return resetIndexStats(
      new cockroach.server.serverpb.ResetIndexUsageStatsRequest({}),
    );
  };
