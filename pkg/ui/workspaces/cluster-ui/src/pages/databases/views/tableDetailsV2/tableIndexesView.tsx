// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { Row, Tag } from "antd";
import React, { useContext } from "react";

import {
  resetIndexStatsApi,
  TableIndex,
  useTableIndexStats,
} from "src/api/databases/tableIndexesApi";
import { IndexStatsLink } from "src/components/links/indexStatsLink";
import { Tooltip } from "src/components/tooltip";
import { ActionCell } from "src/databaseTablePage/helperComponents";
import Button from "src/sharedFromCloud/button";
import { Table, TableColumnProps } from "src/sharedFromCloud/table";
import { Timestamp } from "src/timestamp";
import { DATE_WITH_SECONDS_FORMAT_24_TZ } from "src/util";

import { CockroachCloudContext } from "../contexts";

type TableIndexRow = TableIndex & {
  key: React.Key;
};

const COLUMNS: (TableColumnProps<TableIndexRow> & { hideIfCloud?: boolean })[] =
  [
    {
      title: "Index Name",
      render: (idx: TableIndexRow) => (
        <IndexStatsLink
          dbName={idx.dbName}
          escSchemaQualifiedTableName={idx.escSchemaQualifiedTableName}
          indexName={idx.indexName}
        />
      ),
    },
    {
      title: "Last Read",
      render: (idx: TableIndexRow) => (
        <Timestamp
          time={idx.lastRead}
          format={DATE_WITH_SECONDS_FORMAT_24_TZ}
          fallback={"Never"}
        />
      ),
    },
    {
      title: "Total Reads",
      sorter: true,
      render: (idx: TableIndexRow) => idx.totalReads,
      align: "right",
    },
    {
      title: (
        <Tooltip
          title={`Index recommendations will appear if the system detects improper index usage, 
    such as the occurrence of unused indexes. Following index recommendations may 
    help improve query performance.`}
        >
          Recommendations
        </Tooltip>
      ),
      sorter: true,
      render: (idx: TableIndexRow) => {
        if (idx.indexRecs.length === 0 || idx.indexType === "primary") {
          return "None";
        }
        const recs = idx.indexRecs.map((rec, i) => {
          // The only rec right now is "DROP_UNUSED".
          return (
            <Tooltip noUnderline key={i} title={rec.reason}>
              <Tag color="blue">Drop unused index</Tag>
            </Tooltip>
          );
        });
        return <span>{recs}</span>;
      },
    },
    {
      title: "Action",
      sorter: false,
      hideIfCloud: true,
      render: (idx: TableIndexRow) => {
        if (idx.indexRecs.length === 0 || idx.indexType === "primary") {
          return null;
        }

        const stat = {
          indexName: idx.indexName,
          indexRecommendations: idx.indexRecs,
        };
        // The action button expects an escaped schema qualified table name.
        return (
          <ActionCell
            indexStat={stat}
            tableName={idx.escSchemaQualifiedTableName}
            databaseName={idx.dbName}
          />
        );
      },
    },
  ];

type Props = {
  dbName: string;
  schemaName: string;
  tableName: string;
};

export const TableIndexesView: React.FC<Props> = ({
  dbName,
  tableName,
  schemaName,
}) => {
  const { indexStats, isLoading, refreshIndexStats } = useTableIndexStats({
    dbName: dbName,
    tableName: tableName,
    schemaName: schemaName,
  });
  const isCloud = useContext(CockroachCloudContext);

  const tableIndexRows = indexStats.tableIndexes.map((idx, i) => ({
    ...idx,
    key: i.toString(),
  }));

  const resetAllIndexStats = async () => {
    await resetIndexStatsApi();
    return refreshIndexStats();
  };

  const filteredCols = COLUMNS.filter(col => !isCloud || !col.hideIfCloud);

  return (
    <div>
      <Row align={"middle"} justify={"end"}>
        <Tooltip
          title={`Index stats accumulate from the time the index was created or had its stats reset. 
          Clicking ‘Reset all index stats’ will reset index stats for the entire cluster. Last 
          reset is the timestamp at which the last reset started.`}
        >
          Last reset:{" "}
          <Timestamp
            format={DATE_WITH_SECONDS_FORMAT_24_TZ}
            time={indexStats.lastReset}
            fallback={"Never"}
          />
        </Tooltip>
        <Button
          category={"tertiary"}
          onClick={resetAllIndexStats}
          text={"Reset all index stats"}
        />
      </Row>

      <Table
        loading={isLoading}
        columns={filteredCols}
        dataSource={tableIndexRows}
      />
    </div>
  );
};
