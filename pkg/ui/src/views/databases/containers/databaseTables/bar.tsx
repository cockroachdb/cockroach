// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import _ from "lodash";
import React from "react";
import { connect } from "react-redux";
import { AdminUIState } from "src/redux/state";
import { Bytes } from "src/util/format";
import { DatabaseSummaryExplicitData, tableInfos as selectTableInfos } from "src/views/databases/containers/databaseSummary";
import { SummaryBar, SummaryHeadlineStat } from "src/views/shared/components/summaryBar";

interface ISummaryBarContainerProps {
  tableInfosStats: {
    totalSize: number;
    totalRangeCount: number;
    numTables?: number;
    totalBytes?: number;
  };
}

class SummaryBarContainer extends React.Component<ISummaryBarContainerProps> {
  render() {
    const { tableInfosStats } = this.props;
    return (
      <SummaryBar>
        <SummaryHeadlineStat
          title="Database Size"
          tooltip="Approximate total disk size of this database across all replicas."
          value={tableInfosStats.totalSize}
          format={Bytes} />
        <SummaryHeadlineStat
          title={(tableInfosStats.numTables === 1) ? "Table" : "Tables"}
          tooltip="The total number of tables in this database."
          value={tableInfosStats.numTables} />
        <SummaryHeadlineStat
          title="Total Range Count"
          tooltip="The total ranges across all tables in this database."
          value={tableInfosStats.totalRangeCount} />
      </SummaryBar>
    );
  }
}

const selectTableInfosStats = (infos: {
  physicalSize: number;
  rangeCount: number;
}[]) => {
  const totalSize = _.sumBy(infos, (ti) => ti.physicalSize);
  const totalRangeCount = _.sumBy(infos, (ti) => ti.rangeCount);
  const totalBytes = _.sumBy(infos, (ti) => parseInt(Bytes(ti.physicalSize), 10));
  const numTables = infos && infos.length || 0;

  return { totalSize, totalRangeCount, numTables, totalBytes };
};

const mapStateToProps = (state: AdminUIState, ownProps: DatabaseSummaryExplicitData) => ({
  tableInfosStats: selectTableInfosStats(selectTableInfos(state, ownProps.name)),
});

export default connect(
  mapStateToProps,
)(SummaryBarContainer as any);
