// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import React from "react";
import { connect } from "react-redux";
import * as protos from "src/js/protos";
import { refreshNonTableStats } from "src/redux/apiReducers";
import { AdminUIState } from "src/redux/state";
import { FixLong } from "src/util/fixLong";
import { Bytes } from "src/util/format";
import { Loading } from "@cockroachlabs/cluster-ui";
import { CachedDataReducerState } from "src/redux/cachedDataReducer";
import { NonTableStatsResponseMessage } from "src/util/api";
import { TimeSeriesTooltip } from "src/views/databases/containers/databases/tooltips";
import "src/views/shared/components/sortabletable/sortabletable.styl";

interface TimeSeriesSummaryProps {
  nonTableStats: protos.cockroach.server.serverpb.NonTableStatsResponse;
  // Must be connected to react-redux in order to auto-refresh time series
  // information.
  nonTableStatsValid: boolean;
  refreshNonTableStats: typeof refreshNonTableStats;
  lastError: CachedDataReducerState<NonTableStatsResponseMessage>["lastError"];
}

// NonTableSummary displays a summary section describing the current data
// usage of the time series system.
export class NonTableSummary extends React.Component<TimeSeriesSummaryProps> {
  componentDidMount() {
    // Refresh nodes status query when mounting.
    this.props.refreshNonTableStats();
  }

  componentDidUpdate() {
    // Refresh nodes status query when props are received; this will immediately
    // trigger a new request if previous results are invalidated.
    this.props.refreshNonTableStats();
  }

  renderTable = () => {
    return (
      <div className="database-summary-table sql-table">
        <table className="sort-table">
          <thead>
            <tr className="sort-table__row sort-table__row--header">
              <td className="sort-table__cell">Data Type</td>
              <td className="sort-table__cell">Size</td>
              <td className="sort-table__cell">Ranges</td>
            </tr>
          </thead>
          <tbody>
            <tr className="sort-table__row sort-table__row--body">
              <td className="sort-table__cell">
                <TimeSeriesTooltip>Time Series</TimeSeriesTooltip>
              </td>
              <td className="sort-table__cell">
                {Bytes(
                  FixLong(
                    this.props.nonTableStats.time_series_stats
                      .approximate_disk_bytes,
                  ).toNumber(),
                )}
              </td>
              <td className="sort-table__cell">
                {FixLong(
                  this.props.nonTableStats.time_series_stats.range_count,
                ).toNumber()}
              </td>
            </tr>
            <tr className="sort-table__row sort-table__row--body">
              <td className="sort-table__cell">Internal Use</td>
              <td className="sort-table__cell">
                {Bytes(
                  FixLong(
                    this.props.nonTableStats.internal_use_stats
                      .approximate_disk_bytes,
                  ).toNumber(),
                )}
              </td>
              <td className="sort-table__cell">
                {FixLong(
                  this.props.nonTableStats.internal_use_stats.range_count,
                ).toNumber()}
              </td>
            </tr>
          </tbody>
        </table>
      </div>
    );
  };

  render() {
    return (
      <div className="database-summary">
        <div className="database-summary-title">
          <h2 className="base-heading">Non-Table Cluster Data</h2>
        </div>
        <div className="l-columns">
          <div className="l-columns__left">
            <Loading
              loading={!this.props.nonTableStats}
              error={this.props.lastError}
              render={this.renderTable}
            />
          </div>
          <div className="l-columns__right" />
        </div>
      </div>
    );
  }
}

// Base selectors to extract data from redux state.
const nonTableStatsData = (state: AdminUIState) =>
  state.cachedData.nonTableStats;

const mapStateToProps = (state: AdminUIState) => {
  const ntStats = nonTableStatsData(state);
  return {
    nonTableStats: ntStats && ntStats.data,
    nonTableStatsValid: ntStats && ntStats.valid,
    lastError: ntStats && ntStats.lastError,
  };
};

const mapDispatchToProps = {
  refreshNonTableStats,
};

export default connect(mapStateToProps, mapDispatchToProps)(NonTableSummary);
