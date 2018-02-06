import React from "react";
import { connect } from "react-redux";

import * as protos from "src/js/protos";
import { refreshNonTableStats } from "src/redux/apiReducers";
import { AdminUIState } from "src/redux/state";
import { Bytes } from "src/util/format";
import { FixLong } from "src/util/fixLong";

interface TimeSeriesSummaryProps {
  nonTableStats: protos.cockroach.server.serverpb.NonTableStatsResponse;
  // Must be connected to react-redux in order to auto-refresh time series
  // information.
  nonTableStatsValid: boolean;
  refreshNonTableStats: typeof refreshNonTableStats;
}

// NonTableSummary displays a summary section describing the current data
// usage of the time series system.
class NonTableSummary extends React.Component<TimeSeriesSummaryProps> {
  componentWillMount() {
    // Refresh nodes status query when mounting.
    this.props.refreshNonTableStats();
  }

  componentWillReceiveProps(props: TimeSeriesSummaryProps) {
    // Refresh nodes status query when props are received; this will immediately
    // trigger a new request if previous results are invalidated.
    props.refreshNonTableStats();
  }

  renderTable() {
    return (
      <div className="database-summary-table sql-table">
        <table className="sort-table">
          <thead>
            <tr className="sort-table__row sort-table__row--header">
              <td className="sort-table__cell">
                Data Type
              </td>
              <td className="sort-table__cell">
                Size
              </td>
              <td className="sort-table__cell">
                Ranges
              </td>
            </tr>
          </thead>
          <tbody>
            <tr className="sort-table__row sort-table__row--body">
              <td className="sort-table__cell">
                Time Series
              </td>
              <td className="sort-table__cell">
                { Bytes(FixLong(this.props.nonTableStats.time_series_stats.approximate_disk_bytes).toNumber()) }
              </td>
              <td className="sort-table__cell">
                { FixLong(this.props.nonTableStats.time_series_stats.range_count).toNumber() }
              </td>
            </tr>
          </tbody>
        </table>
      </div>
    );
  }

  render() {
    const hasData = this.props.nonTableStats != null;
    return (
      <div className="database-summary">
        <div className="database-summary-title">
          <h2>Non-Table Cluster Data</h2>
        </div>
        <div className="l-columns">
          <div className="l-columns__left">
            { hasData ? this.renderTable() : "loading..." }
          </div>
          <div className="l-columns__right" />
        </div>
      </div>
    );
  }
}

// Base selectors to extract data from redux state.
const nonTableStatsData = (state: AdminUIState) => state.cachedData.nonTableStats;

function mapStateToProps(state: AdminUIState) {
  const ntStats = nonTableStatsData(state);
  return {
    nonTableStats: ntStats && ntStats.data,
    nonTableStatsValid: ntStats && ntStats.valid,
  };
}

const mapDispatchToProps = {
  refreshNonTableStats,
};

export default connect(mapStateToProps, mapDispatchToProps)(NonTableSummary);
