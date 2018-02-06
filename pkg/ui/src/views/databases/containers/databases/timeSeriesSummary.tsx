import React from "react";
import { connect } from "react-redux";

import * as protos from "src/js/protos";
import { refreshTimeSeriesStats } from "src/redux/apiReducers";
import { AdminUIState } from "src/redux/state";
import { Bytes } from "src/util/format";

interface TimeSeriesSummaryProps {
  timeSeriesStats: protos.cockroach.server.serverpb.TableStatsResponse;
  // Must be connected to react-redux in order to auto-refresh time series
  // information.
  timeSeriesStatsValid: boolean;
  refreshTimeSeriesStats: typeof refreshTimeSeriesStats;
}

// TimeSeriesSummary displays a summary section describing the current data
// usage of the time series system.
class TimeSeriesSummary extends React.Component<TimeSeriesSummaryProps> {
  componentWillMount() {
    // Refresh nodes status query when mounting.
    this.props.refreshTimeSeriesStats();
  }

  componentWillReceiveProps(props: TimeSeriesSummaryProps) {
    // Refresh nodes status query when props are received; this will immediately
    // trigger a new request if previous results are invalidated.
    props.refreshTimeSeriesStats();
  }

  renderTable() {
    return (
      <div className="database-summary-table sql-table">
        <table className="sort-table">
          <thead>
            <tr className="sort-table__row sort-table__row--header">
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
                { Bytes(this.props.timeSeriesStats.approximate_disk_bytes.toNumber()) }
              </td>
              <td className="sort-table__cell">
                { this.props.timeSeriesStats.range_count.toNumber() }
              </td>
            </tr>
          </tbody>
        </table>
      </div>
    );
  }

  render() {
    const hasData = this.props.timeSeriesStats != null;
    return (
      <div className="database-summary">
        <div className="database-summary-title">
          <h2>Time Series Data</h2>
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
const timeSeriesData = (state: AdminUIState) => state.cachedData.timeSeriesStats;

function mapStateToProps(state: AdminUIState) {
  const tsData = timeSeriesData(state);
  return {
    timeSeriesStats: tsData && tsData.data,
    timeSeriesStatsValid: tsData && tsData.valid,
  };
}

const mapDispatchToProps = {
  refreshTimeSeriesStats,
};

export default connect(mapStateToProps, mapDispatchToProps)(TimeSeriesSummary);
