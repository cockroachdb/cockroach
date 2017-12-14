import d3 from "d3";
import moment from "moment";
import React from "react";
import { connect } from "react-redux";
import _ from "lodash";

import { hoverStateSelector, HoverState } from "src/redux/hover";
import { MetricQuerySet } from "src/redux/metrics";
import { nodesSummarySelector, NodesSummary } from "src/redux/nodes";
import { AdminUIState } from "src/redux/state";
import { ChartConfig, isMetricsChart, isNodesChart, Measure } from "src/util/charts";
import { NanoToMilli } from "src/util/convert";
import { Bytes, Count, Duration } from "src/util/format";

import { charts as distributedCharts } from "src/views/cluster/containers/nodeGraphs/dashboards/distributed";
import { charts as overviewCharts } from "src/views/cluster/containers/nodeGraphs/dashboards/overview";
import { charts as queuesCharts } from "src/views/cluster/containers/nodeGraphs/dashboards/queues";
import { charts as replicationCharts } from "src/views/cluster/containers/nodeGraphs/dashboards/replication";
import { charts as requestsCharts } from "src/views/cluster/containers/nodeGraphs/dashboards/requests";
import { charts as runtimeCharts } from "src/views/cluster/containers/nodeGraphs/dashboards/runtime";
import { charts as sqlCharts } from "src/views/cluster/containers/nodeGraphs/dashboards/sql";
import { charts as storageCharts } from "src/views/cluster/containers/nodeGraphs/dashboards/storage";
import { nodeDisplayName } from "src/views/cluster/containers/nodeGraphs/dashboards/dashboardUtils";
import { seriesPalette } from "src/views/cluster/util/graphs";

interface DetailsTooltipOwnProps {
  hoverState: HoverState;
  metrics: MetricQuerySet;
  nodesSummary: NodesSummary;
}

interface DetailsTooltipConnectedProps {
  nodeSources: string[];
}

type DetailsTooltipProps = DetailsTooltipOwnProps & DetailsTooltipConnectedProps;

interface DetailsTooltipMetric {
  title: string;
  color: string;
  value: string;
}

type Formatter = (n: number) => string;
function getFormatter(m: Measure): Formatter {
  switch (m) {
    case "bytes": return Bytes;
    case "count": return Count;
    case "duration": return Duration;
    default: return _.identity;
  }
}

const charts: ChartConfig = _.assign(
  {},
  distributedCharts,
  overviewCharts,
  queuesCharts,
  replicationCharts,
  requestsCharts,
  runtimeCharts,
  sqlCharts,
  storageCharts,
);

class DetailsTooltip extends React.Component<DetailsTooltipProps, {}> {
  render() {
    const { hoverChart, hoverTime, x, y } = this.props.hoverState;

    if (!hoverChart) {
      return null;
    }

    // TODO(couchand): make this safer
    const data = this.props.metrics[hoverChart].data.results;

    const bisect = d3.bisector((d: any) => NanoToMilli(d.timestamp_nanos.toNumber())).left;

    const { nodeSources } = this.props;
    const nodeIDs = nodeSources ? nodeSources : this.props.nodesSummary.nodeIDs;

    if (!(hoverChart in charts)) {
      return null;
    }

    const currentChart = charts[hoverChart];
    const formatValue = getFormatter(currentChart.measure);

    let metrics: DetailsTooltipMetric[];
    if (isMetricsChart(currentChart)) {
      metrics = currentChart.metrics
        .map(({ title }, i) => ({ title, datapoints: data[i].datapoints }))
        .map(({ title, datapoints }, i) => {
          let value = "-";
          if (datapoints && datapoints.length) {
            const index = bisect(datapoints, moment(hoverTime).valueOf());
            value = formatValue(datapoints[index].value);
          }
          return {
            title,
            color: seriesPalette[i], // TODO(couchand): this seems hacky.
            value,
          };
        });
    } else if (isNodesChart(currentChart)) {
      metrics = nodeIDs
        .map((id, i) => ({ id, datapoints: data[i].datapoints }))
        .filter(({ datapoints }) => datapoints && datapoints.length )
        .map(({ id, datapoints }, i) => {
          const index = bisect(datapoints, moment(hoverTime).valueOf());
          return {
            title: nodeDisplayName(this.props.nodesSummary, id),
            color: seriesPalette[i], // TODO(couchand): this seems hacky.
            value: formatValue(datapoints[index].value),
          };
        });
    }

    const time = moment(hoverTime).utc();
    return (
      <div className="hover-thing nvtooltip xy-tooltip" style={{
        position: "absolute",
        left: 0,
        top: 0,
        transform: `translate(${x + 25}px,${y - 20}px)`,
      }}>
        {/* TODO(couchand): revisit this markup */}
        <table>
          <thead>
            <tr>
              <td colSpan={3}>
                  <strong className="x-value">
                    { time.format("HH:mm:ss") }
                    { " " }
                    <span className="legend-subtext">on</span>
                    { " " }
                    { time.format("MMM Do, YYYY") }
                  </strong>
              </td>
            </tr>
          </thead>
          <tbody>
            {
              metrics.map((metric) => (
                <tr>
                  <td className="legend-color-guide">
                    <svg width={9} height={9}>
                      <circle cx={4.5} cy={4.5} r={3.5} fill={metric.color} />
                    </svg>
                  </td>
                  <td className="key">{ metric.title }</td>
                  <td className="value">{ metric.value }</td>
                </tr>
              ))
            }
          </tbody>
        </table>
      </div>
    );
  }
}

function mapStateToProps(state: AdminUIState): DetailsTooltipOwnProps {
  return {
    hoverState: hoverStateSelector(state),
    metrics: state.metrics.queries,
    nodesSummary: nodesSummarySelector(state),
  };
}

// tslint:disable-next-line:variable-name
const DetailsTooltipConnected = connect<DetailsTooltipOwnProps, {}, DetailsTooltipConnectedProps>(mapStateToProps)(DetailsTooltip);
export default DetailsTooltipConnected;
