import * as _ from "lodash";
import * as React from "react";

import { MetricsDataProvider } from "../containers/metricsDataProvider";
import { MetricsDataComponentProps } from "../components/graphs";

interface SummaryStatProps {
  title: React.ReactNode;
  tooltip?: string;
  value?: number;
  format?: (i: number) => string;
}

function numberToString(n: number) {
  return n.toString();
}

function computeValue(value: number, format: (n: number) => string = numberToString) {
  if (!_.isNumber(value)) {
    return "-";
  }
  return format(value);
}

// SummaryBar is a simple component backing a common motif in our UI: a
// collection of summarized statistics.
// TODO(mrtracy): Add tooltip support.
export function SummaryBar(props: { children?: any }) {
  return <div className="summary-section">
    { props.children }
  </div>;
}

export function SummaryLabel(props: {children?: any}) {
  return <div className="summary-label">
    { props.children }
  </div>;
}

export function SummaryStat(props: SummaryStatProps & {children?: any}) {
  return <div className="summary-stat">
    <span className="summary-stat__title">
      { props.title }
    </span>
    <span className="summary-stat__value">
      { computeValue(props.value, props.format) }
    </span>
    {
      !!props.tooltip ?
        <span className="summary-stat__tooltip">{ props.tooltip }</span>
        : null
    }
  </div>;
}

function SummaryMetricStatHelper(props: MetricsDataComponentProps & SummaryStatProps & { children?: any }) {
  let datapoints = props.data && props.data.results && props.data.results[0] && props.data.results[0].datapoints;
  let value = datapoints && datapoints[0] && _.last(datapoints).value;
  return <SummaryStat {...props} value={_.isNumber(value) ? value : props.value} />;
}

export function SummaryMetricStat(props: SummaryStatProps & { id: string } & { children?: any }) {
  return <MetricsDataProvider current id={props.id} >
    <SummaryMetricStatHelper {...props} />
  </MetricsDataProvider>;
}

// SummaryHeadlineStat displays a single item in a summary bar as a "Headline
// Stat", which is formatted to place attention on the number.
export class SummaryHeadlineStat extends React.Component<SummaryStatProps, {}> {
  render() {
    return <div className="summary-headline">
      <div className="summary-headline__value">{computeValue(this.props.value, this.props.format)}</div>
      <div className="summary-headline__title">{this.props.title}</div>
    </div>;
  }
}
