import _ from "lodash";
import * as React from "react";
import classNames from "classnames";

import { MetricsDataProvider } from "../containers/metricsDataProvider";
import { MetricsDataComponentProps } from "../components/graphs";

interface SummaryValueProps {
  title: React.ReactNode;
  value: React.ReactNode;
  classModifier?: string;
}

interface SummaryStatProps {
  title: React.ReactNode;
  value?: number;
  format?: (n: number) => string;
}

interface SummaryHeadlineStatProps extends SummaryStatProps {
  tooltip?: string;
}

interface SummaryStatMessageProps {
  message: string;
}

interface SummaryStatBreakdownProps {
  title: React.ReactNode;
  tooltip?: string;
  value?: number;
  format?: (i: number) => string;
  modifier?: "dead" | "suspect" | "healthy";
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

/**
 * SummaryBar is a simple component backing a common motif in our UI - a
 * collection of summarized statistics.
 */
export function SummaryBar(props: { children?: React.ReactNode }) {
  return <div className="summary-section">
    { props.children }
  </div>;
}

/**
 * SummaryValue places a single labeled value onto a summary bar; this
 * consists of a label and a formatted value. Summary stats are visually
 * separated from other summary stats. A summary stat can contain children, such
 * as messages and breakdowns.
 */
export function SummaryValue(props: SummaryValueProps & {children?: React.ReactNode}) {
  const topClasses = classNames(
    "summary-stat",
    props.classModifier ? `summary-stat--${props.classModifier}` : null,
  );
  return (
    <div className={topClasses}>
      <div className="summary-stat__body">
        <span className="summary-stat__title">
          { props.title }
        </span>
        <span className="summary-stat__value">
          { props.value }
        </span>
      </div>
      { props.children }
    </div>
  );
}

/**
 * SummaryStat is a convenience component for SummaryValues where the value
 * consists of a single formatted number; it automatically handles cases where
 * the value is a non-numeric value and applies an appearance modifier specific
 * to numeric values.
 */
export function SummaryStat(props: SummaryStatProps & {children?: React.ReactNode}) {
  return (
    <SummaryValue
      title={props.title}
      value={computeValue(props.value, props.format)}
      classModifier="number"
    >
      {props.children}
    </SummaryValue>
  );
}

/**
 * SummaryLabel places a label onto a SummaryBar without a corresponding
 * statistic. This can be used to label a section of the bar.
 */
export function SummaryLabel(props: {children?: React.ReactNode}) {
  return <div className="summary-label">
    { props.children }
  </div>;
}

/**
 * SummaryStatMessage can be placed inside of a SummaryStat to provide visible
 * descriptive information about that statistic.
 */
export function SummaryStatMessage(props: SummaryStatMessageProps & {children?: React.ReactNode}) {
  return <span className="summary-stat__tooltip">{ props.message }</span>;
}

/**
 * SummaryStatBreakdown can be placed inside of a SummaryStat to provide
 * a detailed breakdown of the main statistic. Each breakdown contains a label
 * and numeric statistic.
 */
export function SummaryStatBreakdown(props: SummaryStatBreakdownProps & {children?: React.ReactNode}) {
  const modifierClass = props.modifier ? `summary-stat-breakdown--${props.modifier}` : null;
  return <div className={classNames("summary-stat-breakdown", modifierClass)}>
    <div className="summary-stat-breakdown__body">
      <span className="summary-stat-breakdown__title">
        { props.title }
      </span>
      <span className="summary-stat-breakdown__value">
        { computeValue(props.value, props.format) }
      </span>
    </div>
  </div>;
}

/**
 * SummaryMetricStat is a helpful component that creates a SummaryStat where
 * metric data is automatically derived from a metric component.
 */
export function SummaryMetricStat(propsWithID: SummaryStatProps & { id: string } & { children?: React.ReactNode }) {
  const { id, ...props } = propsWithID;
  return <MetricsDataProvider current id={id} >
    <SummaryMetricStatHelper {...props} />
  </MetricsDataProvider>;
}

function SummaryMetricStatHelper(props: MetricsDataComponentProps & SummaryStatProps & { children?: React.ReactNode }) {
  const datapoints = props.data && props.data.results && props.data.results[0] && props.data.results[0].datapoints;
  const value = datapoints && datapoints[0] && _.last(datapoints).value;
  const {title, format} = props;
  return <SummaryStat title={title} format={format} value={_.isNumber(value) ? value : props.value} />;
}

/**
 * SummaryHeadlineStat is similar to a normal SummaryStat, but is visually laid
 * out to draw attention to the numerical statistic.
 */
export class SummaryHeadlineStat extends React.Component<SummaryHeadlineStatProps, {}> {
  render() {
    return <div className="summary-headline">
      <div className="summary-headline__value">{computeValue(this.props.value, this.props.format)}</div>
      <div className="summary-headline__title">{this.props.title}</div>
    </div>;
  }
}
