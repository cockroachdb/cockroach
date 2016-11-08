import _ from "lodash";
import * as React from "react";

interface SummaryItemProps {
  title: string;
  tooltip: string;
  value?: number;
  format?: (i: number) => string;
}

// SummaryBar is a simple component backing a common motif in our UI: a 
// collection of summarized statistics.
// TODO(mrtracy): Add tooltip support.
export function SummaryBar(props: { children?: any }) {
  return <div className="summary">
    { props.children }
  </div>;
}

// SummaryItem displays a single item in a summary bar.
export class SummaryItem extends React.Component<SummaryItemProps, {}> {
  computeValue(): string {
    let { value, format } = this.props;
    if (!_.isNumber(value)) {
      return "-";
    }
    if (_.isFunction(format)) {
      return format(value);
    }
    return value.toString();
  }

  render() {
    return <div className="summary-item">
      <div className="summary-value">{this.computeValue()}</div>
      <div className="summary-title">{this.props.title}</div>
    </div>;
  }
}
