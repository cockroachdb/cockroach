import _ from "lodash";
import * as React from "react";
import Select from "react-select";

import * as protos from  "src/js/protos";
import { DropdownOption } from "src/views/shared/components/dropdown";

import TimeSeriesQueryAggregator = protos.cockroach.ts.tspb.TimeSeriesQueryAggregator;
import TimeSeriesQueryDerivative = protos.cockroach.ts.tspb.TimeSeriesQueryDerivative;

const aggregatorOptions: DropdownOption[] = [
  TimeSeriesQueryAggregator.AVG,
  TimeSeriesQueryAggregator.MAX,
  TimeSeriesQueryAggregator.MIN,
  TimeSeriesQueryAggregator.SUM,
].map(agg => ({ label: TimeSeriesQueryAggregator[agg], value: agg.toString() }));

const derivativeOptions: DropdownOption[] = [
  { label: "Normal", value: TimeSeriesQueryDerivative.NONE.toString() },
  { label: "Rate", value: TimeSeriesQueryDerivative.DERIVATIVE.toString() },
  { label: "Non-negative Rate", value: TimeSeriesQueryDerivative.NON_NEGATIVE_DERIVATIVE.toString() },
];

export class CustomMetricState {
  metric: string;
  downsampler = TimeSeriesQueryAggregator.AVG;
  aggregator = TimeSeriesQueryAggregator.SUM;
  derivative = TimeSeriesQueryDerivative.NONE;
  source = "";
}

interface CustomMetricRowProps {
  metricOptions: DropdownOption[];
  nodeOptions: DropdownOption[];
  index: number;
  rowState: CustomMetricState;
  onChange: (index: number, newState: CustomMetricState) => void;
  onDelete: (index: number) => void;
}

export class CustomMetricRow extends React.Component<CustomMetricRowProps> {
  changeState(newState: Partial<CustomMetricState>) {
    this.props.onChange(this.props.index, _.assign(this.props.rowState, newState));
  }

  changeMetric = (selectedOption: DropdownOption) => {
    this.changeState({
      metric: selectedOption.value,
    });
  }

  changeDownsampler = (selectedOption: DropdownOption) => {
    this.changeState({
      downsampler: +selectedOption.value,
    });
  }

  changeAggregator = (selectedOption: DropdownOption) => {
    this.changeState({
      aggregator: +selectedOption.value,
    });
  }

  changeDerivative = (selectedOption: DropdownOption) => {
    this.changeState({
      derivative: +selectedOption.value,
    });
  }

  changeSource = (selectedOption: DropdownOption) => {
    this.changeState({
      source: selectedOption.value,
    });
  }

  deleteOption = () => {
    this.props.onDelete(this.props.index);
  }

  render() {
    const {
      metricOptions,
      nodeOptions,
      rowState: { metric, downsampler, aggregator, derivative, source },
    } = this.props;

    return (
      <tr>
        <td>
          <button className="metric-edit-button" onClick={this.deleteOption}>X</button>
        </td>
        <td>
          <div className="metric-table-dropdown">
            <Select
              className="metric-table-dropdown__select"
              clearable={true}
              resetValue=""
              searchable={true}
              value={metric}
              options={metricOptions}
              onChange={this.changeMetric}
              placeholder="Select a metric..."
            />
          </div>
        </td>
        <td>
          <div className="metric-table-dropdown">
            <Select
              className="metric-table-dropdown__select"
              clearable={false}
              searchable={false}
              value={downsampler.toString()}
              options={aggregatorOptions}
              onChange={this.changeDownsampler}
            />
          </div>
        </td>
        <td>
          <div className="metric-table-dropdown">
            <Select
              className="metric-table-dropdown__select"
              clearable={false}
              searchable={false}
              value={aggregator.toString()}
              options={aggregatorOptions}
              onChange={this.changeAggregator}
            />
          </div>
        </td>
        <td>
          <div className="metric-table-dropdown">
            <Select
              className="metric-table-dropdown__select"
              clearable={false}
              searchable={false}
              value={derivative.toString()}
              options={derivativeOptions}
              onChange={this.changeDerivative}
            />
          </div>
        </td>
        <td>
          <div className="metric-table-dropdown">
            <Select
              className="metric-table-dropdown__select"
              clearable={false}
              searchable={false}
              value={source}
              options={nodeOptions}
              onChange={this.changeSource}
            />
          </div>
        </td>
      </tr>
    );
  }
}
