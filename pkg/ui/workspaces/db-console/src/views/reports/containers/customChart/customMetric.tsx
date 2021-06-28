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
import * as React from "react";
import Select from "react-select";

import * as protos from "src/js/protos";
import { AxisUnits } from "src/views/shared/components/metricQuery";
import Dropdown, { DropdownOption } from "src/views/shared/components/dropdown";

import { MetricOption } from "./metricOption";

import TimeSeriesQueryAggregator = protos.cockroach.ts.tspb.TimeSeriesQueryAggregator;
import TimeSeriesQueryDerivative = protos.cockroach.ts.tspb.TimeSeriesQueryDerivative;

const axisUnitsOptions: DropdownOption[] = [
  AxisUnits.Count,
  AxisUnits.Bytes,
  AxisUnits.Duration,
].map((au) => ({ label: AxisUnits[au], value: au.toString() }));

const downsamplerOptions: DropdownOption[] = [
  TimeSeriesQueryAggregator.AVG,
  TimeSeriesQueryAggregator.MAX,
  TimeSeriesQueryAggregator.MIN,
  TimeSeriesQueryAggregator.SUM,
].map((agg) => ({
  label: TimeSeriesQueryAggregator[agg],
  value: agg.toString(),
}));

const aggregatorOptions = downsamplerOptions;

const derivativeOptions: DropdownOption[] = [
  { label: "Normal", value: TimeSeriesQueryDerivative.NONE.toString() },
  { label: "Rate", value: TimeSeriesQueryDerivative.DERIVATIVE.toString() },
  {
    label: "Non-negative Rate",
    value: TimeSeriesQueryDerivative.NON_NEGATIVE_DERIVATIVE.toString(),
  },
];

export class CustomMetricState {
  metric: string;
  downsampler = TimeSeriesQueryAggregator.AVG;
  aggregator = TimeSeriesQueryAggregator.SUM;
  derivative = TimeSeriesQueryDerivative.NONE;
  perNode = false;
  source = "";
}

export class CustomChartState {
  metrics: CustomMetricState[];
  axisUnits: AxisUnits = AxisUnits.Count;

  constructor() {
    this.metrics = [new CustomMetricState()];
  }
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
    this.props.onChange(
      this.props.index,
      _.assign(this.props.rowState, newState),
    );
  }

  changeMetric = (selectedOption: DropdownOption) => {
    this.changeState({
      metric: selectedOption.value,
    });
  };

  changeDownsampler = (selectedOption: DropdownOption) => {
    this.changeState({
      downsampler: +selectedOption.value,
    });
  };

  changeAggregator = (selectedOption: DropdownOption) => {
    this.changeState({
      aggregator: +selectedOption.value,
    });
  };

  changeDerivative = (selectedOption: DropdownOption) => {
    this.changeState({
      derivative: +selectedOption.value,
    });
  };

  changeSource = (selectedOption: DropdownOption) => {
    this.changeState({
      source: selectedOption.value,
    });
  };

  changePerNode = (selection: React.FormEvent<HTMLInputElement>) => {
    this.changeState({
      perNode: selection.currentTarget.checked,
    });
  };

  deleteOption = () => {
    this.props.onDelete(this.props.index);
  };

  render() {
    const {
      metricOptions,
      nodeOptions,
      rowState: {
        metric,
        downsampler,
        aggregator,
        derivative,
        source,
        perNode,
      },
    } = this.props;

    return (
      <tr>
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
              optionComponent={MetricOption}
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
              options={downsamplerOptions}
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
        <td className="metric-table__cell">
          <input
            type="checkbox"
            checked={perNode}
            onChange={this.changePerNode}
          />
        </td>
        <td className="metric-table__cell">
          <button
            className="edit-button metric-edit-button"
            onClick={this.deleteOption}
          >
            Remove Metric
          </button>
        </td>
      </tr>
    );
  }
}

interface CustomChartTableProps {
  metricOptions: DropdownOption[];
  nodeOptions: DropdownOption[];
  index: number;
  chartState: CustomChartState;
  onChange: (index: number, newState: CustomChartState) => void;
  onDelete: (index: number) => void;
}

export class CustomChartTable extends React.Component<CustomChartTableProps> {
  currentMetrics() {
    return this.props.chartState.metrics;
  }

  addMetric = () => {
    this.props.onChange(this.props.index, {
      metrics: [...this.currentMetrics(), new CustomMetricState()],
      axisUnits: this.currentAxisUnits(),
    });
  };

  updateMetricRow = (index: number, newState: CustomMetricState) => {
    const metrics = this.currentMetrics().slice();
    metrics[index] = newState;
    this.props.onChange(this.props.index, {
      metrics,
      axisUnits: this.currentAxisUnits(),
    });
  };

  removeMetric = (index: number) => {
    const metrics = this.currentMetrics();
    this.props.onChange(this.props.index, {
      metrics: metrics.slice(0, index).concat(metrics.slice(index + 1)),
      axisUnits: this.currentAxisUnits(),
    });
  };

  currentAxisUnits(): AxisUnits {
    return this.props.chartState.axisUnits;
  }

  changeAxisUnits = (selected: DropdownOption) => {
    this.props.onChange(this.props.index, {
      metrics: this.currentMetrics(),
      axisUnits: +selected.value,
    });
  };

  removeChart = () => {
    this.props.onDelete(this.props.index);
  };

  render() {
    const metrics = this.currentMetrics();
    let table: JSX.Element = (
      <h3>Click "Add Metric" to add a metric to the custom chart.</h3>
    );

    if (!_.isEmpty(metrics)) {
      table = (
        <table className="metric-table">
          <thead>
            <tr>
              <td className="metric-table__header">Metric Name</td>
              <td className="metric-table__header">Downsampler</td>
              <td className="metric-table__header">Aggregator</td>
              <td className="metric-table__header">Rate</td>
              <td className="metric-table__header">Source</td>
              <td className="metric-table__header">Per Node</td>
              <td className="metric-table__header"></td>
            </tr>
          </thead>
          <tbody>
            {metrics.map((row, i) => (
              <CustomMetricRow
                key={i}
                metricOptions={this.props.metricOptions}
                nodeOptions={this.props.nodeOptions}
                index={i}
                rowState={row}
                onChange={this.updateMetricRow}
                onDelete={this.removeMetric}
              />
            ))}
          </tbody>
        </table>
      );
    }

    return (
      <div>
        <div className="custom-metric__chart-controls-container">
          <Dropdown
            title="Units"
            selected={this.currentAxisUnits().toString()}
            options={axisUnitsOptions}
            onChange={this.changeAxisUnits}
          />
          <button
            className="edit-button chart-edit-button chart-edit-button--remove"
            onClick={this.removeChart}
          >
            Remove Chart
          </button>
        </div>
        {table}
        <button
          className="edit-button metric-edit-button metric-edit-button--add"
          onClick={this.addMetric}
        >
          Add Metric
        </button>
      </div>
    );
  }
}
