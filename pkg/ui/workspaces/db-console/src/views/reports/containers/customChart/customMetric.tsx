// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { AxisUnits } from "@cockroachlabs/cluster-ui";
import assign from "lodash/assign";
import isEmpty from "lodash/isEmpty";
import * as React from "react";
import Select, { Option } from "react-select";

import * as protos from "src/js/protos";
import { isSystemTenant } from "src/redux/tenants";
import Dropdown, { DropdownOption } from "src/views/shared/components/dropdown";

import { MetricOption } from "./metricOption";

import TimeSeriesQueryAggregator = protos.cockroach.ts.tspb.TimeSeriesQueryAggregator;
import TimeSeriesQueryDerivative = protos.cockroach.ts.tspb.TimeSeriesQueryDerivative;

const axisUnitsOptions: DropdownOption[] = [
  AxisUnits.Count,
  AxisUnits.Bytes,
  AxisUnits.Duration,
].map(au => ({ label: AxisUnits[au], value: au.toString() }));

const downsamplerOptions: DropdownOption[] = [
  TimeSeriesQueryAggregator.AVG,
  TimeSeriesQueryAggregator.MAX,
  TimeSeriesQueryAggregator.MIN,
  TimeSeriesQueryAggregator.SUM,
].map(agg => ({
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
  perSource = false;
  perTenant = false;
  nodeSource = "";
  tenantSource = "";
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
  tenantOptions: DropdownOption[];
  canViewTenantOptions: boolean;
  index: number;
  rowState: CustomMetricState;
  onChange: (index: number, newState: CustomMetricState) => void;
  onDelete: (index: number) => void;
}

export class CustomMetricRow extends React.Component<CustomMetricRowProps> {
  changeState(newState: Partial<CustomMetricState>) {
    this.props.onChange(
      this.props.index,
      assign(this.props.rowState, newState),
    );
  }

  changeMetric = (selectedOption: Option<string>) => {
    this.changeState({
      metric: selectedOption.value,
    });
  };

  changeDownsampler = (selectedOption: Option<string>) => {
    this.changeState({
      downsampler: +selectedOption.value,
    });
  };

  changeAggregator = (selectedOption: Option<string>) => {
    this.changeState({
      aggregator: +selectedOption.value,
    });
  };

  changeDerivative = (selectedOption: Option<string>) => {
    this.changeState({
      derivative: +selectedOption.value,
    });
  };

  changeNodeSource = (selectedOption: Option<string>) => {
    this.changeState({
      nodeSource: selectedOption.value,
    });
  };

  changeTenant = (selectedOption: Option<string>) => {
    this.changeState({
      tenantSource: selectedOption.value,
    });
  };

  changePerSource = (selection: React.FormEvent<HTMLInputElement>) => {
    this.changeState({
      perSource: selection.currentTarget.checked,
    });
  };

  changePerTenant = (selection: React.FormEvent<HTMLInputElement>) => {
    this.changeState({
      perTenant: selection.currentTarget.checked,
    });
  };

  deleteOption = () => {
    this.props.onDelete(this.props.index);
  };

  render() {
    const {
      metricOptions,
      nodeOptions,
      tenantOptions,
      canViewTenantOptions,
      rowState: {
        metric,
        downsampler,
        aggregator,
        derivative,
        nodeSource,
        perSource,
        tenantSource,
        perTenant,
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
              value={nodeSource}
              options={nodeOptions}
              onChange={this.changeNodeSource}
            />
          </div>
        </td>
        <td className="metric-table__cell">
          <input
            type="checkbox"
            checked={perSource}
            onChange={this.changePerSource}
          />
        </td>
        {canViewTenantOptions && (
          <td>
            <div className="metric-table-dropdown">
              <Select
                className="metric-table-dropdown__select"
                clearable={false}
                searchable={false}
                value={tenantSource}
                options={tenantOptions}
                onChange={this.changeTenant}
              />
            </div>
          </td>
        )}
        {canViewTenantOptions && (
          <td className="metric-table__cell">
            <input
              type="checkbox"
              checked={perTenant}
              onChange={this.changePerTenant}
            />
          </td>
        )}
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
  tenantOptions: DropdownOption[];
  currentTenant: string | null;
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
    const { tenantOptions, currentTenant } = this.props;
    const metrics = this.currentMetrics();
    const canViewTenantOptions =
      isSystemTenant(currentTenant) && tenantOptions.length > 1;
    let table: JSX.Element = (
      <h3>Click "Add Metric" to add a metric to the custom chart.</h3>
    );

    if (!isEmpty(metrics)) {
      table = (
        <table className="metric-table">
          <thead>
            <tr>
              <td className="metric-table__header">Metric Name</td>
              <td className="metric-table__header">Downsampler</td>
              <td className="metric-table__header">Aggregator</td>
              <td className="metric-table__header">Rate</td>
              <td className="metric-table__header">Source</td>
              <td className="metric-table__header">Per Node/Store</td>
              {canViewTenantOptions && (
                <td className="metric-table__header">Virtual Cluster</td>
              )}
              {canViewTenantOptions && (
                <td className="metric-table__header">Per Virtual Cluster</td>
              )}
              <td className="metric-table__header"></td>
            </tr>
          </thead>
          <tbody>
            {metrics.map((row, i) => (
              <CustomMetricRow
                key={i}
                metricOptions={this.props.metricOptions}
                nodeOptions={this.props.nodeOptions}
                tenantOptions={tenantOptions}
                canViewTenantOptions={canViewTenantOptions}
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
