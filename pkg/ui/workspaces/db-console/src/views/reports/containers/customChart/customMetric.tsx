// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { AxisUnits } from "@cockroachlabs/cluster-ui";
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

export function CustomMetricRow({
  metricOptions,
  nodeOptions,
  tenantOptions,
  canViewTenantOptions,
  index,
  rowState,
  onChange,
  onDelete,
}: CustomMetricRowProps): React.ReactElement {
  const changeState = (newState: Partial<CustomMetricState>) => {
    onChange(index, { ...rowState, ...newState });
  };

  const {
    metric,
    downsampler,
    aggregator,
    derivative,
    nodeSource,
    perSource,
    tenantSource,
    perTenant,
  } = rowState;

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
            onChange={(opt: Option<string>) =>
              changeState({ metric: opt.value })
            }
            placeholder="Select a metric..."
            optionComponent={MetricOption}
            matchProp="label"
            ignoreCase={true}
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
            onChange={(opt: Option<string>) =>
              changeState({ downsampler: +opt.value })
            }
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
            onChange={(opt: Option<string>) =>
              changeState({ aggregator: +opt.value })
            }
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
            onChange={(opt: Option<string>) =>
              changeState({ derivative: +opt.value })
            }
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
            onChange={(opt: Option<string>) =>
              changeState({ nodeSource: opt.value })
            }
          />
        </div>
      </td>
      <td className="metric-table__cell">
        <input
          type="checkbox"
          checked={perSource}
          onChange={(e: React.FormEvent<HTMLInputElement>) =>
            changeState({ perSource: e.currentTarget.checked })
          }
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
              onChange={(opt: Option<string>) =>
                changeState({ tenantSource: opt.value })
              }
            />
          </div>
        </td>
      )}
      {canViewTenantOptions && (
        <td className="metric-table__cell">
          <input
            type="checkbox"
            checked={perTenant}
            onChange={(e: React.FormEvent<HTMLInputElement>) =>
              changeState({ perTenant: e.currentTarget.checked })
            }
          />
        </td>
      )}
      <td className="metric-table__cell">
        <button
          className="edit-button metric-edit-button"
          onClick={() => onDelete(index)}
        >
          Remove Metric
        </button>
      </td>
    </tr>
  );
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

export function CustomChartTable({
  metricOptions,
  nodeOptions,
  tenantOptions,
  currentTenant,
  index,
  chartState,
  onChange,
  onDelete,
}: CustomChartTableProps): React.ReactElement {
  const metrics = chartState.metrics;
  const axisUnits = chartState.axisUnits;
  const canViewTenantOptions =
    isSystemTenant(currentTenant) && tenantOptions.length > 1;

  const addMetric = () => {
    onChange(index, {
      metrics: [...metrics, new CustomMetricState()],
      axisUnits,
    });
  };

  const updateMetricRow = (i: number, newState: CustomMetricState) => {
    const updated = metrics.slice();
    updated[i] = newState;
    onChange(index, { metrics: updated, axisUnits });
  };

  const removeMetric = (i: number) => {
    onChange(index, {
      metrics: metrics.slice(0, i).concat(metrics.slice(i + 1)),
      axisUnits,
    });
  };

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
              metricOptions={metricOptions}
              nodeOptions={nodeOptions}
              tenantOptions={tenantOptions}
              canViewTenantOptions={canViewTenantOptions}
              index={i}
              rowState={row}
              onChange={updateMetricRow}
              onDelete={removeMetric}
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
          selected={axisUnits.toString()}
          options={axisUnitsOptions}
          onChange={(selected: DropdownOption) =>
            onChange(index, { metrics, axisUnits: +selected.value })
          }
        />
        <button
          className="edit-button chart-edit-button chart-edit-button--remove"
          onClick={() => onDelete(index)}
        >
          Remove Chart
        </button>
      </div>
      {table}
      <button
        className="edit-button metric-edit-button metric-edit-button--add"
        onClick={addMetric}
      >
        Add Metric
      </button>
    </div>
  );
}
