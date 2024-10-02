// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { CaretDown, Cancel } from "@cockroachlabs/icons";
import { Input } from "antd";
import { History } from "history";
import isEqual from "lodash/isEqual";
import React from "react";
import Select from "react-select";

import { Button } from "../button";
import { selectCustomStyles } from "../common";
import { MultiSelectCheckbox } from "../multiSelectCheckbox/multiSelectCheckbox";
import { syncHistory } from "../util";

import {
  dropdownButton,
  dropdownContentWrapper,
  timePair,
  filterLabel,
  applyBtn,
  dropdown,
  hidden,
  caretDown,
  checkbox,
  badge,
  clearBnt,
} from "./filterClasses";

interface QueryFilter {
  onSubmitFilters: (filters: Filters) => void;
  smth?: string;
  appNames?: string[];
  activeFilters: number;
  filters: Filters;
  dbNames?: string[];
  usernames?: string[];
  sessionStatuses?: string[];
  executionStatuses?: string[];
  schemaInsightTypes?: string[];
  workloadInsightTypes?: string[];
  regions?: string[];
  nodes?: string[];
  hideAppNames?: boolean;
  hideTimeLabel?: boolean;
  showDB?: boolean;
  showUsername?: boolean;
  showSessionStatus?: boolean;
  showExecutionStatus?: boolean;
  showSchemaInsightTypes?: boolean;
  showWorkloadInsightTypes?: boolean;
  showSqlType?: boolean;
  showScan?: boolean;
  showRegions?: boolean;
  showNodes?: boolean;
  timeLabel?: string;
}
interface FilterState {
  hide: boolean;
  filters: Filters;
}

export interface Filters extends Record<string, string | boolean> {
  app?: string;
  timeNumber?: string;
  timeUnit?: string;
  database?: string;
  sqlType?: string;
  fullScan?: boolean;
  regions?: string;
  nodes?: string;
  username?: string;
  sessionStatus?: string;
  executionStatus?: string;
  schemaInsightType?: string;
  workloadInsightType?: string;
}

const timeUnit = [
  { label: "seconds", value: "seconds" },
  { label: "milliseconds", value: "milliseconds" },
  { label: "minutes", value: "minutes" },
];

export const defaultFilters: Required<Filters> = {
  app: "",
  timeNumber: "0",
  timeUnit: "seconds",
  fullScan: false,
  sqlType: "",
  database: "",
  regions: "",
  nodes: "",
  username: "",
  sessionStatus: "",
  schemaInsightType: "",
  workloadInsightType: "",
  executionStatus: "",
};

// getFullFiltersObject returns Filters with every field defined as
// either what is specified in partialFilters, or 'null' if unset in
// partialFilters.
export function getFullFiltersAsStringRecord(
  partialFilters: Partial<Filters>,
): Record<string, string | null> {
  const filters: Record<string, string> = {};
  Object.keys(defaultFilters).forEach(filterKey => {
    if (
      filterKey in partialFilters &&
      partialFilters[filterKey] !== inactiveFiltersState[filterKey]
    ) {
      filters[filterKey] = partialFilters[filterKey]?.toString();
      return;
    }
    filters[filterKey] = null;
  });
  return filters;
}

/**
 * For each key on the defaultFilters, check if there is a new value
 * for it on searchParams. If the value is null, returns the default value
 * for that key. If it isn't we use the constructor of the value of
 * the default filter (the function used to create it, e.g. String, Boolean)
 * to cast the string from the searchParams into the same type as the default
 * @param queryString: search param from props.history.location.search
 * @return Filters: the default filters with updated keys existing on
 * queryString
 */
export const getFiltersFromQueryString = (
  queryString: string,
): Record<string, string> => {
  const searchParams = new URLSearchParams(queryString);

  return Object.keys(defaultFilters).reduce((filters, filter): Filters => {
    const defaultValue = defaultFilters[filter];
    const queryStringFilter = searchParams.get(filter);
    const filterValue =
      queryStringFilter == null
        ? defaultValue // If this filter doesn't exist on query string, use default value.
        : typeof defaultValue == "boolean"
          ? searchParams.get(filter) === "true" // If it's a Boolean, convert from String to Boolean;
          : defaultValue.constructor(searchParams.get(filter)); // Otherwise, use the constructor for that class.
    // Boolean is converted without using its own constructor because the value from the query
    // params is a string and Boolean('false') = true, which would be incorrect.
    return { [filter]: filterValue, ...filters };
  }, {});
};

/**
 * Get Filters from Query String and if its value is different from the current
 * filters value, it calls the onFilterChange function.
 * @param history History
 * @param filters the current active filters
 * @param onFilterChange function to be called if the values from the search
 * params are different from the current ones. This function can update
 * the value stored on localStorage for example
 * @returns Filters the active filters
 */
export const handleFiltersFromQueryString = (
  history: History,
  filters: Filters,
  onFilterChange: (value: Filters) => void,
): Filters => {
  const filtersQueryString = getFiltersFromQueryString(history.location.search);
  const searchParams = new URLSearchParams(history.location.search);
  let hasFilter = false;

  for (const key of Object.keys(defaultFilters)) {
    if (searchParams.get(key)) {
      hasFilter = true;
      break;
    }
  }

  if (onFilterChange && hasFilter && !isEqual(filtersQueryString, filters)) {
    // If we have filters on query string and they're different
    // from the current filter state on props (localStorage),
    // we want to update the value on localStorage.
    onFilterChange(filtersQueryString);
  } else if (!isEqual(filters, defaultFilters)) {
    // If the filters on props (localStorage) are different
    // from the default values, we want to update the History,
    // so the url can be easily shared with the filters selected.
    syncHistory(
      {
        app: filters.app,
        timeNumber: filters.timeNumber,
        timeUnit: filters.timeUnit,
        fullScan: filters.fullScan?.toString(),
        sqlType: filters.sqlType,
        database: filters.database,
        regions: filters.regions,
        nodes: filters.nodes,
      },
      history,
    );
  }
  // If we have a new filter selection on query params, they
  // take precedent on what is stored on localStorage.
  return hasFilter ? filtersQueryString : filters;
};

/**
 * Update the query params to the current values of the Filter.
 * When we change tabs inside the SQL Activity page for example,
 * the constructor is called only on the first time.
 * The component update event is called frequently and can be used to
 * update the query params by using this function that only updates
 * the query params if the values did change and we're on the correct tab.
 * @param tab which the query params should update
 * @param filters the current filters
 * @param history
 */
export const updateFiltersQueryParamsOnTab = (
  tab: string,
  filters: Filters,
  history: History,
): void => {
  const filtersQueryString = getFiltersFromQueryString(history.location.search);
  const searchParams = new URLSearchParams(history.location.search);
  const currentTab = searchParams.get("tab") || "";
  if (
    currentTab === tab &&
    !isEqual(filters, defaultFilters) &&
    !isEqual(filters, filtersQueryString)
  ) {
    syncHistory(
      {
        app: filters.app,
        timeNumber: filters.timeNumber,
        timeUnit: filters.timeUnit,
        fullScan: filters.fullScan?.toString(),
        sqlType: filters.sqlType,
        database: filters.database,
        regions: filters.regions,
        nodes: filters.nodes,
      },
      history,
    );
  }
};

/**
 * The State of the filter that is considered inactive.
 * It's different from defaultFilters because we don't want to take
 * timeUnit into consideration.
 * For example, if the timeUnit changes, but the timeValue is still 0,
 * we want to consider 0 active Filters
 */
export const inactiveFiltersState: Required<Omit<Filters, "timeUnit">> = {
  app: "",
  timeNumber: "0",
  fullScan: false,
  sqlType: "",
  database: "",
  regions: "",
  sessionStatus: "",
  nodes: "",
  workloadInsightType: "",
  schemaInsightType: "",
  executionStatus: "",
  username: "",
};

const getActiveFilters = (filters: Filters): string[] => {
  return Object.keys(inactiveFiltersState).filter(
    filter =>
      filters[filter] != null &&
      inactiveFiltersState[filter] !== filters[filter],
  );
};

export const calculateActiveFilters = (filters: Filters): number => {
  return getActiveFilters(filters).length;
};

export const getTimeValueInSeconds = (filters: Filters): number | "empty" => {
  if (filters.timeNumber === "0" || filters.timeNumber == null) return "empty";
  switch (filters.timeUnit) {
    case "seconds":
      return Number(filters.timeNumber);
    case "minutes":
      return Number(filters.timeNumber) * 60;
    default:
      // Milliseconds
      return Number(filters.timeNumber) / 1000;
  }
};

export class Filter extends React.Component<QueryFilter, FilterState> {
  state: FilterState = {
    hide: true,
    filters: {
      ...this.props.filters,
    },
  };

  dropdownRef: React.RefObject<HTMLDivElement> = React.createRef();

  componentDidMount(): void {
    window.addEventListener("click", this.outsideClick, false);
  }
  componentWillUnmount(): void {
    window.removeEventListener("click", this.outsideClick, false);
  }
  componentDidUpdate(prevProps: QueryFilter): void {
    if (prevProps.filters !== this.props.filters) {
      this.setState({
        filters: {
          ...this.props.filters,
        },
      });
    }
  }
  outsideClick = (): void => {
    this.setState({ hide: true });
  };

  insideClick = (event: React.MouseEvent<HTMLDivElement>): void => {
    event.stopPropagation();
  };

  toggleFilters = (): void => {
    this.setState({
      hide: !this.state.hide,
    });
  };

  handleSubmit = (): void => {
    this.props.onSubmitFilters(this.state.filters);
    this.setState({ hide: true });
  };

  handleSelectChange = (
    event: { label: string; value: string },
    field: string,
  ): void => {
    this.setState({
      filters: {
        ...this.state.filters,
        [field]: event.value,
      },
    });
  };

  handleChange = (
    event: React.ChangeEvent<HTMLInputElement>,
    field: string,
  ): void => {
    this.setState({
      filters: {
        ...this.state.filters,
        [field]: event.target.checked || this.validateInput(event.target.value),
      },
    });
  };

  toggleFullScan = (event: React.ChangeEvent<HTMLInputElement>): void => {
    this.setState({
      filters: {
        ...this.state.filters,
        fullScan: event.target.checked,
      },
    });
  };

  validateInput = (value: string): string => {
    const isInteger = /^[0-9]+$/;
    return (value === "" || isInteger.test(value)) && value.length <= 3
      ? value
      : this.state.filters.timeNumber ?? "";
  };

  clearInput = (): void => {
    this.setState({
      filters: {
        ...this.state.filters,
        timeNumber: "",
      },
    });
  };

  isOptionSelected = (option: string, field: string): boolean => {
    const selection = field?.split(",");
    return selection?.length > 0 && selection?.includes(option);
  };

  render(): React.ReactElement {
    const { hide, filters } = this.state;
    const {
      appNames,
      dbNames,
      usernames,
      sessionStatuses,
      executionStatuses,
      schemaInsightTypes,
      workloadInsightTypes,
      regions,
      nodes,
      activeFilters,
      hideAppNames,
      showDB,
      showSqlType,
      showScan,
      showRegions,
      showNodes,
      timeLabel,
      hideTimeLabel,
      showUsername,
      showSessionStatus,
      showExecutionStatus,
      showSchemaInsightTypes,
      showWorkloadInsightTypes,
    } = this.props;
    const dropdownArea = hide ? hidden : dropdown;
    const customStylesSmall = { ...selectCustomStyles };
    customStylesSmall.container = (provided: any) => ({
      ...provided,
      width: "141px",
      border: "none",
    });

    const appFilters = filters.app ?? "";
    const appsOptions =
      !hideAppNames && appNames
        ? appNames.map(app => ({
            label: app,
            value: app,
            isSelected: this.isOptionSelected(app, appFilters),
          }))
        : [];

    const selectedApps = appFilters.split(",");
    const appValue = appsOptions.filter(option => {
      return selectedApps.includes(option.label);
    });
    const appFilter = (
      <div>
        <div className={filterLabel.margin}>{getLabelFromKey("app")}</div>
        <MultiSelectCheckbox
          options={appsOptions}
          placeholder="All"
          field="app"
          parent={this}
          value={appValue}
        />
      </div>
    );

    const databaseFilters = filters.database ?? "";
    const databasesOptions =
      showDB && dbNames
        ? dbNames.map(db => ({
            label: db,
            value: db,
            isSelected: this.isOptionSelected(db, databaseFilters),
          }))
        : [];

    const selectedDatabases = databaseFilters.split(",");
    const databaseValue = databasesOptions.filter(option => {
      return selectedDatabases.includes(option.label);
    });
    const dbFilter = (
      <div>
        <div className={filterLabel.margin}>{getLabelFromKey("database")}</div>
        <MultiSelectCheckbox
          options={databasesOptions}
          placeholder="All"
          field="database"
          parent={this}
          value={databaseValue}
        />
      </div>
    );

    const usernameFilters = filters.username ?? "";
    const usernameOptions =
      showUsername && usernames
        ? usernames.map(username => ({
            label: username,
            value: username,
            isSelected: this.isOptionSelected(username, usernameFilters),
          }))
        : [];

    const selectedUsernames = usernameFilters.split(",");
    const usernameValue = usernameOptions.filter(option => {
      return selectedUsernames.includes(option.label);
    });
    const usernameFilter = (
      <div>
        <div className={filterLabel.margin}>{getLabelFromKey("username")}</div>
        <MultiSelectCheckbox
          options={usernameOptions}
          placeholder="All"
          field="username"
          parent={this}
          value={usernameValue}
        />
      </div>
    );

    const sessionStatusFilters = filters.sessionStatus ?? "";
    const sessionStatusOptions =
      showSessionStatus && sessionStatuses
        ? sessionStatuses.map(sessionStatus => ({
            label: sessionStatus,
            value: sessionStatus,
            isSelected: this.isOptionSelected(
              sessionStatus,
              sessionStatusFilters,
            ),
          }))
        : [];

    const selectedSessionStatuses = sessionStatusFilters.split(",");
    const sessionStatusValue = sessionStatusOptions.filter(option => {
      return selectedSessionStatuses.includes(option.label);
    });
    const sessionStatusFilter = (
      <div>
        <div className={filterLabel.margin}>
          {getLabelFromKey("sessionStatus")}
        </div>
        <MultiSelectCheckbox
          options={sessionStatusOptions}
          placeholder="All"
          field="sessionStatus"
          parent={this}
          value={sessionStatusValue}
        />
      </div>
    );

    const executionStatusFilters = filters.executionStatus ?? "";
    const executionStatusOptions =
      showExecutionStatus && executionStatuses
        ? executionStatuses.map(executionStatus => ({
            label: executionStatus,
            value: executionStatus,
            isSelected: this.isOptionSelected(
              executionStatus,
              executionStatusFilters,
            ),
          }))
        : [];

    const selectedExecutionStatuses = executionStatusFilters.split(",");
    const executionStatusValue = executionStatusOptions.filter(option =>
      selectedExecutionStatuses.includes(option.label),
    );
    const executionStatusFilter = (
      <div>
        <div className={filterLabel.margin}>
          {getLabelFromKey("executionStatus")}
        </div>
        <MultiSelectCheckbox
          options={executionStatusOptions}
          placeholder="All"
          field="executionStatus"
          parent={this}
          value={executionStatusValue}
        />
      </div>
    );

    const schemaInsightType = filters.schemaInsightType ?? "";
    const schemaInsightTypeOptions =
      showSchemaInsightTypes && schemaInsightTypes
        ? schemaInsightTypes.map(schemaInsight => ({
            label: schemaInsight,
            value: schemaInsight,
            isSelected: this.isOptionSelected(schemaInsight, schemaInsightType),
          }))
        : [];
    const selectedSchemaInsightTypes = schemaInsightType.split(",");
    const schemaInsightTypeValue = schemaInsightTypeOptions.filter(option =>
      selectedSchemaInsightTypes.includes(option.label),
    );
    const schemaInsightTypeFilter = (
      <div>
        <div className={filterLabel.margin}>
          {getLabelFromKey("schemaInsightType")}
        </div>
        <MultiSelectCheckbox
          options={schemaInsightTypeOptions}
          placeholder="All"
          field="schemaInsightType"
          parent={this}
          value={schemaInsightTypeValue}
        />
      </div>
    );

    const workloadInsightTypeFilters = filters.workloadInsightType ?? "";
    const workloadInsightTypeOptions =
      showWorkloadInsightTypes && workloadInsightTypes
        ? workloadInsightTypes.map(workloadInsight => ({
            label: workloadInsight,
            value: workloadInsight,
            isSelected: this.isOptionSelected(
              workloadInsight,
              workloadInsightTypeFilters,
            ),
          }))
        : [];

    const selectedWorkloadInsightTypes = workloadInsightTypeFilters.split(",");
    const workloadInsightTypeValue = workloadInsightTypeOptions.filter(
      option => {
        return selectedWorkloadInsightTypes.includes(option.label);
      },
    );
    const workloadInsightTypeFilter = (
      <div>
        <div className={filterLabel.margin}>
          {getLabelFromKey("workloadInsightType")}
        </div>
        <MultiSelectCheckbox
          options={workloadInsightTypeOptions}
          placeholder="All"
          field="workloadInsightType"
          parent={this}
          value={workloadInsightTypeValue}
        />
      </div>
    );

    const regionsFilters = filters.regions ?? "";
    const regionsOptions =
      showRegions && regions
        ? regions.map(region => ({
            label: region,
            value: region,
            isSelected: this.isOptionSelected(region, regionsFilters),
          }))
        : [];
    const selectedRegions = regionsFilters.split(",");
    const regionsValue = regionsOptions.filter(option =>
      selectedRegions.includes(option.label),
    );
    const regionsFilter = (
      <div>
        <div className={filterLabel.margin}>{getLabelFromKey("regions")}</div>
        <MultiSelectCheckbox
          options={regionsOptions}
          placeholder="All"
          field="regions"
          parent={this}
          value={regionsValue}
        />
      </div>
    );

    const nodeFilters = filters.nodes ?? "";
    const nodesOptions =
      showNodes && nodes
        ? nodes.map(node => ({
            label: node,
            value: node,
            isSelected: this.isOptionSelected(node, nodeFilters),
          }))
        : [];

    const selectedNodes = nodeFilters.split(",");
    const nodesValue = nodesOptions.filter(option => {
      return selectedNodes.includes(option.label);
    });
    const nodesFilter = (
      <div>
        <div className={filterLabel.margin}>{getLabelFromKey("nodes")}</div>
        <MultiSelectCheckbox
          options={nodesOptions}
          placeholder="All"
          field="nodes"
          parent={this}
          value={nodesValue}
        />
      </div>
    );

    const sqlTypeFilters = filters.sqlType ?? "";
    const sqlTypes = showSqlType
      ? [
          {
            label: "DDL",
            value: "TypeDDL",
            isSelected: this.isOptionSelected("DDL", sqlTypeFilters),
          },
          {
            label: "DML",
            value: "TypeDML",
            isSelected: this.isOptionSelected("DML", sqlTypeFilters),
          },
          {
            label: "DCL",
            value: "TypeDCL",
            isSelected: this.isOptionSelected("DCL", sqlTypeFilters),
          },
          {
            label: "TCL",
            value: "TypeTCL",
            isSelected: this.isOptionSelected("TCL", sqlTypeFilters),
          },
        ]
      : [];

    const selectedSqlTypes = sqlTypeFilters.split(",");
    const sqlTypeValue = sqlTypes?.filter(option => {
      return selectedSqlTypes.includes(option.label);
    });
    const sqlTypeFilter = (
      <div>
        <div className={filterLabel.margin}>{getLabelFromKey("sqlType")}</div>
        <MultiSelectCheckbox
          options={sqlTypes}
          placeholder="All"
          field="sqlType"
          parent={this}
          value={sqlTypeValue}
        />
      </div>
    );

    const fullScanFilter = (
      <div className={filterLabel.margin}>
        <input
          type="checkbox"
          id="full-table-scan-toggle"
          checked={filters.fullScan}
          onChange={e => this.toggleFullScan(e)}
          className={checkbox.input}
        />
        <label htmlFor="full-table-scan-toggle" className={checkbox.label}>
          Only show statements with full table scans
        </label>
      </div>
    );
    // TODO replace all onChange actions in Selects and Checkboxes with one onSubmit in <form />

    return (
      <div onClick={this.insideClick} ref={this.dropdownRef}>
        <div className={dropdownButton} onClick={this.toggleFilters}>
          Filters ({activeFilters})&nbsp;
          <CaretDown className={caretDown} />
        </div>
        <div className={dropdownArea}>
          <div className={dropdownContentWrapper}>
            {!hideAppNames ? appFilter : ""}
            {showDB ? dbFilter : ""}
            {showUsername ? usernameFilter : ""}
            {showSessionStatus ? sessionStatusFilter : ""}
            {showExecutionStatus ? executionStatusFilter : ""}
            {showSchemaInsightTypes ? schemaInsightTypeFilter : ""}
            {showWorkloadInsightTypes ? workloadInsightTypeFilter : ""}
            {showSqlType ? sqlTypeFilter : ""}
            {showRegions ? regionsFilter : ""}
            {showNodes ? nodesFilter : ""}
            {hideTimeLabel
              ? ""
              : filters.timeUnit && (
                  <>
                    <div className={filterLabel.margin}>
                      {timeLabel
                        ? `${timeLabel} runs longer than`
                        : "Statement fingerprint runs longer than"}
                    </div>
                    <section className={timePair.wrapper}>
                      <Input
                        value={filters.timeNumber}
                        onChange={(e: React.ChangeEvent<HTMLInputElement>) =>
                          this.handleChange(e, "timeNumber")
                        }
                        onFocus={this.clearInput}
                        className={timePair.timeNumber}
                      />
                      <Select
                        options={timeUnit}
                        value={timeUnit.filter(
                          unit => unit.label === filters.timeUnit,
                        )}
                        onChange={e => this.handleSelectChange(e, "timeUnit")}
                        className={timePair.timeUnit}
                        styles={customStylesSmall}
                      />
                    </section>
                  </>
                )}
            {showScan ? fullScanFilter : ""}
            <div className={applyBtn.wrapper}>
              <Button
                className={applyBtn.btn}
                textAlign="center"
                onClick={this.handleSubmit}
              >
                Apply
              </Button>
            </div>
          </div>
        </div>
      </div>
    );
  }
}

interface SelectedFilterProps {
  filters: Filters;
  onRemoveFilter: (filters: Filters) => void;
  onClearFilters: () => void;
  className?: string;
}
export function SelectedFilters(
  props: SelectedFilterProps,
): React.ReactElement {
  const { filters, onRemoveFilter, onClearFilters, className } = props;
  const activeFilters = getActiveFilters(filters);
  const badges = activeFilters.map(filter => {
    return (
      <FilterBadge
        filters={filters}
        name={filter}
        values={filters[filter]}
        unit={filters["timeUnit"] ?? ""}
        key={filter}
        onRemoveFilter={onRemoveFilter}
      />
    );
  });

  return (
    <div className={`${badge.area} ${className}`}>
      {badges}
      {activeFilters.length > 0 && (
        <Button
          onClick={() => onClearFilters()}
          type="flat"
          size="small"
          className={clearBnt.btn}
        >
          Clear filters
        </Button>
      )}
    </div>
  );
}

function removeFilter(
  filters: Filters,
  filter: string,
  onRemoveFilter: (filters: Filters) => void,
): void {
  filters[filter] = inactiveFiltersState[filter];
  onRemoveFilter({ ...filters });
}
interface FilterBadgeProps {
  filters: Filters;
  name: string;
  values: string | boolean;
  unit: string;
  onRemoveFilter: (filters: Filters) => void;
}
function FilterBadge(props: FilterBadgeProps): React.ReactElement {
  const { filters, name, values, onRemoveFilter } = props;
  const unit = name === "timeNumber" ? props.unit : "";
  let value = `${getLabelFromKey(name)}: ${values.toString()} ${unit}`;
  if (value.length > 100) {
    value = value.substring(0, 100) + "...";
  }
  return (
    <div className={badge.wrapper}>
      {value}
      <Cancel
        className={badge.closeArea}
        onClick={() => removeFilter({ ...filters }, name, onRemoveFilter)}
      />
    </div>
  );
}

function getLabelFromKey(key: string): string {
  switch (key) {
    case "app":
      return "Application Name";
    case "database":
      return "Database";
    case "executionStatus":
      return "Execution Status";
    case "fullScan":
      return "Full Scan";
    case "nodes":
      return "Node";
    case "regions":
      return "Region";
    case "schemaInsightType":
      return "Schema Insight Type";
    case "sessionStatus":
      return "Session Status";
    case "sqlType":
      return "Statement Type";
    case "timeNumber":
      return "Runs Longer Than";
    case "username":
      return "User Name";
    case "workloadInsightType":
      return "Workload Insight Type";
    default:
      return key;
  }
}
