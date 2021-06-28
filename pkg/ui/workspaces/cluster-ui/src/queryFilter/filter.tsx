// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import React from "react";
import Select from "react-select";
import { Button } from "../button";
import { CaretDown } from "@cockroachlabs/icons";
import { Input } from "antd";
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
} from "./filterClasses";
import { MultiSelectCheckbox } from "../multiSelectCheckbox/multiSelectCheckbox";

interface QueryFilter {
  onSubmitFilters: (filters: Filters) => void;
  smth?: string;
  appNames: SelectOptions[];
  activeFilters: number;
  filters: Filters;
  dbNames?: string[];
  regions?: string[];
  nodes?: string[];
  showDB?: boolean;
  showSqlType?: boolean;
  showScan?: boolean;
  showRegions?: boolean;
  showNodes?: boolean;
}
interface FilterState {
  hide: boolean;
  filters: Filters;
}

export interface SelectOptions {
  label: string;
  value: string;
}

export interface Filters {
  app?: string;
  timeNumber?: string;
  timeUnit?: string;
  database?: string;
  sqlType?: string;
  fullScan?: boolean;
  distributed?: boolean;
  regions?: string;
  nodes?: string;
}

const timeUnit = [
  { label: "seconds", value: "seconds" },
  { label: "milliseconds", value: "milliseconds" },
];

export const defaultFilters: Filters = {
  app: "All",
  timeNumber: "0",
  timeUnit: "seconds",
  fullScan: false,
  sqlType: "",
  database: "",
  regions: "",
  nodes: "",
};

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
export const getFiltersFromQueryString = (queryString: string) => {
  const searchParams = new URLSearchParams(queryString);

  return Object.keys(defaultFilters).reduce(
    (filters, filter: keyof Filters): Filters => {
      const defaultValue = defaultFilters[filter];
      const queryStringFilter = searchParams.get(filter);
      const filterValue =
        queryStringFilter === null
          ? defaultValue
          : defaultValue.constructor(searchParams.get(filter));
      return { [filter]: filterValue, ...filters };
    },
    {},
  );
};

/**
 * The State of the filter that is consider inactive.
 * It's different from defaultFilters because we don't want to take
 * timeUnit into consideration.
 * For example, if the timeUnit changes, but the timeValue is still 0,
 * we want to consider 0 active Filters
 */
export const inactiveFiltersState: Filters = {
  app: "All",
  timeNumber: "0",
  fullScan: false,
  sqlType: "",
  database: "",
  regions: "",
  nodes: "",
};

export const calculateActiveFilters = (filters: Filters) => {
  return Object.keys(inactiveFiltersState).reduce(
    (active, filter: keyof Filters) => {
      return inactiveFiltersState[filter] !== filters[filter]
        ? (active += 1)
        : active;
    },
    0,
  );
};

export const getTimeValueInSeconds = (filters: Filters): number | "empty" => {
  if (filters.timeNumber === "0") return "empty";
  return filters.timeUnit === "seconds"
    ? Number(filters.timeNumber)
    : Number(filters.timeNumber) / 1000;
};

export class Filter extends React.Component<QueryFilter, FilterState> {
  state: FilterState = {
    hide: true,
    filters: {
      ...this.props.filters,
    },
  };

  dropdownRef: React.RefObject<HTMLDivElement> = React.createRef();

  componentDidMount() {
    window.addEventListener("click", this.outsideClick, false);
  }
  componentWillUnmount() {
    window.removeEventListener("click", this.outsideClick, false);
  }
  componentDidUpdate(prevProps: QueryFilter) {
    if (prevProps.filters !== this.props.filters) {
      this.setState({
        filters: {
          ...this.props.filters,
        },
      });
    }
  }
  outsideClick = (event: any) => {
    this.setState({ hide: true });
  };

  insideClick = (event: any) => {
    event.stopPropagation();
  };

  toggleFilters = () => {
    this.setState({
      hide: !this.state.hide,
    });
  };

  handleSubmit = () => {
    this.props.onSubmitFilters(this.state.filters);
    this.setState({ hide: true });
  };

  handleChange = (event: any, field: string) => {
    this.setState({
      filters: {
        ...this.state.filters,
        [field]:
          event.value ||
          event.target.checked ||
          this.validateInput(event.target.value),
      },
    });
  };

  toggleFullScan = (event: any) => {
    this.setState({
      filters: {
        ...this.state.filters,
        fullScan: event.target.checked,
      },
    });
  };

  validateInput = (value: string) => {
    const isInteger = /^[0-9]+$/;
    return (value === "" || isInteger.test(value)) && value.length <= 3
      ? value
      : this.state.filters.timeNumber;
  };

  clearInput = () => {
    this.setState({
      filters: {
        ...this.state.filters,
        timeNumber: "",
      },
    });
  };

  isOptionSelected = (option: string, field: string) => {
    const selection = field.split(",");
    if (selection.length > 0 && selection.includes(option)) return true;
    return false;
  };

  render() {
    const { hide, filters } = this.state;
    const {
      appNames,
      dbNames,
      regions,
      nodes,
      activeFilters,
      showDB,
      showSqlType,
      showScan,
      showRegions,
      showNodes,
    } = this.props;
    const dropdownArea = hide ? hidden : dropdown;
    const customStyles = {
      container: (provided: any) => ({
        ...provided,
        border: "none",
      }),
      option: (provided: any, state: any) => ({
        ...provided,
        backgroundColor: state.isSelected
          ? "#DEEBFF"
          : provided.backgroundColor,
        color: "#394455",
      }),
      control: (provided: any) => ({
        ...provided,
        width: "100%",
      }),
      singleValue: (provided: any) => ({
        ...provided,
        color: "hsl(0, 0%, 50%)",
      }),
    };
    const customStylesSmall = { ...customStyles };
    customStylesSmall.container = (provided: any) => ({
      ...provided,
      width: "141px",
      border: "none",
    });

    const databasesOptions = showDB
      ? dbNames.map(db => ({
          label: db,
          value: db,
          isSelected: this.isOptionSelected(db, filters.database),
        }))
      : [];
    const databaseValue = databasesOptions.filter(option => {
      return filters.database.split(",").includes(option.label);
    });
    const dbFilter = (
      <div>
        <div className={filterLabel.margin}>Database</div>
        <MultiSelectCheckbox
          options={databasesOptions}
          placeholder="All"
          field="database"
          parent={this}
          value={databaseValue}
        />
      </div>
    );

    const regionsOptions = showRegions
      ? regions.map(region => ({
          label: region,
          value: region,
          isSelected: this.isOptionSelected(region, filters.regions),
        }))
      : [];
    const regionsValue = regionsOptions.filter(option =>
      filters.regions.split(",").includes(option.label),
    );
    const regionsFilter = (
      <div>
        <div className={filterLabel.margin}>Region</div>
        <MultiSelectCheckbox
          options={regionsOptions}
          placeholder="All"
          field="regions"
          parent={this}
          value={regionsValue}
        />
      </div>
    );

    const nodesOptions = showNodes
      ? nodes.map(node => ({
          label: node,
          value: node,
          isSelected: this.isOptionSelected(node, filters.nodes),
        }))
      : [];
    const nodesValue = nodesOptions.filter(option => {
      return filters.nodes.split(",").includes(option.label);
    });
    const nodesFilter = (
      <div>
        <div className={filterLabel.margin}>Node</div>
        <MultiSelectCheckbox
          options={nodesOptions}
          placeholder="All"
          field="nodes"
          parent={this}
          value={nodesValue}
        />
      </div>
    );

    const sqlTypes = [
      {
        label: "DDL",
        value: "TypeDDL",
        isSelected: this.isOptionSelected("DDL", filters.sqlType),
      },
      {
        label: "DML",
        value: "TypeDML",
        isSelected: this.isOptionSelected("DML", filters.sqlType),
      },
      {
        label: "DCL",
        value: "TypeDCL",
        isSelected: this.isOptionSelected("DCL", filters.sqlType),
      },
      {
        label: "TCL",
        value: "TypeTCL",
        isSelected: this.isOptionSelected("TCL", filters.sqlType),
      },
    ];

    const sqlTypeValue = sqlTypes.filter(option => {
      return filters.sqlType.split(",").includes(option.label);
    });
    const sqlTypeFilter = (
      <div>
        <div className={filterLabel.margin}>Statement Type</div>
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
          Only show statements that contain queries with full table scans
        </label>
      </div>
    );
    // TODO replace all onChange actions in Selects and Checkboxes with one onSubmit in <form />

    // Some app names could be empty strings, so we're adding " " to those names,
    // this way it's easier for the user to recognize the blank name.
    const apps = appNames.map(app => {
      const label =
        app.label.trim().length === 0 ? '"' + app.label + '"' : app.label;
      return { label: label, value: app.value };
    });

    return (
      <div onClick={this.insideClick} ref={this.dropdownRef}>
        <div className={dropdownButton} onClick={this.toggleFilters}>
          Filters ({activeFilters})&nbsp;
          <CaretDown className={caretDown} />
        </div>
        <div className={dropdownArea}>
          <div className={dropdownContentWrapper}>
            <div className={filterLabel.top}>App</div>
            <Select
              options={apps}
              onChange={e => this.handleChange(e, "app")}
              value={apps.filter(app => app.value === filters.app)}
              placeholder="All"
              styles={customStyles}
            />
            {showDB ? dbFilter : ""}
            {showSqlType ? sqlTypeFilter : ""}
            {showRegions ? regionsFilter : ""}
            {showNodes ? nodesFilter : ""}
            <div className={filterLabel.margin}>
              Query fingerprint runs longer than
            </div>
            <section className={timePair.wrapper}>
              <Input
                value={filters.timeNumber}
                onChange={e => this.handleChange(e, "timeNumber")}
                onFocus={this.clearInput}
                className={timePair.timeNumber}
              />
              <Select
                options={timeUnit}
                value={timeUnit.filter(unit => unit.label == filters.timeUnit)}
                onChange={e => this.handleChange(e, "timeUnit")}
                className={timePair.timeUnit}
                styles={customStylesSmall}
              />
            </section>
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
