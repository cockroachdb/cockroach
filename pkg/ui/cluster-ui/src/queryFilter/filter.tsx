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
  dropdownSelect,
  checkbox,
} from "./filterClasses";

interface QueryFilter {
  onSubmitFilters: (filters: Filters) => void;
  smth?: string;
  appNames: SelectOptions[];
  activeFilters: number;
  filters: Filters;
  showScan?: boolean;
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
  fullScan?: boolean;
  distributed?: boolean;
}

const timeUnit = [
  { label: "seconds", value: "seconds" },
  { label: "milliseconds", value: "milliseconds" },
];

const defaultSelectProps = {
  className: dropdownSelect,
  searchable: false,
  clearable: false,
  arrowRenderer: () => <CaretDown className={caretDown} />,
};

export const defaultFilters: Filters = {
  app: "All",
  timeNumber: "0",
  timeUnit: "seconds",
  fullScan: false,
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
    document.addEventListener("click", this.outsideClick, false);
  }
  componentWillUnmount() {
    document.removeEventListener("click", this.outsideClick, false);
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
    if (this.dropdownRef.current.contains(event.target)) {
      return;
    }
    this.setState({ hide: true });
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

  render() {
    const { hide, filters } = this.state;
    const { appNames, activeFilters, showScan } = this.props;
    const dropdownArea = hide ? hidden : dropdown;
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

    return (
      <div onClick={this.outsideClick} ref={this.dropdownRef}>
        <div className={dropdownButton} onClick={this.toggleFilters}>
          Filters ({activeFilters})&nbsp;
          <CaretDown className={caretDown} />
        </div>
        <div className={dropdownArea}>
          <div className={dropdownContentWrapper}>
            <div className={filterLabel.top}>App</div>
            <Select
              options={appNames}
              onChange={e => this.handleChange(e, "app")}
              value={filters.app}
              placeholder="All"
              {...defaultSelectProps}
            />
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
                value={filters.timeUnit}
                onChange={e => this.handleChange(e, "timeUnit")}
                className={timePair.timeUnit}
                {...defaultSelectProps}
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
