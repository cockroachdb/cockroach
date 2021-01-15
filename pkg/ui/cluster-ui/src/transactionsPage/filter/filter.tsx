import React from "react";
import Select from "react-select";
import { Button } from "../../button";
import { CaretDown } from "@cockroachlabs/icons";
import { Filters } from "../../transactionsPage";
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
} from "./filterClasses";

interface TransactionsFilter {
  onSubmitFilters: (filters: Filters) => void;
  smth?: string;
  appNames: SelectOptions[];
  activeFilters: number;
  filters: Filters;
}
interface FilterState {
  hide: boolean;
  filters: Filters;
}

export interface SelectOptions {
  label: string;
  value: string;
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

export class Filter extends React.Component<TransactionsFilter, FilterState> {
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
  componentDidUpdate(prevProps: TransactionsFilter) {
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
    const { appNames, activeFilters } = this.props;
    const dropdownArea = hide ? hidden : dropdown;
    // TODO replace all onChange actions in Selects and Checkboxes with one onSubmit in <form />

    return (
      <div onClick={this.outsideClick} ref={this.dropdownRef}>
        <div className={dropdownButton} onClick={this.toggleFilters}>
          Filters ({activeFilters})&nbsp;
          <CaretDown className={caretDown} />
        </div>
        <div className={dropdownArea}>
          <div className={dropdownContentWrapper}>
            <div className={filterLabel.app}>App</div>
            <Select
              options={appNames}
              onChange={e => this.handleChange(e, "app")}
              value={filters.app}
              placeholder="All"
              {...defaultSelectProps}
            />
            <div className={filterLabel.query}>
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
