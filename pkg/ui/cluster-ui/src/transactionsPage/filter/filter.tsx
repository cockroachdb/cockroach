// Copyright 2018 The Cockroach Authors.
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
    window.addEventListener("click", this.outsideClick, false);
  }
  componentWillUnmount() {
    window.removeEventListener("click", this.outsideClick, false);
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

    return (
      <div onClick={this.insideClick} ref={this.dropdownRef}>
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
              value={appNames.filter(app => app.label === filters.app)}
              placeholder="All"
              styles={customStyles}
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
                value={timeUnit.filter(unit => unit.label === filters.timeUnit)}
                onChange={e => this.handleChange(e, "timeUnit")}
                className={timePair.timeUnit}
                styles={customStylesSmall}
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
