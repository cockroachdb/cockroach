// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { Checkbox, Select } from "antd";
import Dropdown, { arrowRenderer } from "src/views/shared/components/dropdown";
import React from "react";
import classNames from "classnames";
import { NetworkFilter, NetworkSort } from "..";
import "./filter.styl";

interface IFilterProps {
  onChangeFilter: (key: string, value: string) => void;
  deselectFilterByKey: (key: string) => void;
  sort: NetworkSort[];
  filter: NetworkFilter;
  dropDownClassName?: string;
}

interface IFilterState {
  opened: boolean;
  width: number;
}

export class Filter extends React.Component<IFilterProps, IFilterState> {
  state = {
    opened: false,
    width: window.innerWidth,
  };

  private rangeContainer = React.createRef<HTMLDivElement>();

  componentDidMount() {
    window.addEventListener("resize", this.updateDimensions);
  }

  componentWillUnmount() {
    window.removeEventListener("resize", this.updateDimensions);
  }

  updateDimensions = () => {
    this.setState({
      width: window.innerWidth,
    });
  };

  onChange = (key: string, value: string) => () =>
    this.props.onChangeFilter(key, value);

  onDeselect = (key: string) => () => this.props.deselectFilterByKey(key);

  renderSelectValue = (id: string) => {
    const { filter } = this.props;

    if (filter && filter[id]) {
      const value = (key: string) =>
        `${filter[id].length} ${this.firstLetterToUpperCase(key)} Selected`;
      switch (true) {
        case filter[id].length === 1 && id === "cluster":
          return value("Node");
        case filter[id].length === 1:
          return value(id);
        case filter[id].length > 1 && id === "cluster":
          return value("Nodes");
        case filter[id].length > 1:
          return value(`${id}s`);
        default:
          return;
      }
    }
    return;
  };

  firstLetterToUpperCase = (value: string) =>
    value.replace(/^[a-z]/, (m) => m.toUpperCase());

  renderSelect = () => {
    const { sort, filter } = this.props;
    return sort.map((value) => (
      <div style={{ width: "100%" }} className="select__container">
        <p className="filter--label">{`${
          value.id === "cluster"
            ? "Nodes"
            : this.firstLetterToUpperCase(value.id)
        }`}</p>
        <Select
          style={{ width: "100%" }}
          placeholder={`Filter ${
            value.id === "cluster" ? "node" : value.id
          }(s)`}
          value={this.renderSelectValue(value.id)}
          dropdownRender={(_) => (
            <div onMouseDown={(e) => e.preventDefault()}>
              <div className="select-selection__deselect">
                <a onClick={this.onDeselect(value.id)}>Deselect all</a>
              </div>
              {value.filters.map((val) => {
                const checked =
                  filter &&
                  filter[value.id] &&
                  filter[value.id].indexOf(val.name) !== -1;
                return (
                  <div className="filter__checkbox">
                    <Checkbox
                      checked={checked}
                      onChange={this.onChange(value.id, val.name)}
                    />
                    <a
                      className={`filter__checkbox--label ${
                        checked ? "filter__checkbox--label__active" : ""
                      }`}
                      onClick={this.onChange(value.id, val.name)}
                    >{`${value.id === "cluster" ? "N" : ""}${val.name}: ${
                      val.address
                    }`}</a>
                  </div>
                );
              })}
            </div>
          )}
        />
      </div>
    ));
  };

  render() {
    const { opened, width } = this.state;
    const { dropDownClassName } = this.props;
    const containerLeft = this.rangeContainer.current
      ? this.rangeContainer.current.getBoundingClientRect().left
      : 0;
    const left =
      width >= containerLeft + 240 ? 0 : width - (containerLeft + 240);
    return (
      <div className="Filter-latency">
        <Dropdown
          title="Filter"
          options={[]}
          selected=""
          className={classNames(
            { dropdown__focused: opened },
            dropDownClassName,
          )}
          content={
            <div ref={this.rangeContainer} className="Range">
              <div
                className="click-zone"
                onClick={() => this.setState({ opened: !opened })}
              />
              {opened && (
                <div
                  className="trigger-container"
                  onClick={() => this.setState({ opened: false })}
                />
              )}
              <div className="trigger-wrapper">
                <div
                  className={`trigger Select ${(opened && "is-open") || ""}`}
                >
                  <div className="Select-control">
                    <div className="Select-arrow-zone">
                      {arrowRenderer({ isOpen: opened })}
                    </div>
                  </div>
                </div>
                {opened && (
                  <div className="multiple-filter__selection" style={{ left }}>
                    {this.renderSelect()}
                  </div>
                )}
              </div>
            </div>
          }
        />
      </div>
    );
  }
}
