// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { Checkbox, Select } from "antd";
import classNames from "classnames";
import React, { useEffect, useRef, useState } from "react";

import { OutsideEventHandler } from "src/components/outsideEventHandler";
import Dropdown, { arrowRenderer } from "src/views/shared/components/dropdown";

import { NetworkFilter, NetworkSort } from "..";
import "./filter.scss";

interface IFilterProps {
  onChangeFilter: (key: string, value: string) => void;
  deselectFilterByKey: (key: string) => void;
  sort: NetworkSort[];
  filter: NetworkFilter;
  dropDownClassName?: string;
}

function firstLetterToUpperCase(value: string) {
  return value.replace(/^[a-z]/, m => m.toUpperCase());
}

export function Filter({
  onChangeFilter,
  deselectFilterByKey,
  sort,
  filter,
  dropDownClassName,
}: IFilterProps): React.ReactElement {
  const [opened, setOpened] = useState(false);
  const [width, setWidth] = useState(window.innerWidth);
  const rangeContainer = useRef<HTMLDivElement>(null);

  useEffect(() => {
    const updateDimensions = () => setWidth(window.innerWidth);
    window.addEventListener("resize", updateDimensions);
    return () => window.removeEventListener("resize", updateDimensions);
  }, []);

  const renderSelectValue = (id: string) => {
    if (filter && filter[id]) {
      const value = (key: string) =>
        `${filter[id].length} ${firstLetterToUpperCase(key)} Selected`;
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

  const renderSelect = () => {
    return sort.map(value => (
      <div style={{ width: "100%" }} className="select__container">
        <p className="filter--label">{`${
          value.id === "cluster" ? "Nodes" : firstLetterToUpperCase(value.id)
        }`}</p>
        <Select
          style={{ width: "100%" }}
          placeholder={`Filter ${
            value.id === "cluster" ? "node" : value.id
          }(s)`}
          value={renderSelectValue(value.id)}
          dropdownRender={() => (
            <div onMouseDown={e => e.preventDefault()}>
              <div className="select-selection__deselect">
                <a onClick={() => deselectFilterByKey(value.id)}>
                  Deselect all
                </a>
              </div>
              {value.filters.map(val => {
                const checked =
                  filter &&
                  filter[value.id] &&
                  filter[value.id].indexOf(val.name) !== -1;
                return (
                  <div className="filter__checkbox">
                    <Checkbox
                      checked={checked}
                      onChange={() => onChangeFilter(value.id, val.name)}
                    />
                    <a
                      className={`filter__checkbox--label ${
                        checked ? "filter__checkbox--label__active" : ""
                      }`}
                      onClick={() => onChangeFilter(value.id, val.name)}
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

  const containerLeft = rangeContainer.current
    ? rangeContainer.current.getBoundingClientRect().left
    : 0;
  const left = width >= containerLeft + 240 ? 0 : width - (containerLeft + 240);
  return (
    <div className="Filter-latency">
      <OutsideEventHandler onOutsideClick={() => setOpened(false)}>
        <Dropdown
          title="Filter"
          options={[]}
          selected=""
          className={classNames(
            {
              dropdown__focused: opened,
            },
            dropDownClassName,
          )}
          onDropdownClick={() => setOpened(prev => !prev)}
          content={
            <div ref={rangeContainer} className="Range">
              <div
                className="click-zone"
                onClick={() => setOpened(prev => !prev)}
              />
              {opened && (
                <div
                  className="trigger-container"
                  onClick={() => setOpened(false)}
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
                  <div
                    className="multiple-filter__selection"
                    style={{ left }}
                    onClick={e => e.stopPropagation()}
                  >
                    {renderSelect()}
                  </div>
                )}
              </div>
            </div>
          }
        />
      </OutsideEventHandler>
    </div>
  );
}
