// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import React from "react";
import { mount, ReactWrapper } from "enzyme";
import { createMemoryHistory, MemoryHistory } from "history";
import { Router } from "react-router";
import Select from "react-select";
import { Select as AntdSelect, Checkbox as AntdCheckbox } from "antd";
import "antd/lib/checkbox/style";
import "antd/lib/select/style";
import Dropdown from "src/views/shared/components/dropdown";

import Sort from "./index";
import { Filter } from "../filter";
import * as analytics from "src/util/analytics";

describe("<Sort> component", () => {
  let history: MemoryHistory = createMemoryHistory();
  let wrapper: ReactWrapper;
  const onChangeCollapseMock = jest.fn();
  const onChangeFilterMock = jest.fn();
  const deselectFilterByKeyMock = jest.fn();
  const trackNetworkSortSpy = jest
    .spyOn(analytics, "trackNetworkSort")
    .mockImplementation((_sortBy: string) => {});
  const historyListenerMock = jest.fn();

  beforeEach(() => {
    history = createMemoryHistory();
    history.listen((location, action) =>
      historyListenerMock(location.pathname, action),
    );
    onChangeCollapseMock.mockReset();
    onChangeFilterMock.mockReset();
    deselectFilterByKeyMock.mockReset();
    trackNetworkSortSpy.mockReset();
    historyListenerMock.mockReset();

    wrapper = mount(
      <Router history={history}>
        <Sort
          onChangeCollapse={onChangeCollapseMock}
          onChangeFilter={onChangeFilterMock}
          deselectFilterByKey={deselectFilterByKeyMock}
          collapsed={false}
          filter={{
            f1: ["1", "2", "3"],
            f2: ["4", "5", "6"],
          }}
          sort={[
            {
              id: "1",
              filters: [
                { name: "a", address: "a-addr" },
                { name: "b", address: "b-addr" },
              ],
            },
            {
              id: "2",
              filters: [
                { name: "c", address: "c-addr" },
                { name: "d", address: "d-addr" },
              ],
            },
          ]}
        />
      </Router>,
    );
  });

  describe("Sort component", () => {
    it("can select element from list", () => {
      // click on Select component to open dropdown list
      wrapper
        .find(Select)
        .find(".Select")
        .first()
        .find("div.Select-control")
        .simulate("mousedown", {
          button: 0,
        });
      const options = wrapper.find("Option.Select-option");
      expect(options).toHaveLength(2);
      expect(options.first().text()).toEqual("1");
      // click on first option in dropdown
      options.first().simulate("mousedown", {
        button: 0,
      });
      // expecting to have collapsed dropdown after option is selected
      expect(wrapper.exists("Option.Select-option")).toBe(false);
      expect(onChangeCollapseMock).toHaveBeenCalledWith(false);
      expect(trackNetworkSortSpy).toHaveBeenCalledWith("1");
      expect(historyListenerMock).toHaveBeenCalledWith(
        "/reports/network/1",
        "PUSH",
      );
    });
  });

  describe("Filter component", () => {
    it("opens list of filter options", () => {
      // open dropdown
      wrapper
        .find(Filter)
        .first()
        .find(Dropdown)
        .first()
        .find("div.dropdown")
        .first()
        .simulate("click");

      const filter = wrapper
        .find(Filter)
        .first()
        .find(Dropdown)
        .first()
        .find("div.dropdown")
        .first();

      expect(filter.find("div.select__container")).toHaveLength(2);
      expect(
        filter.find("div.select__container>p.filter--label").first().text(),
      ).toEqual("1");

      // open dropdown with filter options
      filter
        .find("div.select__container")
        .first()
        .find(AntdSelect)
        .find("div.ant-select-selection__rendered")
        .first()
        .simulate("click");

      // test that filter contains required filter options.
      expect(
        wrapper.find("div.multiple-filter__selection div.filter__checkbox"),
      ).toHaveLength(2);
      expect(
        wrapper
          .find(
            "div.multiple-filter__selection div.filter__checkbox .filter__checkbox--label",
          )
          .first()
          .text(),
      ).toEqual("a: a-addr");
      expect(
        wrapper
          .find(
            "div.multiple-filter__selection div.filter__checkbox .filter__checkbox--label",
          )
          .at(1)
          .text(),
      ).toEqual("b: b-addr");

      wrapper
        .find(AntdCheckbox)
        .find("span.ant-checkbox > input")
        .first()
        .simulate("change");

      // test that first filter option is checked.
      expect(onChangeFilterMock).toBeCalledWith("1", "a");
    });
  });
});
