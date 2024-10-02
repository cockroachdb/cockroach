// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { Gear } from "@cockroachlabs/icons";
import classNames from "classnames/bind";
import React from "react";
import Select, { components, OptionsType, ActionMeta } from "react-select";

import { Button } from "../button";
import {
  dropdown,
  dropdownContentWrapper,
  hidden,
} from "../queryFilter/filterClasses";

import styles from "./columnsSelector.module.scss";

const cx = classNames.bind(styles);

export interface SelectOption {
  label: string;
  value: string;
  isSelected: boolean;
}

export interface ColumnsSelectorProps {
  // options provides the list of available columns and their initial selection state
  options: SelectOption[];
  onSubmitColumns: (selectedColumns: string[]) => void;
  size?: "default" | "small";
}

export interface ColumnsSelectorState {
  hide: boolean;
  selectionState: Map<string, boolean>;
}

/**
 * Create all options items using the values from options
 * on ColumnsSelector()
 * The options must have the parameters label and isSelected
 * @param props
 * @constructor
 */
const CheckboxOption = (props: any) => {
  return (
    <components.Option {...props}>
      <input
        type="checkbox"
        className={cx("checkbox__input")}
        checked={props.isSelected}
        onChange={() => null}
      />
      <label className={cx("checkbox__label")}>{props.label}</label>
    </components.Option>
  );
};

// customStyles uses the default styles provided from the
// react-select component and add changes
const customStyles = {
  container: (provided: any) => ({
    ...provided,
    border: "none",
  }),
  control: (provided: any) => ({
    ...provided,
    display: "none",
  }),
  menu: (provided: any) => ({
    ...provided,
    position: "relative",
    boxShadow: "none",
  }),
  menuList: (provided: any) => ({
    ...provided,
    maxHeight: "310px",
  }),
  option: (provided: any, _state: any) => ({
    ...provided,
    backgroundColor: "white",
    color: "#475872",
    cursor: "pointer",
    padding: "4px 10px",
  }),
  multiValue: (provided: any) => ({
    ...provided,
    backgroundColor: "#E7ECF3",
    borderRadius: "3px",
  }),
};

/**
 * Creates the ColumnsSelector from the props
 * @param props:
 * options (SelectOption[]): a list of options. Each option object must contain a
 * label, value and isSelected parameters
 * onSubmitColumns (callback function): receives the selected string
 * @constructor
 */
export default class ColumnsSelector extends React.Component<
  ColumnsSelectorProps,
  ColumnsSelectorState
> {
  constructor(props: ColumnsSelectorProps) {
    super(props);
    const allSelected = props.options.every(o => o.isSelected);
    // set initial state of selections based on props
    const selectionState = new Map(
      props.options.map(o => [o.value, allSelected || o.isSelected]),
    );
    selectionState.set("all", allSelected);
    this.state = {
      hide: true,
      selectionState,
    };
  }
  dropdownRef: React.RefObject<HTMLDivElement> = React.createRef();

  componentDidMount(): void {
    window.addEventListener("click", this.outsideClick, false);
  }
  componentWillUnmount(): void {
    window.removeEventListener("click", this.outsideClick, false);
  }

  toggleOpen = () => {
    this.setState({
      hide: !this.state.hide,
    });
  };
  outsideClick = () => {
    this.setState({ hide: true });
  };
  insideClick: React.MouseEventHandler<HTMLDivElement> = event => {
    event.stopPropagation();
  };

  handleChange = (
    _selectedOptions: OptionsType<SelectOption>,
    // get actual selection of specific option and action type from "actionMeta"
    actionMeta: ActionMeta<SelectOption>,
  ): void => {
    const { action } = actionMeta;
    if (action !== "select-option" && action !== "deselect-option") {
      return;
    }
    const option = actionMeta.option;
    const selectionState = new Map(this.state.selectionState);
    // true - if option was selected, false - otherwise
    const isSelectedOption = action === "select-option";

    // if "all" option was toggled - update all other options
    if (option.value === "all") {
      selectionState.forEach((_v, k) =>
        selectionState.set(k, isSelectedOption),
      );
    } else {
      // check if all other options (except current changed and "all" options) are selected as well to select "all" option
      const allOtherOptionsSelected = [...selectionState.entries()]
        .filter(([k, _v]) => ![option.value, "all"].includes(k)) // filter all options except currently changed and "all" option
        .every(([_k, v]) => v);

      // update "all" option if other options are selected
      if (allOtherOptionsSelected) {
        selectionState.set("all", isSelectedOption);
      }

      selectionState.set(option.value, isSelectedOption);
    }
    this.setState({
      selectionState,
    });
  };

  handleSubmit = (): void => {
    const { selectionState } = this.state;
    const selectedValues = this.props.options
      .filter(o => selectionState.get(o.value))
      .filter(o => o.value !== "all") // do not include artificial option "all". It should live only inside this component.
      .map(o => o.value);
    this.props.onSubmitColumns(selectedValues);
    this.setState({ hide: true });
  };

  isAllSelected = (): boolean => {
    return this.state.selectionState.get("all");
  };

  // getOptions returns list of all options with updated selection states
  // and prepends "all" option as an artificial option
  getOptions = (): SelectOption[] => {
    const { options } = this.props;
    const { selectionState } = this.state;
    const isAllSelected = this.isAllSelected();
    const allOption: SelectOption = {
      label: "All",
      value: "all",
      isSelected: isAllSelected,
    };
    return [allOption, ...options].map(o => {
      let isSelected = o.isSelected; // default value;
      if (isAllSelected) {
        isSelected = true;
      } else if (selectionState.has(o.value)) {
        isSelected = selectionState.get(o.value);
      }
      return {
        ...o,
        // if "all" is selected then every item in the list selected as well
        isSelected,
      };
    });
  };

  render(): React.ReactElement {
    const { hide } = this.state;
    const { size = "default" } = this.props;
    const dropdownArea = hide ? hidden : dropdown;
    const options = this.getOptions();
    const columnsSelected = options.filter(o => o.isSelected);

    return (
      <div
        onClick={this.insideClick}
        ref={this.dropdownRef}
        className={cx("btn-area")}
      >
        <Button type="secondary" size={size} onClick={this.toggleOpen}>
          <Gear className={cx("icon")} />
          Columns
        </Button>
        <div className={dropdownArea}>
          <div className={dropdownContentWrapper}>
            <div className={cx("label")}>Hide/show columns</div>
            <Select<SelectOption, true>
              isMulti
              menuIsOpen={true}
              options={options}
              value={columnsSelected}
              onChange={this.handleChange}
              hideSelectedOptions={false}
              closeMenuOnSelect={false}
              components={{ Option: CheckboxOption }}
              styles={customStyles}
              controlShouldRenderValue={false}
            />
            <div className={cx("apply-btn__wrapper")}>
              <Button
                className={cx("apply-btn__btn")}
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
