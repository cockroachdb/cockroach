// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { Gear } from "@cockroachlabs/icons";
import classNames from "classnames/bind";
import React, {
  useState,
  useRef,
  useEffect,
  useCallback,
  useMemo,
} from "react";
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

function createInitialSelectionState(
  options: SelectOption[],
): Map<string, boolean> {
  const allSelected = options.every(o => o.isSelected);
  const selectionState = new Map(
    options.map(o => [o.value, allSelected || o.isSelected]),
  );
  selectionState.set("all", allSelected);
  return selectionState;
}

/**
 * Creates the ColumnsSelector from the props
 * @param props:
 * options (SelectOption[]): a list of options. Each option object must contain a
 * label, value and isSelected parameters
 * onSubmitColumns (callback function): receives the selected string
 * @constructor
 */
export default function ColumnsSelector({
  options,
  onSubmitColumns,
  size = "default",
}: ColumnsSelectorProps): React.ReactElement {
  const [hide, setHide] = useState(true);
  const [selectionState, setSelectionState] = useState<Map<string, boolean>>(
    () => createInitialSelectionState(options),
  );

  const dropdownRef = useRef<HTMLDivElement>(null);

  const outsideClick = useCallback(() => {
    setHide(true);
  }, []);

  // Add and remove window click listener for outside click handling.
  useEffect(() => {
    window.addEventListener("click", outsideClick, false);
    return () => {
      window.removeEventListener("click", outsideClick, false);
    };
  }, [outsideClick]);

  const toggleOpen = useCallback(() => {
    setHide(prev => !prev);
  }, []);

  const insideClick: React.MouseEventHandler<HTMLDivElement> = useCallback(
    event => {
      event.stopPropagation();
    },
    [],
  );

  const handleChange = useCallback(
    (
      _selectedOptions: OptionsType<SelectOption>,
      // get actual selection of specific option and action type from "actionMeta"
      actionMeta: ActionMeta<SelectOption>,
    ): void => {
      const { action } = actionMeta;
      if (action !== "select-option" && action !== "deselect-option") {
        return;
      }
      const option = actionMeta.option;
      // true - if option was selected, false - otherwise
      const isSelectedOption = action === "select-option";

      setSelectionState(prev => {
        const next = new Map(prev);

        // if "all" option was toggled - update all other options
        if (option.value === "all") {
          next.forEach((_v, k) => next.set(k, isSelectedOption));
        } else {
          // check if all other options (except current changed and "all" options) are selected as well to select "all" option
          const allOtherOptionsSelected = [...next.entries()]
            .filter(([k, _v]) => ![option.value, "all"].includes(k)) // filter all options except currently changed and "all" option
            .every(([_k, v]) => v);

          // update "all" option if other options are selected
          if (allOtherOptionsSelected) {
            next.set("all", isSelectedOption);
          }

          next.set(option.value, isSelectedOption);
        }
        return next;
      });
    },
    [],
  );

  const handleSubmit = useCallback((): void => {
    const selectedValues = options
      .filter(o => selectionState.get(o.value))
      .filter(o => o.value !== "all") // do not include artificial option "all". It should live only inside this component.
      .map(o => o.value);
    onSubmitColumns(selectedValues);
    setHide(true);
  }, [options, selectionState, onSubmitColumns]);

  const isAllSelected = selectionState.get("all");

  // getOptions returns list of all options with updated selection states
  // and prepends "all" option as an artificial option
  const computedOptions = useMemo((): SelectOption[] => {
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
  }, [options, selectionState, isAllSelected]);

  const dropdownArea = hide ? hidden : dropdown;
  const columnsSelected = computedOptions.filter(o => o.isSelected);

  return (
    <div onClick={insideClick} ref={dropdownRef} className={cx("btn-area")}>
      <Button type="secondary" size={size} onClick={toggleOpen}>
        <Gear className={cx("icon")} />
        Columns
      </Button>
      <div className={dropdownArea}>
        <div className={dropdownContentWrapper}>
          <div className={cx("label")}>Hide/show columns</div>
          <Select<SelectOption, true>
            isMulti
            menuIsOpen={true}
            options={computedOptions}
            value={columnsSelected}
            onChange={handleChange}
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
              onClick={handleSubmit}
            >
              Apply
            </Button>
          </div>
        </div>
      </div>
    </div>
  );
}
