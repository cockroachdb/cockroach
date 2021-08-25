// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import classNames from "classnames/bind";
import Select from "react-select";
import React from "react";
import _ from "lodash";

import styles from "./dropdown.module.styl";

import { leftArrow, rightArrow } from "src/views/shared/components/icons";
import { trustIcon } from "src/util/trust";
import ReactSelectClass from "react-select";
import { CaretDown } from "src/components/icon/caretDown";

export interface DropdownOption {
  value: string;
  label: string;
}

export enum ArrowDirection {
  LEFT,
  RIGHT,
  CENTER,
}

interface DropdownOwnProps {
  title: string;
  selected: string;
  options: DropdownOption[];
  onChange?: (selected: DropdownOption) => void; // Callback when the value changes.
  // If onArrowClick exists, don't display the arrow next to the dropdown,
  // display left and right arrows to either side instead.
  onArrowClick?: (direction: ArrowDirection) => void;
  // Disable any arrows in the arrow direction array.
  disabledArrows?: ArrowDirection[];
  content?: any;
  isTimeRange?: boolean;
  className?: string;
  type?: "primary" | "secondary";
}

const cx = classNames.bind(styles);

export const arrowRenderer = ({ isOpen }: { isOpen: boolean }) => (
  <span className={cx("caret-down", { active: isOpen })}>
    <CaretDown />
  </span>
);

/**
 * Dropdown component that uses the URL query string for state.
 */
export default class Dropdown extends React.Component<DropdownOwnProps, {}> {
  state = {
    is_focused: false,
  };

  dropdownRef: React.RefObject<HTMLDivElement> = React.createRef();
  titleRef: React.RefObject<HTMLDivElement> = React.createRef();
  selectRef: React.RefObject<ReactSelectClass> = React.createRef();

  triggerSelectClick = (e: any) => {
    const dropdownNode = this.dropdownRef.current as Node;
    const titleNode = this.titleRef.current as Node;
    const selectNode = this.selectRef.current;

    if (
      e.target.isSameNode(dropdownNode) ||
      e.target.isSameNode(titleNode) ||
      e.target.className.indexOf("dropdown__select") > -1
    ) {
      // This is a far-less-than-ideal solution to the need to trigger
      // the react-select dropdown from the entirety of the dropdown area
      // instead of just the nodes rendered by the component itself
      // the approach borrows from:
      // https://github.com/JedWatson/react-select/issues/305#issuecomment-172607534
      //
      // a broader discussion on the status of a possible feature addition that
      // would render this hack moot can be found here:
      // https://github.com/JedWatson/react-select/issues/1989
      (selectNode as any).handleMouseDownOnMenu(e);
    }
  };

  onFocus = () => this.setState({ is_focused: true });

  onClose = () => this.setState({ is_focused: false });

  render() {
    const {
      selected,
      options,
      onChange,
      onArrowClick,
      disabledArrows,
      content,
      isTimeRange,
      type = "secondary",
    } = this.props;

    const className = cx(
      "dropdown",
      `dropdown--type-${type}`,
      {
        _range: isTimeRange,
        "dropdown--side-arrows": !_.isNil(onArrowClick),
        dropdown__focused: this.state.is_focused,
      },
      this.props.className,
    );
    const leftClassName = cx("dropdown__side-arrow", {
      "dropdown__side-arrow--disabled": _.includes(
        disabledArrows,
        ArrowDirection.LEFT,
      ),
    });
    const rightClassName = cx("dropdown__side-arrow", {
      "dropdown__side-arrow--disabled": _.includes(
        disabledArrows,
        ArrowDirection.RIGHT,
      ),
    });

    return (
      <div
        className={className}
        onClick={this.triggerSelectClick}
        ref={this.dropdownRef}
      >
        {/* TODO (maxlang): consider moving arrows outside the dropdown component */}
        <span
          className={leftClassName}
          dangerouslySetInnerHTML={trustIcon(leftArrow)}
          onClick={() => this.props.onArrowClick(ArrowDirection.LEFT)}
        ></span>
        <span
          className={cx({
            "dropdown__range-title": isTimeRange,
            dropdown__title: !isTimeRange,
          })}
          ref={this.titleRef}
        >
          {this.props.title}
          {this.props.title && !isTimeRange ? ":" : ""}
        </span>
        {content ? (
          content
        ) : (
          <Select
            className={cx("dropdown__select")}
            arrowRenderer={arrowRenderer}
            clearable={false}
            searchable={false}
            options={options}
            value={selected}
            onChange={onChange}
            onFocus={this.onFocus}
            onClose={this.onClose}
            ref={this.selectRef}
          />
        )}
        <span
          className={rightClassName}
          dangerouslySetInnerHTML={trustIcon(rightArrow)}
          onClick={() => this.props.onArrowClick(ArrowDirection.RIGHT)}
        ></span>
      </div>
    );
  }
}
