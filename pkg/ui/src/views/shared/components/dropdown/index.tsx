import classNames from "classnames";
import Select from "react-select";
import React from "react";
import _ from "lodash";

import "./dropdown.styl";

import {leftArrow, rightArrow} from "src/views/shared/components/icons";
import { trustIcon } from "src/util/trust";
import ReactSelectClass from "react-select";

export interface DropdownOption {
  value: string;
  label: string;
}

export enum ArrowDirection {
  LEFT, RIGHT,
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
}

/**
 * Dropdown component that uses the URL query string for state.
 */
export default class Dropdown extends React.Component<DropdownOwnProps, {}> {
  constructor(props: DropdownOwnProps) {
    super(props);

    this.dropdownRef = React.createRef();
    this.titleRef = React.createRef();
    this.selectRef = React.createRef();
    this.triggerSelectClick = this.triggerSelectClick.bind(this);
  }
  
  dropdownRef: React.RefObject<HTMLDivElement>;
  titleRef: React.RefObject<HTMLDivElement>;
  selectRef: React.RefObject<ReactSelectClass>;

  triggerSelectClick(e: any) {
    const dropdownNode: any = this.dropdownRef.current as Node;
    const titleNode: any = this.titleRef.current as Node;
    const selectNode: any = this.selectRef.current;

    if (e.target === dropdownNode || e.target === titleNode || e.target.className.indexOf("dropdown__select") > -1) {
      selectNode.handleMouseDownOnMenu(e);
    } 
  }

  render() {
    const {selected, options, onChange, onArrowClick, disabledArrows} = this.props;

    const className = classNames(
      "dropdown",
      { "dropdown--side-arrows": !_.isNil(onArrowClick) },
    );
    const leftClassName = classNames(
      "dropdown__side-arrow",
      { "dropdown__side-arrow--disabled": _.includes(disabledArrows, ArrowDirection.LEFT) },
    );
    const rightClassName = classNames(
      "dropdown__side-arrow",
      { "dropdown__side-arrow--disabled": _.includes(disabledArrows, ArrowDirection.RIGHT) },
    );

    return <div className={className} onClick={this.triggerSelectClick} ref={this.dropdownRef}>
      {/* TODO (maxlang): consider moving arrows outside the dropdown component */}
      <span
        className={leftClassName}
        dangerouslySetInnerHTML={trustIcon(leftArrow)}
        onClick={() => this.props.onArrowClick(ArrowDirection.LEFT)}>
      </span>
      <span className="dropdown__title" ref={this.titleRef}>{this.props.title}{this.props.title ? ":" : ""}</span>
      <Select className="dropdown__select" clearable={false} searchable={false} options={options} value={selected} onChange={onChange} ref={this.selectRef}/>
      <span
        className={rightClassName}
        dangerouslySetInnerHTML={trustIcon(rightArrow)}
        onClick={() => this.props.onArrowClick(ArrowDirection.RIGHT)}>
      </span>
    </div>;
  }
}
