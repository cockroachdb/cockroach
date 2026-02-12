// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import classNames from "classnames/bind";
import includes from "lodash/includes";
import isNil from "lodash/isNil";
import React, { useRef, useState } from "react";
import Select from "react-select";

import { CaretDown } from "src/components/icon/caretDown";
import { trustIcon } from "src/util/trust";
import { leftArrow, rightArrow } from "src/views/shared/components/icons";

import styles from "./dropdown.module.scss";

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
  onDropdownClick?: () => void;
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
export default function Dropdown({
  title,
  selected,
  options,
  onChange,
  onArrowClick,
  onDropdownClick,
  disabledArrows,
  content,
  isTimeRange,
  className: classNameProp,
  type = "secondary",
}: DropdownOwnProps): React.ReactElement {
  const [isFocused, setIsFocused] = useState(false);

  const dropdownRef = useRef<HTMLDivElement>(null);
  const titleRef = useRef<HTMLDivElement>(null);
  const selectRef = useRef<Select<DropdownOption>>(null);

  const triggerSelectClick = (e: any) => {
    onDropdownClick && onDropdownClick();
    // Don't handle click if custom dropdown menu content is rendered.
    if (content) {
      return;
    }
    const dropdownNode = dropdownRef.current as Node;
    const titleNode = titleRef.current as Node;
    const selectNode = selectRef.current;

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

  const className = cx(
    "dropdown",
    `dropdown--type-${type}`,
    {
      _range: isTimeRange,
      "dropdown--side-arrows": !isNil(onArrowClick),
      dropdown__focused: isFocused,
    },
    classNameProp,
  );
  const leftClassName = cx("dropdown__side-arrow", {
    "dropdown__side-arrow--disabled": includes(
      disabledArrows,
      ArrowDirection.LEFT,
    ),
  });
  const rightClassName = cx("dropdown__side-arrow", {
    "dropdown__side-arrow--disabled": includes(
      disabledArrows,
      ArrowDirection.RIGHT,
    ),
  });

  return (
    <div className={className} onClick={triggerSelectClick} ref={dropdownRef}>
      {/* TODO (maxlang): consider moving arrows outside the dropdown component */}
      <span
        className={leftClassName}
        dangerouslySetInnerHTML={trustIcon(leftArrow)}
        onClick={() => onArrowClick(ArrowDirection.LEFT)}
      ></span>
      <span
        className={cx({
          "dropdown__range-title": isTimeRange,
          dropdown__title: !isTimeRange,
        })}
        ref={titleRef}
      >
        {title}
        {title && !isTimeRange ? ":" : ""}
      </span>
      {content ? (
        content
      ) : (
        // The below typescript fails to typecheck because the
        // react-select library's types aren't flexible enough to
        // accept the `options` and `onChange` props that use the
        // custom `DropdownOption` type as the target. It's likely
        // that an upgrade of react-select would fix this but we
        // avoid it here because it will likely break implementation.

        /* eslint @typescript-eslint/ban-ts-comment: "off" */
        // @ts-ignore
        <Select
          className={cx("dropdown__select")}
          arrowRenderer={arrowRenderer}
          clearable={false}
          searchable={false}
          options={options}
          value={selected}
          onChange={onChange}
          onFocus={() => setIsFocused(true)}
          onClose={() => setIsFocused(false)}
          ref={selectRef}
        />
      )}
      <span
        className={rightClassName}
        dangerouslySetInnerHTML={trustIcon(rightArrow)}
        onClick={() => onArrowClick(ArrowDirection.RIGHT)}
      ></span>
    </div>
  );
}
