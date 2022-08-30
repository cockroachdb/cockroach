// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import React from "react";
import { DropdownButton } from "../dropdown";
import { OutsideEventHandler } from "../outsideEventHandler";
import classnames from "classnames/bind";
import styles from "../dropdown/dropdown.module.scss";
import { applyBtn } from "../queryFilter/filterClasses";
import { Button } from "../button";

const cx = classnames.bind(styles);

type FilterDropdownProps = React.PropsWithChildren<{
  className?: string;
  label: string;
  onSubmit: () => void;
}>;

export const FilterDropdown = ({
  className,
  label,
  onSubmit,
  children,
}: FilterDropdownProps): React.ReactElement => {
  const [isOpen, setIsOpen] = React.useState<boolean>(false);
  const toggleMenuState = React.useCallback(() => {
    setIsOpen(!isOpen);
  }, [isOpen]);

  const onSubmitCallback = React.useCallback(() => {
    onSubmit();
    setIsOpen(false);
  }, [onSubmit]);

  const menuStyles = cx(
    "crl-dropdown__menu",
    `crl-dropdown__menu--align-left`,
    {
      "crl-dropdown__menu--open": isOpen,
    },
  );

  return (
    <div
      className={cx("crl-dropdown", className)}
      onClick={event => event.stopPropagation()}
    >
      <OutsideEventHandler onOutsideClick={() => setIsOpen(false)}>
        <div className={cx("crl-dropdown__handler")} onClick={toggleMenuState}>
          <DropdownButton isOpen={true}>{label}</DropdownButton>
        </div>
        <div className={cx("crl-dropdown__overlay")}>
          <div className={menuStyles}>
            <div
              className={cx(
                "crl-dropdown__container",
                "crl-dropdown__container__wrapped",
              )}
            >
              {children}
              <div className={applyBtn.wrapper}>
                <Button
                  className={applyBtn.btn}
                  textAlign="center"
                  onClick={onSubmitCallback}
                >
                  Apply
                </Button>
              </div>
            </div>
          </div>
        </div>
      </OutsideEventHandler>
    </div>
  );
};
