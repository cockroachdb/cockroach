import React from "react";
import classnames from "classnames/bind";

import { OutsideEventHandler } from "../outsideEventHandler";
import styles from "./dropdown.module.scss";
import { Button, ButtonProps } from "src/button";
import { CaretDown } from "@cockroachlabs/icons";

const cx = classnames.bind(styles);

export interface DropdownOption<T = string> {
  value: T;
  name: React.ReactNode | string;
  disabled?: boolean;
}

export interface DropdownProps<T> {
  items: Array<DropdownOption<T>>;
  onChange: (item: DropdownOption<T>["value"]) => void;
  children?: React.ReactNode;
  customToggleButton?: React.ReactNode;
  customToggleButtonOptions?: Partial<ButtonProps>;
  menuPosition?: "left" | "right";
  className?: string;
}

interface DropdownState {
  isOpen: boolean;
}

interface DropdownButtonProps {
  children: React.ReactNode;
  isOpen: boolean;
  onClick?: (event: React.MouseEvent<HTMLElement>) => void;
  customProps?: Partial<ButtonProps>;
}

const DropdownButton: React.FC<DropdownButtonProps> = ({
  children,
  customProps = {},
}) => {
  return (
    <Button
      type="secondary"
      size="default"
      iconPosition="right"
      icon={<CaretDown />}
      {...customProps}
    >
      {children}
    </Button>
  );
};

function DropdownItem<T = string>(props: DropdownItemProps<T>) {
  const { children, value, onClick, disabled } = props;
  return (
    <div
      onClick={() => onClick(value)}
      className={cx("crl-dropdown__item", {
        "crl-dropdown__item--disabled": disabled,
      })}
    >
      {children}
    </div>
  );
}

export class Dropdown<T = string> extends React.Component<
  DropdownProps<T>,
  DropdownState
> {
  state = {
    isOpen: false,
  };

  handleMenuOpen = () => {
    this.setState({
      isOpen: !this.state.isOpen,
    });
  };

  changeMenuState = (nextState: boolean) => {
    this.setState({
      isOpen: nextState,
    });
  };

  handleItemSelection = (value: T) => {
    this.props.onChange(value);
    this.handleMenuOpen();
  };

  renderDropdownToggleButton = () => {
    const {
      children,
      customToggleButton,
      customToggleButtonOptions,
    } = this.props;
    const { isOpen } = this.state;

    if (customToggleButton) {
      return customToggleButton;
    } else {
      return (
        <DropdownButton isOpen={isOpen} customProps={customToggleButtonOptions}>
          {children}
        </DropdownButton>
      );
    }
  };

  render() {
    const { items, menuPosition = "left", className } = this.props;
    const { isOpen } = this.state;

    const menuStyles = cx(
      "crl-dropdown__menu",
      `crl-dropdown__menu--align-${menuPosition}`,
      {
        "crl-dropdown__menu--open": isOpen,
      },
    );

    const menuItems = items.map((menuItem, idx) => (
      <DropdownItem<T>
        value={menuItem.value}
        onClick={this.handleItemSelection}
        key={idx}
        disabled={menuItem.disabled}
      >
        {menuItem.name}
      </DropdownItem>
    ));

    return (
      <div className={cx("crl-dropdown", className)}>
        <OutsideEventHandler onOutsideClick={() => this.changeMenuState(false)}>
          <div
            className={cx("crl-dropdown__handler")}
            onClick={this.handleMenuOpen}
          >
            {this.renderDropdownToggleButton()}
          </div>
          <div className={cx("crl-dropdown__overlay")}>
            <div className={menuStyles}>
              <div className={cx("crl-dropdown__container")}>{menuItems}</div>
            </div>
          </div>
        </OutsideEventHandler>
      </div>
    );
  }
}

export interface DropdownItemProps<T> {
  children: React.ReactNode;
  value: T;
  onClick: (value: T) => void;
  disabled?: boolean;
}
