import React from "react";
import Button from "antd/lib/button";
import Form from "antd/lib/form";
import Input, { InputProps } from "antd/lib/input";
import classNames from "classnames/bind";
import {
  Cancel as CancelIcon,
  Search as SearchIcon,
} from "@cockroachlabs/icons";
import styles from "./search.module.scss";

interface ISearchProps {
  onSubmit: (search: string) => void;
  onClear?: () => void;
  defaultValue?: string;
}

interface ISearchState {
  value: string;
  submitted: boolean;
  submit?: boolean;
}

type TSearchProps = ISearchProps & InputProps;

const cx = classNames.bind(styles);

export class Search extends React.Component<TSearchProps, ISearchState> {
  state = {
    value: this.props.defaultValue || "",
    submitted: false,
  };

  onSubmit = (e: React.SyntheticEvent) => {
    e && e.preventDefault();
    const { value } = this.state;
    const { onSubmit } = this.props;
    onSubmit(value);
    if (value.length > 0) {
      this.setState({
        submitted: true,
      });
    }
  };

  onChange = (event: React.ChangeEvent<HTMLInputElement>) => {
    const value: string = event.target.value;
    const submitted = value.length === 0;
    this.setState(
      { value, submitted },
      () => submitted && this.onSubmit(event),
    );
  };

  onClear = () => {
    const { onClear } = this.props;
    this.setState({ value: "", submit: false });
    onClear();
  };

  renderSuffix = () => {
    const { value, submitted } = this.state;
    if (value.length > 0) {
      if (submitted) {
        return (
          <Button
            onClick={this.onClear}
            type="default"
            className={cx("_clear-search")}
          >
            <CancelIcon className={cx("_suffix-icon")} />
          </Button>
        );
      }
      return (
        <Button
          type="default"
          htmlType="submit"
          className={cx("_submit-search")}
        >
          Enter
        </Button>
      );
    }
    return null;
  };

  render() {
    const { value, submitted } = this.state;
    const { onClear, ...inputProps } = this.props;
    const className = submitted ? cx("_submitted") : "";

    return (
      <Form onSubmit={this.onSubmit} className={cx("_search-form")}>
        <Form.Item>
          <Input
            className={className}
            placeholder="Search Statement"
            onChange={this.onChange}
            prefix={<SearchIcon className={cx("_prefix-icon")} />}
            suffix={this.renderSuffix()}
            value={value}
            {...inputProps}
          />
        </Form.Item>
      </Form>
    );
  }
}
