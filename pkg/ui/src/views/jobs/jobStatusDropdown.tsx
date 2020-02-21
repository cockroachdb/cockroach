import React from "react";
import {statusOptions} from "./jobStatusOptions";
import Dropdown from "src/views/shared/components/dropdown";

interface JobStatusDropdownProps {
  selectedStatus: string;
  onChange: any;
}

export class JobStatusDropdown extends React.Component<JobStatusDropdownProps> {
  render() {
    return (
      <Dropdown
        title="Status"
        options={statusOptions}
        selected={this.props.selectedStatus}
        onChange={this.props.onChange}
      />
    );
  }
}
