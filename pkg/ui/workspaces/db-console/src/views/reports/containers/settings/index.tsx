// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import {
  Loading,
  ColumnDescriptor,
  SortedTable,
  SortSetting,
  util,
  Timestamp,
  useClusterSettings,
  ClusterSetting,
} from "@cockroachlabs/cluster-ui";
import moment from "moment-timezone";
import React, { useState } from "react";
import { Helmet } from "react-helmet";
import { RouteComponentProps, withRouter } from "react-router-dom";

import { BackToAdvanceDebug } from "../util";

import "./index.scss";

interface IterableSetting {
  key: string;
  description?: string;
  type?: string;
  value?: string;
  public?: boolean;
  last_updated?: moment.Moment;
}

const columns: ColumnDescriptor<IterableSetting>[] = [
  {
    name: "name",
    title: "Setting",
    cell: (setting: IterableSetting) => setting.key,
    sort: (setting: IterableSetting) => setting.key,
  },
  {
    name: "value",
    title: "Value",
    cell: (setting: IterableSetting) => setting.value,
  },
  {
    name: "lastUpdated",
    title: "Last Updated",
    cell: (setting: IterableSetting) => (
      <Timestamp
        time={setting.last_updated}
        format={util.DATE_FORMAT_24_TZ}
        fallback={"No overrides"}
      />
    ),
    sort: (setting: IterableSetting) => setting.last_updated?.valueOf(),
  },
  {
    name: "description",
    title: "Description",
    cell: (setting: IterableSetting) => setting.description,
  },
];

function settingsToIterableArray(
  allSettings: Record<string, ClusterSetting>,
): IterableSetting[] {
  return Object.entries(allSettings).map(([key, cs]) => ({
    key,
    description: cs.description,
    type: cs.type,
    value: cs.value,
    public: cs.public,
    last_updated: cs.lastUpdated,
  }));
}

/**
 * Renders the Cluster Settings Report page.
 */
export function Settings({ history }: RouteComponentProps): React.ReactElement {
  const [sortSetting, setSortSetting] = useState({
    ascending: true,
    columnTitle: "lastUpdated",
  });
  const [changedOnly, setChangedOnly] = useState(false);

  const { settingValues, isLoading, error } = useClusterSettings();

  const renderTable = (wantPublic: boolean) => {
    let dataArray = settingsToIterableArray(settingValues).filter(obj =>
      wantPublic ? obj.public : !obj.public,
    );
    if (changedOnly) {
      dataArray = dataArray.filter(obj => obj.last_updated != null);
    }

    return (
      <SortedTable
        data={dataArray}
        columns={columns}
        sortSetting={sortSetting}
        onChangeSortSetting={(ss: SortSetting) =>
          setSortSetting({
            ascending: ss.ascending,
            columnTitle: ss.columnTitle,
          })
        }
      />
    );
  };

  return (
    <div className="section">
      <Helmet title="Cluster Settings | Debug" />
      <BackToAdvanceDebug history={history} />
      <h1 className="base-heading">Cluster Settings</h1>
      <Loading
        loading={isLoading}
        page={"container settings"}
        error={error}
        render={() => (
          <div>
            <label className="settings-changed-only">
              <input
                type="checkbox"
                checked={changedOnly}
                onChange={e => setChangedOnly(e.target.checked)}
              />
              Changed only
            </label>
            <p className="settings-note">
              Note that some settings have been redacted for security purposes.
            </p>
            {renderTable(true)}
            <h3>Reserved settings</h3>
            <p className="settings-note">
              Note that changes to the following settings can yield
              unpredictable or negative effects on the entire cluster. Use at
              your own risk.
            </p>
            {renderTable(false)}
          </div>
        )}
      />
    </div>
  );
}

export default withRouter(Settings);
