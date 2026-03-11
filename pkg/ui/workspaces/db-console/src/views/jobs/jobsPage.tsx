// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.
import {
  JobsPage,
  SortSetting,
  defaultLocalOptions,
} from "@cockroachlabs/cluster-ui";
import React from "react";
import { useSelector, useDispatch } from "react-redux";
import { RouteComponentProps, withRouter } from "react-router-dom";

import { LocalSetting } from "src/redux/localsettings";
import { AdminUIState } from "src/redux/state";

export const statusSetting = new LocalSetting<AdminUIState, string>(
  "jobs/status_setting",
  s => s.localSettings,
  defaultLocalOptions.status,
);

export const typeSetting = new LocalSetting<AdminUIState, number>(
  "jobs/type_setting",
  s => s.localSettings,
  defaultLocalOptions.type,
);

export const showSetting = new LocalSetting<AdminUIState, string>(
  "jobs/show_setting",
  s => s.localSettings,
  defaultLocalOptions.show,
);

export const sortSetting = new LocalSetting<AdminUIState, SortSetting>(
  "sortSetting/Jobs",
  s => s.localSettings,
  { columnTitle: "creationTime", ascending: false },
);

export const columnsLocalSetting = new LocalSetting<AdminUIState, string[]>(
  "jobs/column_setting",
  s => s.localSettings,
  null,
);

const JobsPageWrapper: React.FC<RouteComponentProps> = props => {
  const dispatch = useDispatch();
  const sort = useSelector((state: AdminUIState) =>
    sortSetting.selector(state),
  );
  const status = useSelector((state: AdminUIState) =>
    statusSetting.selector(state),
  );
  const show = useSelector((state: AdminUIState) =>
    showSetting.selector(state),
  );
  const type = useSelector((state: AdminUIState) =>
    typeSetting.selector(state),
  );
  const columns = useSelector((state: AdminUIState) =>
    columnsLocalSetting.selectorToArray(state),
  );

  return (
    <JobsPage
      {...props}
      sort={sort}
      status={status}
      show={show}
      type={type}
      columns={columns}
      setSort={(ss: SortSetting) => dispatch(sortSetting.set(ss))}
      setStatus={(s: string) => dispatch(statusSetting.set(s))}
      setShow={(s: string) => dispatch(showSetting.set(s))}
      setType={(t: number) => dispatch(typeSetting.set(t))}
      onColumnsChange={(cols: string[]) =>
        dispatch(columnsLocalSetting.set(cols))
      }
    />
  );
};

export default withRouter(JobsPageWrapper);
