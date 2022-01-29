import React, { useEffect, useState } from "react";
import {
  ColumnDescriptor,
  EmptyTable,
  Search,
  SortedTable,
  SortSetting,
} from "@cockroachlabs/cluster-ui";
import { Button, Icon } from "@cockroachlabs/ui-components";
import Dropdown from "src/views/shared/components/dropdown";
import {
  PageConfig,
  PageConfigItem,
} from "src/views/shared/components/pageconfig";
import moment from "moment";

const TS_FORMAT = "MMMM Do YYYY, h:mm:ss a"; // January 28th 2022, 7:12:40 pm;

const API = {
  createSnapshot: () => {
    return fetch(`/debug/tracez/snapshot`, { method: "POST" }).then(r =>
      r.json(),
    );
  },
  getSnapshot: (id: string) => {
    return fetch(`/debug/tracez/snapshot?snap=${id}`, {
      method: "GET",
    }).then(r => r.json());
  },
  listSnapshots: () => {
    return fetch("/debug/tracez/snapshot", { method: "GET" }).then(r =>
      r.json(),
    );
  },
};

interface snapshot_info {
  captured_at?: string;
  id?: string;
}

const SnapshotSelector = ({
  setSnapshot,
  snapshots,
  currentSnapshot,
}: {
  setSnapshot: (id: string) => void;
  snapshots: snapshot_info[];
  currentSnapshot: snapshot_info;
}) => {
  return (
    <Dropdown
      title="Snapshots"
      options={snapshots.map(s => {
        return {
          value: s.id,
          label: `${s.id}: ${moment(s.captured_at).format(TS_FORMAT)}`,
        };
      })}
      selected={currentSnapshot.id}
      onChange={dropdownOption => setSnapshot(dropdownOption.value)}
    />
  );
};

interface SnapshotRow {
  operation: string;
  startTime: string;
  relativeTime: string;
  goroutineID: string;
  spanID: string;
  stack: string;
  traceID: string;
}

const GoroutineToggler = ({ id, stack }: { id: string; stack: string }) => {
  const [showStack, setShowStack] = useState<Boolean>(false);

  if (!showStack) {
    return (
      <Button as="a" intent="tertiary" onClick={() => setShowStack(true)}>
        {id}
      </Button>
    );
  } else {
    return (
      <div>
        <Button as="a" intent="tertiary" onClick={() => setShowStack(false)}>
          Hide
        </Button>
        <pre>{stack}</pre>
      </div>
    );
  }
};

const snapshotColumns: ColumnDescriptor<SnapshotRow>[] = [
  {
    title: "Operation",
    name: "operation",
    cell: sr => sr.operation,
    sort: sr => sr.operation,
  },
  {
    title: "Start Time",
    name: "startTime",
    cell: sr => `${moment(sr.startTime).format(TS_FORMAT)}`,
    sort: sr => moment(sr.startTime),
  },
  {
    title: "Goroutine ID",
    name: "goroutineID",
    cell: sr => <GoroutineToggler id={sr.goroutineID} stack={sr.stack} />,
    sort: sr => sr.goroutineID,
  },
];

export class SnapshotSortedTable extends SortedTable<SnapshotRow> {}

const CurrentSnapshot = ({
  snapshot,
  search,
}: {
  snapshot: Snapshot;
  search: string;
}) => {
  const [sortSetting, setSortSetting] = useState<SortSetting>({
    ascending: true,
    columnTitle: "startTime",
  });
  return (
    <SnapshotSortedTable
      data={snapshot.rows.filter(r => {
        return JSON.stringify(r)
          .toLowerCase()
          .includes(search.toLowerCase());
      })}
      columns={snapshotColumns}
      sortSetting={sortSetting}
      onChangeSortSetting={setSortSetting}
      renderNoResult={<EmptyTable title="No snapshot selected" />}
    />
  );
};

interface Snapshot {
  id?: string;
  rows?: SnapshotRow[];
  captured_at?: string;
}

export const Tracez = () => {
  const [snapshots, setSnapshots] = useState([]);
  const [snapshot, setSnapshot] = useState<Snapshot>({ rows: [] });
  const [search, setSearch] = useState<string>("");

  const takeSnapshot = () => {
    API.createSnapshot().then(result => {
      console.log(result); // TODO(davidh): could be clever and append to existing list
      getSnapshots();
    });
  };

  const getSnapshots = () => {
    API.listSnapshots().then(result => {
      setSnapshots(result.all_snapshots);
    });
  };

  const setSnapshotID = (id: string) => {
    API.getSnapshot(id).then(result => {
      const snapshotRows: SnapshotRow[] = [];
      // TODO(davidh): need some grownup types here
      result.spans_list.spans.map((sl: any) => {
        snapshotRows.push({
          goroutineID: sl.GoroutineID,
          operation: sl.Operation,
          relativeTime: result.captured_at,
          spanID: sl.spanID,
          stack: result.spans_list.stacks[sl.GoroutineID],
          startTime: sl.Start,
          traceID: sl.TraceID,
        });
      });
      setSnapshot({
        id: result.snapshot_id,
        captured_at: result.captured_at,
        rows: snapshotRows,
      });
    });
  };

  useEffect(getSnapshots, []);

  return (
    <div>
      <h3 className="base-heading">Active Traces</h3>
      <PageConfig>
        <PageConfigItem>
          <Button onClick={takeSnapshot} intent="secondary">
            <Icon iconName="Download" /> Take snapshot
          </Button>
        </PageConfigItem>
        <PageConfigItem>
          <SnapshotSelector
            setSnapshot={setSnapshotID}
            snapshots={snapshots}
            currentSnapshot={snapshot}
          />
        </PageConfigItem>
        <PageConfigItem>
          <Search
            /* Use of `any` type here is due to some issues with `Search` component. */
            onSubmit={setSearch as any}
            onClear={() => setSearch("")}
            defaultValue={search}
            placeholder={"Search snapshot"}
          />
        </PageConfigItem>
      </PageConfig>
      <section className="section">
        <CurrentSnapshot snapshot={snapshot} search={search} />
      </section>
    </div>
  );
};
