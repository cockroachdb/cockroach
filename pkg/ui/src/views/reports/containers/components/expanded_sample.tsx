// Copyright 2019 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

import _ from "lodash";
import * as React from "react";
import Long from "long";

import * as protos from  "src/js/protos";

import { addDuration, compareTimestamps, subtractTimestamps, formatDuration, formatDateTime, timestampToDate } from "./time_util";

import "./expanded_sample.styl";

function getColor(node_id: number) {
  const colors: string[] = ['#e6b8af', '#fce5cd', '#d9ead3', '#c9daf8', '#d9d2e9', '#f4cccc', '#fff2cc', '#d0e0e3', '#cfe2f3', '#ead1dc'];
  return colors[(node_id - 1) % colors.length];
}

function isSpanPending(sp: protos.cockroach.util.tracing.IRecordedSpan) {
  return !sp.duration || !sp.duration.seconds || !sp.duration.nanos;
}

export class TraceLine {
  node_id: number;
  depth: number;
  span?: protos.cockroach.util.tracing.IRecordedSpan;
  sample?: protos.cockroach.util.tracing.ComponentActivity.ISample;
  log?: protos.cockroach.util.tracing.RecordedSpan.ILogRecord;

  constructor(node_id: number,
              depth: number,
              span: protos.cockroach.util.tracing.IRecordedSpan,
              sample: protos.cockroach.util.tracing.ComponentActivity.ISample,
              log: protos.cockroach.util.tracing.RecordedSpan.ILogRecord) {
    this.node_id = node_id;
    this.depth = depth;
    this.span = span;
    this.sample = sample;
    this.log = log;
  }

  timestamp = () => {
    if (this.span) {
      return this.span.start_time;
    }
    return this.log.time;
  }

  formatMessage = () => {
    if (this.sample) {
      return this.span.tags["component"] + ": " + this.span.operation + (isSpanPending(this.span) ? " [pending]" : "");
    } else if (this.span) {
      return this.span.operation + (isSpanPending(this.span) ? " [pending]" : "");
    } else if (this.log.fields.length == 1) {
      return this.log.fields[0].value;
    }
    var str: string = "";
    this.log.fields.forEach((l) => {
      if (str.length) str += ", ";
      str += l.key + ": " + l.value;
    });
    return str;
  }

  formatMessageTitle = (expanded: boolean) => {
    if (this.sample) {
      if (expanded) {
        return this.formatMessage();
      }
      var attrs: string[] = _.map(this.sample.attributes, (v, k) => { return "\n" + k + ": " + v });
      return "Node: " + this.node_id +
        "\nTimestamp: " + formatDateTime(this.span.start_time, false) +
        "\nDuration: " + formatDuration(this.span.duration, false) +
        "\nPending: " + (this.sample.pending ? "Yes" : "No") +
        "\nStuck: " + (this.sample.stuck ? "Yes" : "No") +
        (this.sample.error ? ("\nError: " + this.sample.error) : "") +
        attrs.join("");
    } else if (this.span) {
      return this.span.operation + " " + formatDuration(this.span.duration, false) +
        " @" + formatDateTime(this.span.start_time, false);
    }
    return this.formatMessage() + " @" + formatDateTime(this.log.time, false);
  }

  formatMessageHTML = () => {
    return (
        <div className="tags">
          <div><span className="tag">Node</span>: {this.node_id}</div>
          <div><span className="tag">Timestamp</span>: {formatDateTime(this.span.start_time, false)}</div>
          <div><span className="tag">Duration</span>: {formatDuration(this.span.duration, false)}</div>
          <div><span className="tag">Pending</span>: {this.sample.pending ? "Yes" : "No"}</div>
          <div><span className="tag">Stuck</span>: {this.sample.stuck ? "Yes" : "No"}</div>
          {this.sample.error &&
           <div><span className="tag">Error</span>: {this.sample.error}</div>
          }
          {
            _.map(this.sample.attributes, (v, k) => (
                <div><span className="tag">{k}</span>: {v}</div>
            ));
        </div>
    );
  }

  formatTime = (last_node_id: number, last_time: protos.google.protobuf.ITimestamp) => {
    if (this.span && this.depth == 0) {
      var date_str: string = "";
      const span_d: any = timestampToDate(this.span.start_time);
      const cur_d: any = new Date();
      if (span_d.getDate() != cur_d.getDate() ||
          span_d.getMonth() != cur_d.getMonth() ||
          span_d.getFullYear() != cur_d.getFullYear()) {
        date_str = " " + span_d.getMonth() + "/" + span_d.getDate() + "/" + span_d.getFullYear() + " @";
      }
      const node_prefix: string = last_node_id != this.node_id ? ("Node " + this.node_id + ": ") : "";
      return node_prefix + date_str + formatDateTime(this.span.start_time, true);
    } else if (this.span) {
      const delta: protos.google.protobuf.Duration = subtractTimestamps(this.span.start_time, last_time);
      const node_prefix: string = last_node_id != this.node_id ? ("Node " + this.node_id + ": ") : "";
      return node_prefix + "+" + formatDuration(delta, true);
    }
    const delta: protos.google.protobuf.Duration = subtractTimestamps(this.log.time, last_time);
    return "+" + formatDuration(delta, true);
  }

  formatTimeTitle = () => {
    if (this.span) {
      return formatDuration(this.span.duration, false) + " @" + formatDateTime(this.span.start_time, false);
    }
    return "@" + formatDateTime(this.log.time, false);
  }
}

export class ExpandedSpan {
  node_id: number;
  span: protos.cockroach.util.tracing.IRecordedSpan;
  // Array of trace lines.
  lines: TraceLine[];
  // Array of overlapping child spans.
  children: ExpandedSpan[];
  // Pre-calculated state for rendering.
  width: number;
  length: number;
  col_no: number;
  line_no: number;

  constructor(node_id: number,
              span: protos.cockroach.util.tracing.IRecordedSpan,
              sample: protos.cockroach.util.tracing.ComponentActivity.ISample) {
    this.node_id = node_id;
    this.span = span;
    this.lines = [new TraceLine(node_id, 0, span, sample, null)];
    span.logs.forEach((l) => {
      this.lines.push(new TraceLine(node_id, 1, null, null, l));
    });
    this.children = [];
  }

  // Returns the index of the log line which occurs just before the start
  // of the supplied span.
  getLineIndex = (ts: protos.google.protobuf.ITimestamp) => {
    var l: number = 0;
    var r: number = this.lines.length;
    while (l < r) {
      var m: number = Math.floor((l + r) / 2);
      if compareTimestamps(this.lines[m].timestamp(), ts) <= 0 {
        l = m + 1;
      } else {
        r = m;
      }
    }
    return l - 1;
  }

  // Returns whether the supplied span's trace lines overlap any of
  // this expanded span's trace lines.
  overlaps = (span: protos.cockroach.util.tracing.IRecordedSpan) => {
    // If the span is still pending, assume overlap.
    if (isSpanPending(span) && this.lines.length > 1) {
      return true;
    }
    // If the child span extends beyond the parent, consider it an overlap.
    if (!isSpanPending(this.span) &&
        compareTimestamps(addDuration(span.start_time, span.duration),
                          addDuration(this.span.start_time, this.span.duration)) > 0) {
      return true;
    }
    if (span.logs.length == 0) {
      return false;
    }
    const start: number = this.getLineIndex(span.start_time);
    const end: number = this.getLineIndex(addDuration(span.start_time, span.duration));
    return start != end;
  }

  // Adds the child span. If it overlaps, add to list of overlapping
  // children. Otherwise, embed the span's trace lines in the parent.
  addOrEmbedChild = (es: ExpandedSpan) => {
    // If the span overlaps, add it to the children array.
    if (this.overlaps(es.span)) {
      this.children.push(es);
      return es;
    }
    // Otherwise, embed the span's trace lines at the appropriate index.
    const idx: number = this.getLineIndex(es.span.start_time);
    const baseDepth: number = (idx >= this.lines.length) ? this.lines[this.lines.length-1].depth : this.lines[idx].depth;
    // Augment depth of embedded lines.
    es.lines.forEach((l) => {
      l.depth += baseDepth;
    });
    // Embed lines.
    this.lines.splice(idx + 1, 0, ...es.lines);
    // Concat child spans from embedded span.
    this.children = this.children.concat(es.children);
    // Return this, to replace child with parent, as child has been embedded.
    return this;
  }

  // Recursively increase the column number for this span and children.
  pushColNumber = (delta: number) => {
    this.col_no += delta;
    this.children.forEach((c) => {
      c.pushColNumber(delta);
    });
  }

  // Organizes child spans by recursively descending the children
  // list and setting line number and column number.
  organizeChildren = (col_no: number, line_no: number) => {
    this.width = 1;
    this.length = this.lines.length;
    this.col_no = col_no;
    this.line_no = line_no;
    // Sort children.
    this.children.sort(function(a, b) {
      return compareTimestamps(a.span.start_time, b.span.start_time);
    });
    // Recurse into children to get width. Push prior children to
    // higher column numbers in the event that a subsequent child will
    // overlap.
    for (let i = 0; i < this.children.length; i++) {
      const cSpan: ExpandedSpan = this.children[i];
      cSpan.organizeChildren(col_no + 1, this.line_no + this.getLineIndex(cSpan.span.start_time) + 1);
      if (i > 0) {
        var cur: ExpandedSpan = cSpan;
        for (let j = i-1; j >= 0; j--) {
          const prev: ExpandedSpan = this.children[j];
          if (cur.line_no <= prev.line_no + prev.length && cur.col_no + cur.width > prev.col_no) {
            const delta: number = cur.col_no + cur.width - prev.col_no;
            prev.pushColNumber(delta);
            cur = prev;
          }
        }
      }
      this.length = Math.max(this.length, cSpan.line_no - this.line_no + cSpan.length);
    }

    // Compute the width now that all children have been fully pushed.
    this.children.forEach((cSpan) => {
      this.width = Math.max(this.width, (cSpan.col_no - this.col_no) + cSpan.width);
    });
  }

  renderTraceColumns = (props: ExpandedSampleProps) => {
    var columns = [];
    const line: TraceLine = this.lines[props.line_no - this.line_no];
    const expanded: boolean = line.sample && (line.span.span_id in props.expandedComponents);
    function onClick(e) {
      props.onToggleComponent(line.span.span_id);
    }
    const log_style: any = {
      "padding-left": (5 + line.depth * 10) + "px",
      "background":   getColor(line.node_id),
    };
    const time_style: any = {
      "background": getColor(line.node_id),
    };
    var log_class: string = "log";
    var time_class: string = "time";
    if (line.sample) {
      log_class += " component-span";
      if (expanded) {
        log_class += " expanded";
      }
    } else if (line.span) {
      log_class += " span";
    }
    if (props.line_no == this.line_no) {
      log_class += " top";
      time_class += " top";
    }
    if (props.line_no == this.line_no + this.lines.length - 1) {
      log_class += " bottom";
      time_class += " bottom";
    }
    const msg: string = line.formatMessage();
    const msg_title: string = line.formatMessageTitle(expanded);
    columns.push(
        <td className={log_class} style={log_style} title={msg_title} onClick={onClick}>
          {msg}
          {expanded &&
           <line.formatMessageHTML />
          }
        </td>
    );
    const last_line: TraceLine = (props.line_no - this.line_no) == 0 ? null : this.lines[props.line_no - this.line_no - 1];
    const last_node_id: number = last_line ? last_line.node_id : null;
    const last_time: protos.google.protobuf.ITimestamp = last_line ? last_line.timestamp() : null;
    const time: string = line.formatTime(last_node_id, last_time);
    const time_title: string = line.formatTimeTitle();
    columns.push(<td className={time_class} style={time_style} title={time_title}>{time}</td>);

    return columns;
  }

  connector = (props: any) => {
    switch (props.style) {
    case "none":
      return ( <svg width="100%" height="20" display="block" preserveAspectRatio="none"></svg> );
    case "straight":
      return (
          <svg width="100%" height="20" display="block" preserveAspectRatio="none">
            <line x1="0" y1="12" x2="100%" y2="12" className="connector" />
          </svg>
      );
    case "tee":
      return (
          <svg width="100%" height="20" display="block" preserveAspectRatio="none">
            <line x1="0" y1="12" x2="100%" y2="12" className="connector" />
            <line x1="12" y1="12" x2="12" y2="20" className="connector" />
          </svg>
      );
    case "angle":
      return (
          <svg width="100%" height="20" display="block" preserveAspectRatio="none">
            <line x1="0" y1="12" x2="12" y2="12" className="connector" />
            <line x1="12" y1="12" x2="12" y2="20" className="connector" />
          </svg>
      );
    }
  }

  // Renders line 'props.line_no' from this trace, calling this
  // method recursively for children spans.
  renderTraceLine = (props: ExpandedSampleProps) => {
    if (props.line_no < this.line_no || props.line_no >= this.line_no + this.length) {
      return null;
    }

    var prepend = [];
    // If the column number we're supposed to render is less than
    // the column we're rendering, insert empty columns as padding.
    if (props.col_no != 0 && props.col_no < this.col_no) {
      //console.log("line", props.line_no, "prepending", this.col_no, "-", props.col_no, "empty columns");
      for (let i = 0; i < this.col_no - props.col_no; i++) {
        if (props.col_no + i > 0) {
          prepend.push(<td className="gap"></td>);
        }
        prepend.push(<td className="empty"></td>);
        prepend.push(<td className="empty"></td>);
      }
    }

    var columns = [];
    // If this isn't the first column, add a gap.
    if (props.col_no != 0) {
      columns.push(<td className="gap"></td>);
    }

    // Render the appropriate trace line.
    if (props.line_no >= this.line_no + this.lines.length) {
      //console.log("line", props.line_no, "adding empty columns within trace because", props.line_no, ">=", this.line_no, "+", this.lines.length);
      columns.push(<td className="empty"></td>);
      columns.push(<td className="empty"></td>);
    } else {
      //console.log("line", props.line_no, "adding columns");
      columns.push(...this.renderTraceColumns(props));
    }

    // Recurse to render children.
    var col_no: number = this.col_no + 1;
    var next_children_cols: number[] = []; // children which start on the following line.
    for (let i = this.children.length - 1; i >= 0; i--) {
      const result: any = this.children[i].renderTraceLine(Object.assign({}, props, {col_no: col_no}));
      if (result) {
        columns.push(...result);
        col_no = this.children[i].col_no + this.children[i].width;
      } else if (this.children[i].line_no == props.line_no + 1) {
        // Here's a child that starts on the following line; add it to next_children_cols.
        next_children_cols.push(this.children[i].col_no);
        // If there are no more children or no more on the next line, add the connector columns.
        if (i == 0 || this.children[i-1].line_no != props.line_no + 1) {
          var next_col: number = next_children_cols.shift();
          for (; col_no <= this.children[i].col_no; col_no++) {
            // If the current column isn't yet at the next child column, add a straight connector.
            if (col_no < next_col) {
              columns.push(<td className="gap"><this.connector style="straight" /></td>);
              columns.push(<td className="empty"><this.connector style="straight" /></td>);
              columns.push(<td className="empty"><this.connector style="straight" /></td>);
            } else if (col_no == next_col) {
              columns.push(<td className="gap"><this.connector style="straight" /></td>);
              columns.push(<td className="empty"><this.connector style={next_children_cols.length ? "tee" : "angle"} /></td>);
              columns.push(<td className="empty"><this.connector style={next_children_cols.length ? "straight" : "none"} /></td>);
              next_col = next_children_cols.shift();
            }
          }
        }
      }
    }
    const width: number = Math.floor((columns.length + 1) / 3);
    //console.log("line", props.line_no, "width", width, "(" + columns.length + "+1) / 3", "vs this.width=", this.width);
    if (width < this.width) {
      //console.log("line", props.line_no, "adding", (this.width - width), "empty columns at col no", col_no);
      for (let i = 0; i < this.width - width; i++) {
        columns.push(<td className="gap"></td>);
        columns.push(<td className="empty"></td>);
        columns.push(<td className="empty"></td>);
      }
    }

    prepend.push(...columns);
    return prepend;
  }
}

export class ExpandedSampleProps {
  line_no: number;
  col_no: number;
}

export class ExpandedSample {
  active: boolean;
  error: any;
  trace_id: Long;
  span_id: Long;
  root: ExpandedSpan;

  constructor(trace_id: Long, span_id: Long, nodes: protos.cockroach.server.serverpb.ComponentTraceResponse.INodeResponse[]) {
    this.active = false;
    this.trace_id = trace_id;
    this.span_id = span_id;

    const spans: {[span_id: Long]: ExpandedSpan} = {};
    const roots: Long[] = [];
    const children: {[span_id: Long]: Long[]} = {};

    // For each node response, traverse the samples in each reported
    // component, adding each to the spans and children maps if the
    // trace id matches the trace id of the sample to be expanded.
    nodes.forEach((n) => {
      _.map(n.samples, (ca, name) => {
        ca.samples.forEach((s) => {
          s.spans.forEach((sp) => {
            spans[sp.span_id] = new ExpandedSpan(n.node_id, sp, sp.tags["component"] == name ? s : null);
            if (sp.parent_span_id && sp.parent_span_id.toNumber() != 0) {
              if (!(sp.parent_span_id.toString() in children)) {
                children[sp.parent_span_id] = [];
              }
              children[sp.parent_span_id].push(sp.span_id);
            } else {
              roots.push(sp.span_id);
            }
          });
        });
      });
    });

    // Sort all child lists. If any child list is orphaned, create parent
    // span placeholder and add to roots slice..
    _.map(children, (l, parent_id) => {
      l.sort(function(a, b) {
        return compareTimestamps(spans[a].span.start_time, spans[b].span.start_time);
      });
      if (!(parent_id.toString() in spans)) {
        // Create a parent span placeholder for orphaned child list.
        const firstChild: ExpandedSpan = spans[l[0]]; 
        const placeSpan= protos.cockroach.util.tracing.RecordedSpan.create({
          trace_id: trace_id,
          span_id: parent_id,
          operation: "[missing parent span]",
          start_time: firstChild.span.start_time,
          duration: firstChild.span.duration,
          logs: [],
        });
        const placeholder: ExpandedSpan = new ExpandedSpan(firstChild.node_id, placeSpan, null);
        spans[parent_id] = placeholder;
        roots.push(parent_id);
      }
    });

    // Sort all roots by start time and make the earliest start time the
    // single root and the others its children, Note that we can expect
    // multiple roots if there are orphaned spans.
    roots.sort(function(a, b) {
      return compareTimestamps(spans[a].span.start_time, spans[b].span.start_time);
    });
    const root: Long = roots[0];
    if (!(root in children)) {
      children[root] = [];
    }
    roots.slice(1).forEach((r) => {
      children[root].push(r);
    });

    // Add all children to parents. Note that when adding a child, it can be
    // subsumed by the parent span if it's embedded. In this case, we must
    // replace the child in the spans map.
    _.map(children, (l, parent_id) => {
      l.forEach((c) => {
        spans[c] = spans[parent_id].addOrEmbedChild(spans[c]);
      });
    });

    // Organize all children by setting their column and line numbers
    // such that they won't overlap when rendered.
    spans[root].organizeChildren(0, 0);

    this.root = spans[root];
  }

  // Render the component trace.
  Render = (props) => {
    var line_nos: number[] = [];
    for (let i = 0; i < this.root.length; i++) {
      line_nos.push(i);
    }
    return (
      <table id="sample-trace">
        {
          _.map(line_nos, (no) => (
            <tr className="line">
              <this.root.renderTraceLine {...props} col_no={0} line_no={no} />
            </tr>
          ));
      </table>
    );
  }
}
