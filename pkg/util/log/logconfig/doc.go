// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package logconfig manages the configuration of the logging channels
// and their logging sinks.
//
// General format of the command-line flags:
//
//     --log=<yaml>
//
// The YAML configuration format works as follows:
//
//     file-defaults: #optional
//       dir: <path>           # output directory, defaults to first store dir
//       max-file-size: <sz>   # max log file size, default 10MB
//       max-group-size: <sz>  # max log file group size, default 100MB
//       sync-writes: <bool>   # whether to sync each write, default false
//       <common sink parameters>
//
//     sinks: #optional
//      stderr: #optional
//       channels: <chans>        # channel selection for stderr output, default ALL
//       <common sink parameters> # if not specified, inherit from file-defaults
//
//      file-groups: #optional
//        <filename>:
//          channels: <chans>        # channel selection for this file output, mandatory
//          max-file-size: <sz>      # defaults to file-defaults.max-file-size
//          max-group-size: <sz>     # defaults to file-defaults.max-group-size
//          sync-writes: <bool>      # defaults to file-defaults.sync-writes
//          <common sink parameters> # if not specified, inherit from file-defaults
//
//        ... repeat ...
//
//     capture-stray-errors: #optional
//       enable: <bool>       # whether to enable internal fd2 capture
//       dir: <optional>      # output directory, defaults to file-defaults.dir
//       max-group-size: <sz> # defaults to file-defaults.max-group-size
//
//     <common sink parameters>
//       filter: <severity>    # min severity level for file output, default INFO
//       redact: <bool>        # whether to remove sensitive info, default false
//       redactable: <bool>    # whether to strip redaction markers, default false
//       format: <fmt>         # format to use for log enries, default
//                             # crdb-v1 for files, crdb-v1-tty for stderr
//       exit-on-error: <bool> # whether to terminate upon a write error
//                             # default true for file+stderr sinks
//       auditable: <bool>     # if true, activates sink-specific features
//                             # that enhance non-repudiability.
//                             # also implies exit-on-error: true.
//
package logconfig
