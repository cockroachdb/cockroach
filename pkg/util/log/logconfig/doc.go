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
//     --log=<channel(s)>:<sink>:<parameters...>
//
// Examples:
//
//     --log=DEV:file:dir=/tmp,max-file-size=10MB
//     --log=DEV,STORAGE:file:dir=/tmp,max-file-size=10MB
//     --log=DEV,STORAGE:file:dir=/tmp,max-file-size=10MB
//
package logconfig
