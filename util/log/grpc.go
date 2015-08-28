// Copyright 2016 The Cockroach Authors.
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
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Tamir Duberstein (tamird@gmail.com)

package log

import "google.golang.org/grpc/grpclog"

func init() {
	grpclog.SetLogger(logger{})
}

type logger struct{}

func (logger) Fatal(args ...interface{}) {
	logDepth(nil, 2, FatalLog, "", args)
}

func (logger) Fatalf(format string, args ...interface{}) {
	logDepth(nil, 2, FatalLog, format, args)
}

func (logger) Fatalln(args ...interface{}) {
	logDepth(nil, 2, FatalLog, "", args)
}

func (logger) Print(args ...interface{}) {
	logDepth(nil, 2, InfoLog, "", args)
}

func (logger) Printf(format string, args ...interface{}) {
	logDepth(nil, 2, InfoLog, format, args)
}

func (logger) Println(args ...interface{}) {
	logDepth(nil, 2, InfoLog, "", args)
}
