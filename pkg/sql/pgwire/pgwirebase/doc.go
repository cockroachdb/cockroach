// Copyright 2018 The Cockroach Authors.
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

/*
Package pgwirebase contains type definitions and very basic protocol structures
to be used by both the pgwire package and by others (particularly by the sql
package). The contents of this package have been extracted from the pgwire
package for the implementation of the COPY IN protocol, which lives in sql.
*/
package pgwirebase
