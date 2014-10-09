# Copyright 2014 The Cockroach Authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http:#www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied.  See the License for the specific language governing
# permissions and limitations under the License. See the AUTHORS file
# for names of contributors.
#
# Author: Spencer Kimball (spencer.kimball@gmail.com)
# Author: Andrew Bonventre (andybons@gmail.com)

ROACH_LIB := libroach.a
SOURCES   := compaction.cc merge.cc
OBJECTS   := $(SOURCES:.cc=.o)

CXXFLAGS += -std=c++11 -I../proto/lib -I../_vendor/rocksdb/include

all: static_lib

static_lib: $(ROACH_LIB)

$(ROACH_LIB): $(OBJECTS)
	rm -f $@
	ar -rsv $(ROACH_LIB) $(OBJECTS)

.cc.o:
	$(CXX) $(CXXFLAGS) -c $< -o $@

clean:
	rm -f $(OBJECTS) $(ROACH_LIB)
