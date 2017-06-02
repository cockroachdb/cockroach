# Copyright 2016 The Cockroach Authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied. See the License for the specific language governing
# permissions and limitations under the License. See the AUTHORS file
# for names of contributors.

# ADDR will be something like postgres://localhost:26257/company_activerecord.
# We need to change the protocol name to cockroachdb so Rails uses our adapter.
URL := $(subst postgresql,cockroachdb,$(ADDR))

.PHONY: start
start:
	@DATABASE_URL="$(URL)" bin/bundle exec rake db:migrate
	@DATABASE_URL="$(URL)" PORT=6543 bin/bundle exec rails server

.PHONY: deps
deps:
	@gem install bundler
	@bin/bundle
