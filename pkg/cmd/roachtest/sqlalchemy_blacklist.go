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
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.

package main

var sqlalchemyBlacklists = blacklistsForVersion{
	{"sqlalchemy/a v19.1", "sqlAlchemyABlackList19_1", sqlAlchemyABlackList19_1, "", nil},
	{"sqlalchemy/b v19.1", "sqlAlchemyBBlackList19_1", sqlAlchemyBBlackList19_1, "", nil},
	{"sqlalchemy/c v19.1", "sqlAlchemyCBlackList19_1", sqlAlchemyCBlackList19_1, "", nil},
	{"sqlalchemy/d v19.1", "sqlAlchemyDBlackList19_1", sqlAlchemyDBlackList19_1, "", nil},
	{"sqlalchemy/e v19.1", "sqlAlchemyEBlackList19_1", sqlAlchemyEBlackList19_1, "", nil},
	{"sqlalchemy/f v19.1", "sqlAlchemyFBlackList19_1", sqlAlchemyFBlackList19_1, "", nil},
	{"sqlalchemy/g v19.1", "sqlAlchemyGBlackList19_1", sqlAlchemyGBlackList19_1, "", nil},
	{"sqlalchemy/h v19.1", "sqlAlchemyHBlackList19_1", sqlAlchemyHBlackList19_1, "", nil},
	{"sqlalchemy/i v19.1", "sqlAlchemyIBlackList19_1", sqlAlchemyIBlackList19_1, "", nil},
	{"sqlalchemy/j v19.1", "sqlAlchemyJBlackList19_1", sqlAlchemyJBlackList19_1, "", nil},
	{"sqlalchemy/k v19.1", "sqlAlchemyKBlackList19_1", sqlAlchemyKBlackList19_1, "", nil},
	{"sqlalchemy/l v19.1", "sqlAlchemyLBlackList19_1", sqlAlchemyLBlackList19_1, "", nil},
	{"sqlalchemy/m v19.1", "sqlAlchemyMBlackList19_1", sqlAlchemyMBlackList19_1, "", nil},
	{"sqlalchemy/n v19.1", "sqlAlchemyNBlackList19_1", sqlAlchemyNBlackList19_1, "", nil},
	{"sqlalchemy/o v19.1", "sqlAlchemyOBlackList19_1", sqlAlchemyOBlackList19_1, "", nil},
	{"sqlalchemy/p v19.1", "sqlAlchemyPBlackList19_1", sqlAlchemyPBlackList19_1, "", nil},
	{"sqlalchemy/q v19.1", "sqlAlchemyQBlackList19_1", sqlAlchemyQBlackList19_1, "", nil},
	{"sqlalchemy/r v19.1", "sqlAlchemyRBlackList19_1", sqlAlchemyRBlackList19_1, "", nil},
	{"sqlalchemy/s v19.1", "sqlAlchemySBlackList19_1", sqlAlchemySBlackList19_1, "", nil},
	{"sqlalchemy/t v19.1", "sqlAlchemyTBlackList19_1", sqlAlchemyTBlackList19_1, "", nil},
	{"sqlalchemy/u v19.1", "sqlAlchemyUBlackList19_1", sqlAlchemyUBlackList19_1, "", nil},
	{"sqlalchemy/v v19.1", "sqlAlchemyVBlackList19_1", sqlAlchemyVBlackList19_1, "", nil},
	{"sqlalchemy/w v19.1", "sqlAlchemyWBlackList19_1", sqlAlchemyWBlackList19_1, "", nil},
	{"sqlalchemy/x v19.1", "sqlAlchemyXBlackList19_1", sqlAlchemyXBlackList19_1, "", nil},
	{"sqlalchemy/y v19.1", "sqlAlchemyYBlackList19_1", sqlAlchemyYBlackList19_1, "", nil},
	{"sqlalchemy/z v19.1", "sqlAlchemyZBlackList19_1", sqlAlchemyZBlackList19_1, "", nil},
}

// Please keep these lists alphabetized for easy diffing.
// After a failed run, an updated version of this blacklist should be available
// in the test log.
// In this blacklist, all tests have a "--x:n" appended to them. The x
// represents the testset that it was run in, the n is for when there are
// duplicates within a single testset.

var sqlAlchemyABlackList19_1 = blacklist{}
var sqlAlchemyBBlackList19_1 = blacklist{}
var sqlAlchemyCBlackList19_1 = blacklist{}
var sqlAlchemyDBlackList19_1 = blacklist{}
var sqlAlchemyEBlackList19_1 = blacklist{}
var sqlAlchemyFBlackList19_1 = blacklist{}
var sqlAlchemyGBlackList19_1 = blacklist{}
var sqlAlchemyHBlackList19_1 = blacklist{}
var sqlAlchemyIBlackList19_1 = blacklist{}
var sqlAlchemyJBlackList19_1 = blacklist{}
var sqlAlchemyKBlackList19_1 = blacklist{}
var sqlAlchemyLBlackList19_1 = blacklist{}
var sqlAlchemyMBlackList19_1 = blacklist{}
var sqlAlchemyNBlackList19_1 = blacklist{}
var sqlAlchemyOBlackList19_1 = blacklist{}
var sqlAlchemyPBlackList19_1 = blacklist{}
var sqlAlchemyQBlackList19_1 = blacklist{}
var sqlAlchemyRBlackList19_1 = blacklist{}
var sqlAlchemySBlackList19_1 = blacklist{}
var sqlAlchemyTBlackList19_1 = blacklist{}
var sqlAlchemyUBlackList19_1 = blacklist{}
var sqlAlchemyVBlackList19_1 = blacklist{}
var sqlAlchemyWBlackList19_1 = blacklist{}
var sqlAlchemyXBlackList19_1 = blacklist{}
var sqlAlchemyYBlackList19_1 = blacklist{}
var sqlAlchemyZBlackList19_1 = blacklist{}
