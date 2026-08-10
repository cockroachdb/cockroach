// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package fktxn

// Test schemas used across multiple test files. Each schema is a semicolon-
// separated string of DDL statements that can be passed to
// discoverSchemaFromDDL.

const simplePairDDL = `
CREATE TABLE a (id INT PRIMARY KEY);
CREATE TABLE b (id INT PRIMARY KEY, a_id INT NOT NULL REFERENCES a(id))`

const fanOutDDL = `
CREATE TABLE a (id INT PRIMARY KEY);
CREATE TABLE b (id INT PRIMARY KEY, a_id INT NOT NULL REFERENCES a(id));
CREATE TABLE c (id INT PRIMARY KEY, a_id INT NOT NULL REFERENCES a(id))`

const chainDDL = `
CREATE TABLE a (id INT PRIMARY KEY);
CREATE TABLE b (id INT PRIMARY KEY, a_id INT NOT NULL REFERENCES a(id));
CREATE TABLE c (id INT PRIMARY KEY, b_id INT NOT NULL REFERENCES b(id));
CREATE TABLE d (id INT PRIMARY KEY, c_id INT NOT NULL REFERENCES c(id))`

// fanOutWithSeparateChildDDL is a fan-out where d references both b and c via
// separate non-overlapping columns. This does NOT form a single FKGraph — b-d
// and c-d land in separate graphs.
const fanOutWithSeparateChildDDL = `
CREATE TABLE a (id INT PRIMARY KEY);
CREATE TABLE b (id INT PRIMARY KEY, a_id INT NOT NULL REFERENCES a(id));
CREATE TABLE c (id INT PRIMARY KEY, a_id INT NOT NULL REFERENCES a(id));
CREATE TABLE d (id INT PRIMARY KEY, b_id INT NOT NULL REFERENCES b(id), c_id INT NOT NULL REFERENCES c(id))`

// diamondCompositeDDL uses composite keys with org_id threaded through all
// tables so all FK edges land in a single FKGraph.
const diamondCompositeDDL = `
CREATE TABLE orgs (org_id INT PRIMARY KEY);
CREATE TABLE depts (
	org_id INT NOT NULL REFERENCES orgs(org_id),
	dept_id INT NOT NULL,
	PRIMARY KEY (org_id, dept_id));
CREATE TABLE teams (
	org_id INT NOT NULL REFERENCES orgs(org_id),
	team_id INT NOT NULL,
	PRIMARY KEY (org_id, team_id));
CREATE TABLE projects (
	id INT PRIMARY KEY,
	org_id INT NOT NULL,
	dept_id INT NOT NULL,
	team_id INT NOT NULL,
	FOREIGN KEY (org_id, dept_id) REFERENCES depts(org_id, dept_id),
	FOREIGN KEY (org_id, team_id) REFERENCES teams(org_id, team_id))`

// diamondDDL is a diamond with single-column PKs. dept_id is threaded through
// composite UCs so all FK edges overlap into a single FKGraph.
const diamondDDL = `
CREATE TABLE departments (dept_id INT PRIMARY KEY);
CREATE TABLE managers (
	manager_id INT PRIMARY KEY,
	dept_id INT NOT NULL REFERENCES departments(dept_id),
	UNIQUE (dept_id, manager_id));
CREATE TABLE teams (
	team_id INT PRIMARY KEY,
	dept_id INT NOT NULL REFERENCES departments(dept_id),
	UNIQUE (dept_id, team_id));
CREATE TABLE projects (
	project_id INT PRIMARY KEY,
	dept_id INT NOT NULL,
	manager_id INT NOT NULL,
	team_id INT NOT NULL,
	FOREIGN KEY (dept_id, manager_id) REFERENCES managers(dept_id, manager_id),
	FOREIGN KEY (dept_id, team_id) REFERENCES teams(dept_id, team_id))`

const selfRefDDL = `
CREATE TABLE employees (id INT PRIMARY KEY, manager_id INT REFERENCES employees(id))`

const transitiveOverlapDDL = `
CREATE TABLE countries (country_code STRING PRIMARY KEY);
CREATE TABLE regions (
	country_code STRING NOT NULL REFERENCES countries(country_code),
	region_name STRING NOT NULL,
	PRIMARY KEY (country_code, region_name));
CREATE TABLE stores (
	id INT PRIMARY KEY,
	country_code STRING NOT NULL,
	region_name STRING NOT NULL,
	FOREIGN KEY (country_code, region_name) REFERENCES regions(country_code, region_name))`

const nonTransitiveDDL = `
CREATE TABLE countries (country_code STRING PRIMARY KEY);
CREATE TABLE regions (
	country_code STRING NOT NULL REFERENCES countries(country_code),
	region_name STRING NOT NULL,
	PRIMARY KEY (country_code, region_name));
CREATE TABLE stores (
	store_id INT PRIMARY KEY,
	country_code STRING NOT NULL,
	region_name STRING NOT NULL,
	FOREIGN KEY (country_code, region_name) REFERENCES regions(country_code, region_name));
CREATE TABLE inventory (id INT PRIMARY KEY, store_id INT NOT NULL REFERENCES stores(store_id))`

const deepTransitiveChainDDL = `
CREATE TABLE l1 (a INT PRIMARY KEY);
CREATE TABLE l2 (
	a INT NOT NULL REFERENCES l1(a),
	b INT NOT NULL,
	PRIMARY KEY (a, b));
CREATE TABLE l3 (
	a INT NOT NULL,
	b INT NOT NULL,
	c INT NOT NULL,
	PRIMARY KEY (a, b, c),
	FOREIGN KEY (a, b) REFERENCES l2(a, b));
CREATE TABLE l4 (
	a INT NOT NULL,
	b INT NOT NULL,
	c INT NOT NULL,
	d INT NOT NULL,
	PRIMARY KEY (a, b, c, d),
	FOREIGN KEY (a, b, c) REFERENCES l3(a, b, c))`

const fanInCompositeDDL = `
CREATE TABLE tenants (tenant_id INT PRIMARY KEY);
CREATE TABLE users (
	tenant_id INT NOT NULL REFERENCES tenants(tenant_id),
	user_id INT NOT NULL,
	PRIMARY KEY (tenant_id, user_id));
CREATE TABLE posts (
	id INT PRIMARY KEY,
	tenant_id INT NOT NULL,
	user_id INT NOT NULL,
	FOREIGN KEY (tenant_id, user_id) REFERENCES users(tenant_id, user_id));
CREATE TABLE comments (
	id INT PRIMARY KEY,
	tenant_id INT NOT NULL,
	user_id INT NOT NULL,
	FOREIGN KEY (tenant_id, user_id) REFERENCES users(tenant_id, user_id))`

const mixedOverlapAndIsolatedDDL = `
CREATE TABLE tenants (tenant_id INT PRIMARY KEY);
CREATE TABLE users (
	tenant_id INT NOT NULL REFERENCES tenants(tenant_id),
	user_id INT NOT NULL,
	PRIMARY KEY (tenant_id, user_id));
CREATE TABLE posts (
	id INT PRIMARY KEY,
	tenant_id INT NOT NULL,
	user_id INT NOT NULL,
	FOREIGN KEY (tenant_id, user_id) REFERENCES users(tenant_id, user_id));
CREATE TABLE configs (id INT PRIMARY KEY);
CREATE TABLE settings (id INT PRIMARY KEY, config_id INT NOT NULL REFERENCES configs(id))`

const noFKsDDL = `
CREATE TABLE standalone1 (id INT PRIMARY KEY);
CREATE TABLE standalone2 (id INT PRIMARY KEY)`

const twoDisconnectedFKsDDL = `
CREATE TABLE a (id INT PRIMARY KEY);
CREATE TABLE b (id INT PRIMARY KEY, a_id INT NOT NULL REFERENCES a(id));
CREATE TABLE c (id INT PRIMARY KEY);
CREATE TABLE d (id INT PRIMARY KEY, c_id INT NOT NULL REFERENCES c(id))`

// fanOut3DDL is a fan-out with 3 children referencing the same parent.
const fanOut3DDL = `
CREATE TABLE a (id INT PRIMARY KEY);
CREATE TABLE b (id INT PRIMARY KEY, a_id INT NOT NULL REFERENCES a(id));
CREATE TABLE c (id INT PRIMARY KEY, a_id INT NOT NULL REFERENCES a(id));
CREATE TABLE d (id INT PRIMARY KEY, a_id INT NOT NULL REFERENCES a(id))`
