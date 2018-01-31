package main

import (
	"database/sql"
	"fmt"
)

var ddls = [...]struct {
	ddl        string
	interleave string
	index      bool
}{
	{
		ddl: `
create table warehouse (
  w_id        integer   not null primary key,
  w_name      varchar(10),
  w_street_1  varchar(20),
  w_street_2  varchar(20),
  w_city      varchar(20),
  w_state     char(2),
  w_zip       char(9),
  w_tax       decimal(4,4),
  w_ytd       decimal(12,2)
)`}, {
		// 10 districts per warehouse
		ddl: `
create table district (
  d_id         integer       not null,
  d_w_id       integer       not null,
  d_name       varchar(10),
  d_street_1   varchar(20),
  d_street_2   varchar(20),
  d_city       varchar(20),
  d_state      char(2),
  d_zip        char(9),
  d_tax        decimal(4,4),
  d_ytd        decimal(12,2),
  d_next_o_id  integer,
  primary key (d_w_id, d_id)
)`,
		interleave: `interleave in parent warehouse (d_w_id)`,
	},
	{
		ddl: `
create table customer (
  c_id           integer        not null,
  c_d_id         integer        not null,
  c_w_id         integer        not null,
  c_first        varchar(16),
  c_middle       char(2),
  c_last         varchar(16),
  c_street_1     varchar(20),
  c_street_2     varchar(20),
  c_city         varchar(20),
  c_state        char(2),
  c_zip          char(9),
  c_phone        char(16),
  c_since        timestamp,
  c_credit       char(2),
  c_credit_lim   decimal(12,2),
  c_discount     decimal(4,4),
  c_balance      decimal(12,2),
  c_ytd_payment  decimal(12,2),
  c_payment_cnt  integer,
  c_delivery_cnt integer,
  c_data         varchar(500),
  primary key (c_w_id, c_d_id, c_id)
)`,
		interleave: `interleave in parent district (c_w_id, c_d_id)`,
	},
	{
		// No PK necessary for this table.
		ddl: `
create table history (
  h_c_id   integer,
  h_c_d_id integer,
  h_c_w_id integer,
  h_d_id   integer,
  h_w_id   integer,
  h_date   timestamp,
  h_amount decimal(6,2),
  h_data   varchar(24)
)`,
	},
	{
		ddl: `
create table "order" (
  o_id         integer      not null,
  o_d_id       integer      not null,
  o_w_id       integer      not null,
  o_c_id       integer,
  o_entry_d    timestamp,
  o_carrier_id integer,
  o_ol_cnt     integer,
  o_all_local  integer,
  primary key (o_w_id, o_d_id, o_id DESC)
)`,

		interleave: `interleave in parent district (o_w_id, o_d_id)`,
	},
	{
		ddl: `
create table new_order (
  no_o_id  integer   not null,
  no_d_id  integer   not null,
  no_w_id  integer   not null,
  primary key (no_w_id, no_d_id, no_o_id)
)`,
		// This natural-seeming interleave makes performance worse, because this
		// table has a ton of churn and produces a lot of MVCC tombstones, which
		// then will gum up the works of scans over the parent table.
		// interleave: `interleave in parent "order" (no_w_id, no_d_id, no_o_id)`,
	},
	{
		ddl: `create table item (
  i_id     integer      not null,
  i_im_id  integer,
  i_name   varchar(24),
  i_price  decimal(5,2),
  i_data   varchar(50),
  primary key (i_id)
)`,
	},
	{
		ddl: `
create table stock (
  s_i_id       integer       not null,
  s_w_id       integer       not null,
  s_quantity   integer,
  s_dist_01    char(24),
  s_dist_02    char(24),
  s_dist_03    char(24),
  s_dist_04    char(24),
  s_dist_05    char(24),
  s_dist_06    char(24),
  s_dist_07    char(24),
  s_dist_08    char(24),
  s_dist_09    char(24),
  s_dist_10    char(24),
  s_ytd        integer,
  s_order_cnt  integer,
  s_remote_cnt integer,
  s_data       varchar(50),
  primary key (s_w_id, s_i_id)
)`,
		interleave: `interleave in parent warehouse (s_w_id)`,
	},
	{
		ddl: `
create table order_line (
  ol_o_id         integer   not null,
  ol_d_id         integer   not null,
  ol_w_id         integer   not null,
  ol_number       integer   not null,
  ol_i_id         integer   not null,
  ol_supply_w_id  integer,
  ol_delivery_d   timestamp,
  ol_quantity     integer,
  ol_amount       decimal(6,2),
  ol_dist_info    char(24),
  primary key (ol_w_id, ol_d_id, ol_o_id DESC, ol_number)
)`,
		interleave: `interleave in parent "order" (ol_w_id, ol_d_id, ol_o_id)`,
	},
	{
		ddl:        `create index customer_idx on customer (c_w_id, c_d_id, c_last, c_first)`,
		interleave: `interleave in parent district (c_w_id, c_d_id)`,
		index:      true,
	},
	{
		ddl:        `create unique index order_idx on "order" (o_w_id, o_d_id, o_carrier_id, o_id)`,
		interleave: `interleave in parent district (o_w_id, o_d_id)`,
		index:      true,
	},
	{
		ddl:   `alter table district add foreign key (d_w_id) references warehouse (w_id)`,
		index: true,
	},
	{
		ddl:   `alter table customer add foreign key (c_w_id, c_d_id) references district (d_w_id, d_id)`,
		index: true,
	},
	{
		ddl:   `create index on history(h_w_id, h_d_id)`,
		index: true,
	},
	{
		ddl:   `create index on history(h_c_w_id, h_c_d_id, h_c_id)`,
		index: true,
	},
	{
		ddl:   `alter table history add foreign key (h_c_w_id, h_c_d_id, h_c_id) references customer (c_w_id, c_d_id, c_id)`,
		index: true,
	},
	{
		ddl:   `alter table history add foreign key (h_w_id, h_d_id) references district (d_w_id, d_id)`,
		index: true,
	},
	{
		ddl:   `create index on "order"(o_w_id, o_d_id, o_c_id)`,
		index: true,
	},
	{
		ddl:   `alter table "order" add foreign key (o_w_id, o_d_id, o_c_id) references customer (c_w_id, c_d_id, c_id)`,
		index: true,
	},
	{
		ddl:   `alter table stock add foreign key (s_w_id) references warehouse (w_id)`,
		index: true,
	},
	{
		ddl:   `create index on stock(s_i_id)`,
		index: true,
	},
	{
		ddl:   `alter table stock add foreign key (s_i_id) references item (i_id)`,
		index: true,
	},
	{
		ddl:   `alter table order_line add foreign key (ol_w_id, ol_d_id, ol_o_id) references "order" (o_w_id, o_d_id, o_id)`,
		index: true,
	},
	{
		ddl:   `create index order_line_fk on order_line(ol_supply_w_id, ol_d_id)`,
		index: true,
	},
	{
		ddl:   `alter table order_line add foreign key (ol_supply_w_id, ol_d_id) references stock (s_w_id, s_i_id)`,
		index: true,
	},
}

// loadSchema loads the entire TPCC schema into the database.
func loadSchema(db *sql.DB, interleave bool, index bool) {
	schemaType := "tables"
	if index {
		schemaType = "indexes"
	}
	for i, stmt := range ddls {
		sql := stmt.ddl
		if index != stmt.index {
			continue
		}
		if interleave {
			sql = sql + stmt.interleave
		}
		if _, err := db.Exec(sql); err != nil {
			panic(fmt.Sprintf("Couldn't exec %s: %s\n", sql, err))
		}
		fmt.Printf("Created %d %s\r", i+1, schemaType)
	}
	fmt.Printf("\n")
}
