package dbworkloadgo

import (
	"fmt"
	"os"
	"testing"
)

// tweak these paths as needed
var (
	testZipDir     = "/Users/pradyumagarwal/workloads/git-dbworkload/dbworkload_intProj/debug-zips/debug_2"
	testDBName     = "tpcc"
	testOutputFile = "test_schema.ddl"
)

// examples can be customized to stress ParseDDL
var examples = []string{
	// warehouse table
	`CREATE TABLE public.warehouse (
        w_id INT8 NOT NULL,
        w_name VARCHAR(10) NOT NULL,
        w_street_1 VARCHAR(20) NOT NULL,
        w_street_2 VARCHAR(20) NOT NULL,
        w_city VARCHAR(20) NOT NULL,
        w_state CHAR(2) NOT NULL,
        w_zip CHAR(9) NOT NULL,
        w_tax DECIMAL(4,4) NOT NULL,
        w_ytd DECIMAL(12,2) NOT NULL,
        CONSTRAINT warehouse_pkey PRIMARY KEY (w_id ASC)
    )`,

	// district table
	`CREATE TABLE public.district (
        d_id INT8 NOT NULL,
        d_w_id INT8 NOT NULL,
        d_name VARCHAR(10) NOT NULL,
        d_street_1 VARCHAR(20) NOT NULL,
        d_street_2 VARCHAR(20) NOT NULL,
        d_city VARCHAR(20) NOT NULL,
        d_state CHAR(2) NOT NULL,
        d_zip CHAR(9) NOT NULL,
        d_tax DECIMAL(4,4) NOT NULL,
        d_ytd DECIMAL(12,2) NOT NULL,
        d_next_o_id INT8 NOT NULL,
        CONSTRAINT district_pkey PRIMARY KEY (d_w_id ASC, d_id ASC),
        CONSTRAINT district_d_w_id_fkey FOREIGN KEY (d_w_id) REFERENCES public.warehouse(w_id) NOT VALID
    )`,

	// customer table
	`CREATE TABLE public.customer (
        c_id INT8 NOT NULL,
        c_d_id INT8 NOT NULL,
        c_w_id INT8 NOT NULL,
        c_first VARCHAR(16) NOT NULL,
        c_middle CHAR(2) NOT NULL,
        c_last VARCHAR(16) NOT NULL,
        c_street_1 VARCHAR(20) NOT NULL,
        c_street_2 VARCHAR(20) NOT NULL,
        c_city VARCHAR(20) NOT NULL,
        c_state CHAR(2) NOT NULL,
        c_zip CHAR(9) NOT NULL,
        c_phone CHAR(16) NOT NULL,
        c_since TIMESTAMP NOT NULL,
        c_credit CHAR(2) NOT NULL,
        c_credit_lim DECIMAL(12,2) NOT NULL,
        c_discount DECIMAL(4,4) NOT NULL,
        c_balance DECIMAL(12,2) NOT NULL,
        c_ytd_payment DECIMAL(12,2) NOT NULL,
        c_payment_cnt INT8 NOT NULL,
        c_delivery_cnt INT8 NOT NULL,
        c_data VARCHAR(500) NOT NULL,
        CONSTRAINT customer_pkey PRIMARY KEY (c_w_id ASC, c_d_id ASC, c_id ASC),
        CONSTRAINT customer_c_w_id_c_d_id_fkey FOREIGN KEY (c_w_id, c_d_id)
            REFERENCES public.district(d_w_id, d_id) NOT VALID,
        INDEX customer_idx (c_w_id ASC, c_d_id ASC, c_last ASC, c_first ASC)
    )`,

	// history table
	`CREATE TABLE public.history (
        rowid UUID NOT NULL DEFAULT gen_random_uuid(),
        h_c_id INT8 NOT NULL,
        h_c_d_id INT8 NOT NULL,
        h_c_w_id INT8 NOT NULL,
        h_d_id INT8 NOT NULL,
        h_w_id INT8 NOT NULL,
        h_date TIMESTAMP NULL,
        h_amount DECIMAL(6,2) NULL,
        h_data VARCHAR(24) NULL,
        CONSTRAINT history_pkey PRIMARY KEY (h_w_id ASC, rowid ASC),
        CONSTRAINT history_h_c_w_id_h_c_d_id_h_c_id_fkey FOREIGN KEY (h_c_w_id, h_c_d_id, h_c_id)
            REFERENCES public.customer(c_w_id, c_d_id, c_id) NOT VALID,
        CONSTRAINT history_h_w_id_h_d_id_fkey FOREIGN KEY (h_w_id, h_d_id)
            REFERENCES public.district(d_w_id, d_id) NOT VALID
    )`,

	// "order" table
	`CREATE TABLE public."order" (
        o_id INT8 NOT NULL,
        o_d_id INT8 NOT NULL,
        o_w_id INT8 NOT NULL,
        o_c_id INT8 NULL,
        o_entry_d TIMESTAMP NULL,
        o_carrier_id INT8 NULL,
        o_ol_cnt INT8 NULL,
        o_all_local INT8 NULL,
        CONSTRAINT order_pkey PRIMARY KEY (o_w_id ASC, o_d_id ASC, o_id DESC),
        CONSTRAINT order_o_w_id_o_d_id_o_c_id_fkey FOREIGN KEY (o_w_id, o_d_id, o_c_id)
            REFERENCES public.customer(c_w_id, c_d_id, c_id) NOT VALID,
        UNIQUE INDEX order_idx (o_w_id ASC, o_d_id ASC, o_c_id ASC, o_id DESC)
            STORING (o_entry_d, o_carrier_id)
    )`,

	// new_order table
	`CREATE TABLE public.new_order (
        no_o_id INT8 NOT NULL,
        no_d_id INT8 NOT NULL,
        no_w_id INT8 NOT NULL,
        CONSTRAINT new_order_pkey PRIMARY KEY (no_w_id ASC, no_d_id ASC, no_o_id ASC),
        CONSTRAINT new_order_no_w_id_no_d_id_no_o_id_fkey FOREIGN KEY (no_w_id, no_d_id, no_o_id)
            REFERENCES public."order"(o_w_id, o_d_id, o_id) NOT VALID
    )`,

	// item table
	`CREATE TABLE public.item (
        i_id INT8 NOT NULL,
        i_im_id INT8 NULL,
        i_name VARCHAR(24) NULL,
        i_price DECIMAL(5,2) NULL,
        i_data VARCHAR(50) NULL,
        CONSTRAINT item_pkey PRIMARY KEY (i_id ASC)
    )`,

	// stock table
	`CREATE TABLE public.stock (
        s_i_id INT8 NOT NULL,
        s_w_id INT8 NOT NULL,
        s_quantity INT8 NULL,
        s_dist_01 CHAR(24) NULL,
        s_dist_02 CHAR(24) NULL,
        s_dist_03 CHAR(24) NULL,
        s_dist_04 CHAR(24) NULL,
        s_dist_05 CHAR(24) NULL,
        s_dist_06 CHAR(24) NULL,
        s_dist_07 CHAR(24) NULL,
        s_dist_08 CHAR(24) NULL,
        s_dist_09 CHAR(24) NULL,
        s_dist_10 CHAR(24) NULL,
        s_ytd INT8 NULL,
        s_order_cnt INT8 NULL,
        s_remote_cnt INT8 NULL,
        s_data VARCHAR(50) NULL,
        CONSTRAINT stock_pkey PRIMARY KEY (s_w_id ASC, s_i_id ASC),
        CONSTRAINT stock_s_w_id_fkey FOREIGN KEY (s_w_id) REFERENCES public.warehouse(w_id) NOT VALID,
        CONSTRAINT stock_s_i_id_fkey FOREIGN KEY (s_i_id) REFERENCES public.item(i_id) NOT VALID
    )`,

	// order_line table
	`CREATE TABLE public.order_line (
        ol_o_id INT8 NOT NULL,
        ol_d_id INT8 NOT NULL,
        ol_w_id INT8 NOT NULL,
        ol_number INT8 NOT NULL,
        ol_i_id INT8 NOT NULL,
        ol_supply_w_id INT8 NULL,
        ol_delivery_d TIMESTAMP NULL,
        ol_quantity INT8 NULL,
        ol_amount DECIMAL(6,2) NULL,
        ol_dist_info CHAR(24) NULL,
        CONSTRAINT order_line_pkey PRIMARY KEY (ol_w_id ASC, ol_d_id ASC, ol_o_id DESC, ol_number ASC),
        CONSTRAINT order_line_ol_w_id_ol_d_id_ol_o_id_fkey FOREIGN KEY (ol_w_id, ol_d_id, ol_o_id)
            REFERENCES public."order"(o_w_id, o_d_id, o_id) NOT VALID,
        CONSTRAINT order_line_ol_supply_w_id_ol_i_id_fkey FOREIGN KEY (ol_supply_w_id, ol_i_id)
            REFERENCES public.stock(s_w_id, s_i_id) NOT VALID
    )`,
}

func TestGenerateDDLs(t *testing.T) {
	schemas, err := GenerateDDLs(testZipDir, testDBName, testZipDir, testOutputFile, false)
	if err != nil {
		t.Fatalf("GenerateDDLs failed: %v", err)
	}
	f, err := os.Create("generate_ddls_output.txt")
	if err != nil {
		t.Fatalf("could not create output file: %v", err)
	}
	defer f.Close()
	for name, s := range schemas {
		fmt.Fprintf(f, "--- %s ---\n%s\n", name, s.String())
	}
}

func TestParseDDL(t *testing.T) {
	out, err := os.Create("parse_ddl_output.txt")
	if err != nil {
		t.Fatalf("could not create output file: %v", err)
	}
	defer out.Close()
	for _, ddl := range examples {
		fmt.Fprintf(out, "IN : %s\n", ddl)
		sch, err := ParseDDL(ddl)
		if err != nil {
			fmt.Fprintf(out, "[ERROR] %v\n\n", err)
			continue
		}
		fmt.Fprintln(out, sch.String())
		fmt.Fprintln(out, "----")
	}
}
