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

package ledger

const (
	ledgerCustomerSchema = `(
		id                          INT           NOT NULL PRIMARY KEY DEFAULT unique_rowid(),
		identifier                  STRING(255)   NOT NULL,
		"name"                      STRING(255)       NULL,
		currency_code               STRING(3)     NOT NULL,
		is_system_customer          BOOL          NOT NULL,
		is_active                   BOOL          NOT NULL,
		created                     TIMESTAMP     NOT NULL DEFAULT clock_timestamp(),
		balance                     DECIMAL(20,3) NOT NULL DEFAULT 0:::DECIMAL,
		credit_limit                DECIMAL(20,3)     NULL,
		sequence_number             INT               NULL DEFAULT (-1):::INT,

		INDEX        customer_identifier_active_idx (identifier ASC, is_active ASC),
		UNIQUE INDEX customer_ide_cur_ia_idx        (identifier ASC, currency_code ASC, is_active ASC)
	)`
	ledgerTransactionSchema = `(
		external_id                STRING(255) NOT NULL PRIMARY KEY,
		tcomment                   STRING(255)     NULL,
		context                    STRING(255)     NULL,
		transaction_type_reference INT         NOT NULL,
		username                   STRING(255)     NULL,
		created_ts                 TIMESTAMP   NOT NULL,
		systimestamp               TIMESTAMP   NOT NULL DEFAULT clock_timestamp(),
		reversed_by                STRING(255)     NULL,
		response                   BYTES           NULL
	)`
	ledgerEntrySchema = `(
		id             INT           NOT NULL PRIMARY KEY DEFAULT unique_rowid(),
		amount         DECIMAL(20,3)     NULL,
		customer_id    INT               NULL,
		transaction_id STRING(255)   NOT NULL,
		system_amount  DECIMAL(24,7)     NULL,
		created_ts     TIMESTAMP     NOT NULL,
		money_type     STRING(1)     NOT NULL
	)`
	ledgerSessionSchema = `(
		session_id       STRING                   NOT NULL PRIMARY KEY,
		expiry_timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
		data             STRING                   NOT NULL,
		last_update      TIMESTAMP                NOT NULL
	)`
)
