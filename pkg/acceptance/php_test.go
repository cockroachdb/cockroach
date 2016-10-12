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
// permissions and limitations under the License.
//
// Author: Matt Jibson (mjibson@cockroachlabs.com)

package acceptance

import (
	"strings"
	"testing"
)

func TestDockerPHP(t *testing.T) {
	testDockerSuccess(t, "php", []string{"php", "-r", strings.Replace(php, "%v", "3", 1)})
	testDockerFail(t, "php", []string{"php", "-r", strings.Replace(php, "%v", `"a"`, 1)})
}

const php = `
function kill($msg) {
	echo($msg);
	exit(1);
}

$dbconn = pg_connect('')
	or kill('Could not connect: ' . pg_last_error());
$result = pg_query_params('SELECT 1, 2 > $1, $1', [%v])
	or kill('Query failed: ' . pg_last_error());
$arr = pg_fetch_row($result);
($arr === ['1', 'f', '3']) or kill('Unexpected: ' . print_r($arr, true));

$dbh = new PDO('pgsql:','', null, array(PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION));
$dbh->exec('CREATE database bank');
$dbh->exec('CREATE table bank.accounts (id INT PRIMARY KEY, balance INT)');
$dbh->exec('INSERT INTO bank.accounts (id, balance) VALUES (1, 1000), (2, 250)');
$dbh->beginTransaction();
$stmt = $dbh->prepare('UPDATE bank.accounts SET balance = balance + :deposit WHERE id=:account');
$stmt->execute(array('account' => 1, 'deposit' => 10));
$stmt->execute(array('account' => 2, 'deposit' => -10));
$dbh->commit();
`
