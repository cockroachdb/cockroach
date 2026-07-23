<?php

function kill($msg) {
	echo($msg);
	exit(1);
}

$dbconn = pg_connect('')
	or kill('Could not connect: ' . pg_last_error());
$result = pg_query_params('SELECT 1, 2 > $1, $1', [intval($argv[1])])
	or kill('Query failed: ' . pg_last_error());
$arr = pg_fetch_row($result);
($arr === ['1', 'f', '3']) or kill('Unexpected: ' . print_r($arr, true));

$dbh = new PDO('pgsql:','root', null, array(PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION));
$dbh->exec('CREATE database bank');
$dbh->exec('CREATE table bank.accounts (id INT PRIMARY KEY, balance INT)');
$dbh->exec('INSERT INTO bank.accounts (id, balance) VALUES (1, 1000), (2, 250)');
$dbh->beginTransaction();
$stmt = $dbh->prepare('UPDATE bank.accounts SET balance = balance + :deposit WHERE id=:account');
$stmt->execute(array('account' => 1, 'deposit' => 10));
$stmt->execute(array('account' => 2, 'deposit' => -10));
$dbh->commit();

// Regression test for #59007.
$stmt = $dbh->prepare("insert into a_table (id, a) select ?, ?, ?, ? returning id");
$stmt->bindValue(1, 'ed66e7c0-5c39-11eb-8992-89bd28f48e75');
$stmt->bindValue(2, 'bugging_a');
$stmt->bindValue(3, 'bugging_b');
try {
  $stmt->execute();
  assert(false, "expected exception in execute");
} catch (Exception $e) {
  assert(strpos($e.getMessage(), "expected 4 arguments, got 3"));
}
