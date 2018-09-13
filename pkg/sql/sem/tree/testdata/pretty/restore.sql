RESTORE bank.customers
FROM 'gs://acme-co-backup/database-bank-2017-03-27-weekly', 'gs://acme-co-backup/database-bank-2017-03-28-nightly', 'gs://acme-co-backup/database-bank-2017-03-29-nightly'
AS OF SYSTEM TIME '2017-02-28 10:00:00'
WITH into_db = 'newdb'
