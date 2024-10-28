RESTORE bank.customers
FROM LATEST IN 'gs://acme-co-backup'
AS OF SYSTEM TIME '2017-02-28 10:00:00'
WITH into_db = 'newdb'
