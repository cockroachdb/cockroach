BACKUP
DATABASE bank TO 'gs://acme-co-backup/database-bank-2017-03-29-nightly'
AS OF SYSTEM TIME '-10s'
INCREMENTAL FROM 'gs://acme-co-backup/database-bank-2017-03-27-weekly', 'gs://acme-co-backup/database-bank-2017-03-28-nightly'
WITH revision_history
