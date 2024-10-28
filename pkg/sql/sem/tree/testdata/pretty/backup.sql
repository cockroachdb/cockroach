BACKUP
DATABASE bank INTO 'gs://acme-co-backup'
AS OF SYSTEM TIME '-10s'
WITH incremental_location = 'gs://acme-co-backup/incrementals',
revision_history = true
