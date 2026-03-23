BACKUP
DATABASE bank INTO 'gs://acme-co-backup'
AS OF SYSTEM TIME '-10s'
WITH revision_history = true
