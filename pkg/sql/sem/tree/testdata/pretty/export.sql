EXPORT INTO CSV
  'azure://acme-co/customer-export-data?AZURE_ACCOUNT_KEY=hash&AZURE_ACCOUNT_NAME=acme-co'
  FROM SELECT * FROM bank.customers WHERE id >= 100
