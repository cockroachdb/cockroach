kadmin.local -q "addprinc -pw changeme tester@MY.EX"
echo changeme | kinit tester@MY.EX
krb5kdc -n