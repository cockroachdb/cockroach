./cockroach demo
BACKUP TO 'userfile:///foo';

# get the right timestamp from show backups in, and then actually see the backup
# directory to make sure it looks good
SHOW BACKUP 'userfile:///foo'; 

# make sure w/o  option we  error out
SET DATABASE = system;
ALTER DATABASE movr RENAME TO movr_old;

# should work fine
RESTORE DATABASE  movr FROM 'userfile:///foo';

# will not work because now there is already a db named movr!
RESTORE DATABASE  movr FROM 'userfile:///foo';              

# test the new functionality
RESTORE DATABASE  movr FROM 'userfile:///foo' WITH new_db_name='movr_new';              

# trying to restore table as sanity check
RESTORE TABLE  movr.rides FROM 'userfile:///foo/2021/09/14-154712.00' WITH into_db='movr_old';

