# Import the driver.
import psycopg2
import sys
import time

if len(sys.argv) < 6:
        print >>sys.stderr, """usage: %s <host> <port> <username> <testdb> <testtable> <count>
Generate many ranges in a CockroachDB cluster.

This tool will create a test database and table if they do not exist yet,
then populate the table ensuring that each row lands in its own range.
It does this by issuing ALTER TABLE ... SPLIT AT after every row inserted.

If the test table already exists, then new rows/ranges are appended to it.

Example:

  %s localhost 26257 root splittest foo 1000
""" % (sys.argv[0], sys.argv[0])
        sys.exit(1)

host = sys.argv[1]
port = int(sys.argv[2])
user = sys.argv[3]
testdb = sys.argv[4]
testtable = sys.argv[5]
count = int(sys.argv[6])

# Connect to the cluster.
conn = psycopg2.connect(database=testdb, user=user, host=host, port=port)

def onestmt(conn, sql):
        with conn:
                with conn.cursor() as cur:
                        cur.execute(sql)

onestmt(conn, 'create database if not exists %s' % testdb)
tname = '%s.%s' % (testdb, testtable)
#onestmt(conn, 'drop table if exists %s' % tname)
onestmt(conn, 'create table if not exists %s(x int primary key)' % tname)

first = 0
with conn:
        with conn.cursor() as cur:
                cur.execute("select max(x) from %s" % tname)
                rows = cur.fetchall()
                if len(rows) > 0:
                        val = rows[0][0]
                        if val is not None:
                                first = val + 1

def o(m):
        sys.stdout.write(m)

startt = time.time()
nrows = 0
nsplits = 0
print "First value: %d" % first
for i in xrange(first, first+count):
        o('.')
        try:
                onestmt(conn, "insert into %s(x) values (%d)" % (tname, i))
                nrows += 1
        except Exception, e:
                o('!')
                print
                print e
        o(',')
        try:
                onestmt(conn, "alter table %s split at (%d)" % (tname, i))
                nsplits += 1
        except Exception, e:
                o('!')
                print
                print e
                
        if i%50 == 49:
                print i
        sys.stdout.flush()
endt = time.time()
        
print
dur = endt - startt
print "%.2f rows/s, %.2f splits/s" % (float(nrows)/dur, float(nsplits)/dur)
conn.close()
