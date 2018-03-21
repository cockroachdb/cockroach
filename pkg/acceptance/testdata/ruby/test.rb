require 'pg'

conn = PG.connect()
res = conn.exec_params('SELECT 1, 2 > $1, $1', [ARGV[0].to_i])
raise 'Unexpected: ' + res.values.to_s unless res.values == [["1", "f", "3"]]

res = conn.exec('SELECT 1e1::decimal')
raise 'Unexpected: ' + res.values.to_s unless res.values == [["1E+1"]]
