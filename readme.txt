
start pos应该选table map之前的点，用于过滤表，否则会跳过后面的事务，去查找下一个事务的table map 。

对于没有主键的表需要小心，生成的update，delete，可能的回滚sql，一个sql可能满足多行的条件，导致修改多行数据

usage：
go run main.go -host 127.0.0.1 -port 20996 -user msandbox -passwd msandbox -tables test.user -start-file mysql-bin.000100 -start-pos 331 -end-pos 759  -flashback true
