HiveUDF
=======
A SET OF UDFs for Hive Datawarehouse 

To use it
CREATE TEMPORARY FUNCTION SetContain AS 'hiveudf.SetContain'
ADD FILE "dic.txt"
select * from table_key where SetContain(key,'dic.txt')
