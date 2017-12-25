/**
 * 
 * @author mg
 * @name asterisk_schema
 */
select * from table1, table2, #entities/sub-entity t0
where table2.fielda < table1.f1 and :p2 = table1.f3 and :p3 = t0.amount