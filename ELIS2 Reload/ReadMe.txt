In PECDW
--------
1. Login as ECISDRDM or DBA and disable the indexes
   @disable_bnft_fact_bnft_curr_fact_ndx.sql

2. Login as PC_ELIS2 in SQL*Developer and delete existing ELIS2 records.  This step could take upto an hour.  If needed, use the /*+ PARALLEL 16 */ hint.

   DELETE FROM ECISDRDM.BNFT_FACT WHERE SRC_SYS_ID=1;  Commit;
   DELETE FROM ECISDRDM.BNFT_CURR_FACT WHERE SRC_SYS_ID=1;  Commit;
   --Truncate command
   exec ecisdrdm.p_truncate_table(‘STG_E2_BNFT_FACT_ARCH’);
   exec ecisdrdm.p_truncate_table(‘BNFT_DLY_SNPSHT’);


3. Kick off the ETL load process.  This step takes upto 6 - 8 hours

4. Rebuild the indexes in parallel

bnft_fact_ndx_rebuild_1.sql
bnft_fact_ndx_rebuild_2.sql
bnft_fact_ndx_rebuild_3.sql
bnft_curr_fact_ndx_rebuild_1.sql
bnft_curr_fact_ndx_rebuild_2.sql


Troubleshooting/Monitoring queries
----------------------------------
select count(distinct USCIS_RCPT_NBR) from ecisdrdm.stg_e2_bnft_fact;
select count(*) from ecisdrdm.stg_e2_bnft_fact;

--Created this index to help with Step 3, but that did not help
CREATE BITMAP INDEX ECISDRDM.stge2bnftfact_uscisrcpt_BMX ON ecisdrdm.stg_e2_bnft_fact (USCIS_RCPT_NBR) 
STORAGE (INITIAL 4m NEXT 4m) NOLOGGING TABLESPACE ECISDRDM_INDEX PARALLEL 4 COMPUTE STATISTICS;

select * from gv$session where username = 'PC_ELIS2';
select * from gv$session_longops where totalwork - sofar > 10;
select count(*) FROM ECISDRDM.BNFT_DLY_SNPSHT;

select count(*) FROM ECISDRDM.BNFT_FACT WHERE SRC_SYS_ID=1;  --20651253  --1339360
select count(*) FROM ECISDRDM.BNFT_CURR_fACT  WHERE SRC_SYS_ID=1;

select * from gv$sqlarea where sql_id = '61kygmha7776g';
select * from gv$session where sql_id = '8p1jy9wak371z';
select * from table(dbms_xplan.display_awr('8p1jy9wak371z'));
select used_ublk from gv$transaction;


exec dbms_metadata.set_transform_param(dbms_metadata.SESSION_TRANSFORM,'SQLTERMINATOR',TRUE);
select dbms_metadata.get_ddl('INDEX',index_name,owner) from dba_indexes where table_name in ('BNFT_FACT', 'BNFT_CURR_FACT') order by table_name, index_type;

select * from dba_indexes where table_name in ('BNFT_FACT', 'BNFT_CURR_FACT') order by table_name, index_type;
select * from dba_ind_partitions where table_name in ('BNFT_FACT', 'BNFT_CURR_FACT') ;

select distinct index_name from SYS.DBA_IND_PARTITIONS where status != 'USABLE' and index_name like 'BNFT_%' order by index_name;
select index_name, status, count(*) from SYS.DBA_IND_PARTITIONS where  index_name like 'BNFT%' group by index_name, status order by index_name;

select dip.index_name, dip.status, count(*) from dba_part_indexes dpi, dba_ind_partitions dip 
where dpi.table_name in ('BNFT_FACT','BNFT_CURR_FACT')
and dpi.index_name = dip.index_name
and dip.status != 'USABLE'
group by dip.index_name, dip.status
order by dip.index_name;

select 'Alter index '||dip.index_owner||'.'||dip.index_name||' rebuild partition '||dip.partition_name||' compute statistics;'
from dba_part_indexes dpi, dba_ind_partitions dip 
where dpi.table_name in ('BNFT_FACT','BNFT_CURR_FACT')
and dpi.index_name = dip.index_name
and dip.status != 'USABLE'
order by dip.index_name;
