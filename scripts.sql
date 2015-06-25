/*
Staging
osql -E -S localhost
*/
USE ResearchNavigator
GO
EXEC sys.sp_cdc_enable_db
GO

USE IACUC
GO
EXEC sys.sp_cdc_enable_db
GO

/*

*/
EXECUTE sys.sp_cdc_enable_table
@source_schema = N'dbo',
@role_name = N'sysamin', 
@source_name = N'_Project',
@supports_net_changes = 0,
@captured_column_list = N'oid, status',
@capture_instance = N'dbo__Project'
GO

/*
  cdc template to use
*/
	DECLARE @begin_time datetime, @end_time datetime, @from_lsn binary(10), @to_lsn binary(10);
         -- Obtain the beginning of the time interval.
         -- SET @begin_time = GETDATE() -1;
         SET @begin_time = dateadd(ss, 1, '1970-01-01') 
         -- Obtain the end of the time interval.
         SET @end_time = GETDATE();
         -- Map the time interval to a change data capture query range.
         SET @from_lsn = sys.fn_cdc_map_time_to_lsn('smallest greater than or equal', @begin_time);
         SET @to_lsn = sys.fn_cdc_map_time_to_lsn('largest less than or equal', @end_time);
         -- Return the net changes occurring within the query window.
         select _uid, start_time, oid, status from (
         select y._uid, left(convert(varchar(24), y.start_time, 126), 19) start_time, y.oid, y.status from ( select max(sys.fn_cdc_map_lsn_to_time ( __$start_lsn )) max_start_time, b._uid FROM cdc.fn_cdc_get_all_changes_dbo__Project(@from_lsn, @to_lsn, 'all') a inner join [__Research Project] b on a.oid = b.oid group by b._uid ) z 
         inner join
         ( select sys.fn_cdc_map_lsn_to_time ( __$start_lsn ) start_time, a.__$start_lsn, a.__$seqval, a.__$operation, a.__$update_mask, convert(varchar(max), a.oid, 2) oid, convert(varchar(max), a.status, 2) status, b._uid FROM cdc.fn_cdc_get_all_changes_dbo__Project(@from_lsn, @to_lsn, 'all') a inner join [__Research Project] b on a.oid = b.oid inner join [__Research Project_CustomAttributesManager] rpc on rpc.oid = b.customAttributes inner join [__StudyDetails] sd on sd.oid = rpc.studyDetails inner join [__StudyDetails_CustomAttributesManager] sdm on sd.customAttributes = sdm.oid inner join [__Study Subject Type] sst on sst.oid = sdm.subjectType inner join [__Study Subject Type_CustomAttributesManager] sstc on sstc.oid = sst.customAttributes where a.status is not null and sstc.name = 'Animal' ) y 
         on z._uid = y._uid and z.max_start_time = y.start_time 
         ) x
         group by _uid, start_time, oid, status 
         order by x.start_time;

