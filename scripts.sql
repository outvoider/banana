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

