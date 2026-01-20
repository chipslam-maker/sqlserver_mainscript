EXEC [dbo].[usp_CompareTablesAndLog] 
    @SourceServer = 'SRV-PROD', 
    @SourceDB = 'SalesDB', 
    @TargetServer = 'SRV-BACKUP', 
    @TargetDB = 'SalesDB_Archive', 
    @TableName = 'Orders', 
    @IgnoreCols = 'RowVersion,LastSyncDate';
