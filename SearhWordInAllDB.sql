DECLARE @SearchText NVARCHAR(100) = '%Update%YourTableName%YourColumnName%'; -- 這裡輸入關鍵字
DECLARE @Command NVARCHAR(MAX);

SET @Command = '
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = ''?'' AND state_desc = ''ONLINE'' AND is_read_only = 0)
BEGIN
    USE [?];
    SELECT 
        DB_NAME() AS DatabaseName,
        SCHEMA_NAME(o.schema_id) AS SchemaName,
        o.name AS ObjectName,
        o.type_desc AS ObjectType,
        m.definition AS FullCode
    FROM sys.sql_modules m
    INNER JOIN sys.objects o ON m.object_id = o.object_id
    WHERE m.definition LIKE ''' + @SearchText + '''
      AND o.type = ''P''; -- 只看 Stored Procedures
END';

-- 執行跨資料庫搜尋
EXEC sp_MSforeachdb @Command;
