-- ============================================
-- 搜尋所有DB中對指定TABLE有INSERT/MERGE的SP
-- ============================================

DECLARE @TargetTable NVARCHAR(255) = 'YourTableName'  -- ← 改這裡輸入你的TABLE名

DECLARE @SQL NVARCHAR(MAX)
DECLARE @Results TABLE (
    DatabaseName    NVARCHAR(255),
    SchemaName      NVARCHAR(255),
    SPName          NVARCHAR(255),
    OperationType   NVARCHAR(50),
    ObjectType      NVARCHAR(50),
    CreateDate      DATETIME,
    ModifyDate      DATETIME
)

DECLARE @DBName NVARCHAR(255)
DECLARE db_cursor CURSOR FOR
    SELECT name 
    FROM sys.databases
    WHERE state_desc = 'ONLINE'
      AND name NOT IN ('master', 'tempdb', 'model', 'msdb')  -- 跳過系統DB，如需搜尋可移除

OPEN db_cursor
FETCH NEXT FROM db_cursor INTO @DBName

WHILE @@FETCH_STATUS = 0
BEGIN
    SET @SQL = N'
    USE [' + @DBName + N']
    
    SELECT 
        DB_NAME()                           AS DatabaseName,
        SCHEMA_NAME(o.schema_id)            AS SchemaName,
        o.name                              AS SPName,
        CASE 
            WHEN UPPER(m.definition) LIKE ''%INSERT%INTO%'' + UPPER(@tbl) + ''%'' 
             AND UPPER(m.definition) LIKE ''%MERGE%'' + UPPER(@tbl) + ''%''
                THEN ''INSERT + MERGE''
            WHEN UPPER(m.definition) LIKE ''%MERGE%'' + UPPER(@tbl) + ''%''
                THEN ''MERGE''
            WHEN UPPER(m.definition) LIKE ''%INSERT%INTO%'' + UPPER(@tbl) + ''%''
                THEN ''INSERT INTO''
            ELSE ''INSERT (other)''
        END                                 AS OperationType,
        o.type_desc                         AS ObjectType,
        o.create_date                       AS CreateDate,
        o.modify_date                       AS ModifyDate
    FROM sys.sql_modules m
    JOIN sys.objects o ON m.object_id = o.object_id
    WHERE o.type IN (''P'', ''TR'', ''V'', ''FN'', ''TF'', ''IF'')  -- SP, Trigger, View, Function
      AND (
            UPPER(m.definition) LIKE ''%INSERT%INTO%'' + UPPER(@tbl) + ''%''
         OR UPPER(m.definition) LIKE ''%INSERT%'' + UPPER(@tbl) + ''%''
         OR UPPER(m.definition) LIKE ''%MERGE%'' + UPPER(@tbl) + ''%''
      )
    '

    INSERT INTO @Results
    EXEC sp_executesql @SQL, N'@tbl NVARCHAR(255)', @tbl = @TargetTable

    FETCH NEXT FROM db_cursor INTO @DBName
END

CLOSE db_cursor
DEALLOCATE db_cursor

-- =====================
-- 顯示結果
-- =====================
SELECT 
    DatabaseName,
    SchemaName,
    SPName,
    OperationType,
    ObjectType,
    CreateDate,
    ModifyDate
FROM @Results
ORDER BY DatabaseName, OperationType, SPName

-- 顯示總數
SELECT 
    DatabaseName,
    OperationType,
    COUNT(*) AS Count
FROM @Results
GROUP BY DatabaseName, OperationType
ORDER BY DatabaseName, OperationType
