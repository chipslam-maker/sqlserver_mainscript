ALTER PROCEDURE dbo.sp_CompareTables_CompositeKey
    @SourceDB NVARCHAR(128),
    @TargetDB NVARCHAR(128),
    @TableName NVARCHAR(128),
    @PKColumns NVARCHAR(MAX),  -- 例如 'ItemID,SRC'
    @IgnoreColumns NVARCHAR(MAX)
AS
BEGIN
    SET NOCOUNT ON;
    
    DECLARE @ColumnList TABLE (ColName NVARCHAR(128));
    DECLARE @PKList TABLE (PKName NVARCHAR(128));
    DECLARE @DynamicSQL NVARCHAR(MAX) = '';
    
    -- 1. 解析 PK 列表 (確保去掉多餘空格)
    DECLARE @PKXML XML = CAST('<x>' + REPLACE(@PKColumns, ',', '</x><x>') + '</x>' AS XML);
    INSERT INTO @PKList 
    SELECT LTRIM(RTRIM(n.value('.', 'NVARCHAR(128)'))) 
    FROM @PKXML.nodes('x') AS T(n);

    -- 2. 解析 Ignore 列表
    DECLARE @IgnoreXML XML = CAST('<x>' + REPLACE(ISNULL(@IgnoreColumns,''), ',', '</x><x>') + '</x>' AS XML);
    DECLARE @IgnoreTable TABLE (ColName NVARCHAR(128));
    INSERT INTO @IgnoreTable 
    SELECT LTRIM(RTRIM(n.value('.', 'NVARCHAR(128)'))) 
    FROM @IgnoreXML.nodes('x') AS T(n);

    -- 3. 抓取一般欄位
    INSERT INTO @ColumnList
    SELECT name FROM sys.columns WHERE object_id = OBJECT_ID(@TableName)
    AND name NOT IN (SELECT PKName FROM @PKList)
    AND name NOT IN (SELECT ColName FROM @IgnoreTable);

    -- 4. 構建動態 JOIN 條件與 PK 資訊顯示
    DECLARE @JoinClause NVARCHAR(MAX) = '';
    DECLARE @PKInfoClause NVARCHAR(MAX) = '';

    -- 這裡使用了更安全的方式來串接字串，避免結尾出現多餘符號
    SELECT @JoinClause = @JoinClause + 'a.' + QUOTENAME(PKName) + ' = b.' + QUOTENAME(PKName) + ' AND '
    FROM @PKList;
    SET @JoinClause = LEFT(@JoinClause, LEN(@JoinClause) - 4); -- 移除最後的 ' AND '

    -- 構建 PK 資訊，例如 'ID: 1 | SRC: A'
    SELECT @PKInfoClause = @PKInfoClause + '''' + PKName + ': '' + CAST(ISNULL(a.' + QUOTENAME(PKName) + ', b.' + QUOTENAME(PKName) + ') AS NVARCHAR(MAX)) + '' | '' + '
    FROM @PKList;
    SET @PKInfoClause = LEFT(@PKInfoClause, LEN(@PKInfoClause) - 7); -- 移除最後的 ' | ' +

    -- 5. 處理「只有 A 有」或「只有 B 有」
    DECLARE @FirstPK NVARCHAR(128);
    SELECT TOP 1 @FirstPK = PKName FROM @PKList;

    SET @DynamicSQL = '
    INSERT INTO dbo.ComparisonReport (TableName, DiffType, PrimaryKeyInfo, ColumnName, ValueInA, ValueInB)
    SELECT 
        ''' + @TableName + ''',
        CASE WHEN a.' + QUOTENAME(@FirstPK) + ' IS NULL THEN ''Only in B'' ELSE ''Only in A'' END,
        ' + @PKInfoClause + ',
        ''[Row Presence]'', NULL, NULL
    FROM ' + QUOTENAME(@SourceDB) + '.dbo.' + QUOTENAME(@TableName) + ' a
    FULL OUTER JOIN ' + QUOTENAME(@TargetDB) + '.dbo.' + QUOTENAME(@TableName) + ' b ON ' + @JoinClause + '
    WHERE a.' + QUOTENAME(@FirstPK) + ' IS NULL OR b.' + QUOTENAME(@FirstPK) + ' IS NULL;'

    -- 6. 逐一欄位比對
    DECLARE @ColName NVARCHAR(128);
    DECLARE col_cursor CURSOR FOR SELECT ColName FROM @ColumnList;
    OPEN col_cursor;
    FETCH NEXT FROM col_cursor INTO @ColName;

    WHILE @@FETCH_STATUS = 0
    BEGIN
        SET @DynamicSQL += '
        INSERT INTO dbo.ComparisonReport (TableName, DiffType, PrimaryKeyInfo, ColumnName, ValueInA, ValueInB)
        SELECT 
            ''' + @TableName + ''',
            ''Data Mismatch'',
            ' + @PKInfoClause + ',
            ''' + @ColName + ''',
            ISNULL(CAST(a.' + QUOTENAME(@ColName) + ' AS NVARCHAR(MAX)), ''[[NULL]]''),
            ISNULL(CAST(b.' + QUOTENAME(@ColName) + ' AS NVARCHAR(MAX)), ''[[NULL]]'')
        FROM ' + QUOTENAME(@SourceDB) + '.dbo.' + QUOTENAME(@TableName) + ' a
        INNER JOIN ' + QUOTENAME(@TargetDB) + '.dbo.' + QUOTENAME(@TableName) + ' b ON ' + @JoinClause + '
        WHERE EXISTS (
            SELECT a.' + QUOTENAME(@ColName) + ' COLLATE DATABASE_DEFAULT 
            EXCEPT 
            SELECT b.' + QUOTENAME(@ColName) + ' COLLATE DATABASE_DEFAULT
        );'

        FETCH NEXT FROM col_cursor INTO @ColName;
    END

    CLOSE col_cursor;
    DEALLOCATE col_cursor;

    -- 執行前先 PRINT 出來，你可以 Copy PRINT 出來的結果去手動 Run，看哪裡報錯
    -- PRINT @DynamicSQL; 
    EXEC sp_executesql @DynamicSQL;
END
