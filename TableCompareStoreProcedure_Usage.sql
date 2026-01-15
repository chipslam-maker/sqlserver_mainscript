-- 1. 清空舊報告
TRUNCATE TABLE dbo.ComparisonReport;

-- 2. 依序填入需要比對的 Table
-- 格式: EXEC sp_CompareTables_DeepDive '來源DB', '目標DB', '資料表', 'PK欄位', '忽略欄位'
EXEC dbo.sp_CompareTables_DeepDive 'MyDB_A', 'MyDB_B', 'Users', 'UserID', 'LastUpdate, LoginTime';
EXEC dbo.sp_CompareTables_DeepDive 'MyDB_A', 'MyDB_B', 'Orders', 'OrderID', 'StatusDate';
EXEC dbo.sp_CompareTables_DeepDive 'MyDB_A', 'MyDB_B', 'Products', 'SKU', 'IsActive'; -- Boolean(BIT) 也沒問題

-- 3. 查看所有差異報告
SELECT * FROM dbo.ComparisonReport 
ORDER BY TableName, DiffType, PrimaryKeyInfo;


CREATE TABLE dbo.ComparisonReport (
    ReportID INT IDENTITY(1,1) PRIMARY KEY,
    TableName NVARCHAR(128),
    DiffType NVARCHAR(50),      -- 'Only in A', 'Only in B', 'Data Mismatch'
    PrimaryKeyInfo NVARCHAR(MAX), -- 顯示該行的 PK 值 (例如 ID: 105)
    ColumnName NVARCHAR(128),    -- 發生不一致的欄位名
    ValueInA NVARCHAR(MAX),     -- A 資料庫的值
    ValueInB NVARCHAR(MAX),     -- B 資料庫的值
    CheckTime DATETIME DEFAULT GETDATE()
);
