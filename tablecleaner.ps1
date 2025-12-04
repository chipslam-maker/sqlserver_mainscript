<#
.SYNOPSIS
    Performs data cleaning and table renaming operations for a SQL Server table.
    It reads all necessary configuration parameters from a Config.json file.

.DESCRIPTION
    This script utilizes SQL Server Management Objects (SMO) to script the original table's structure,
    reads configuration from Config.json, and executes a safe T-SQL transaction:
    1. Creates a structurally identical temporary table ([OriginalTable]_TEMP).
    2. Inserts data retained from the Configured RetentionDays, excluding computed columns.
    3. Renames the original table to the backup table ([OriginalTable]_OLD).
    4. Renames the temporary table to the new original table.

.NOTES
    - Requires the SqlServer PowerShell module to be installed.
    - Requires appropriate database permissions.
#>

# ===============================================
# Parameter Configuration (Read from Config.json File)
# ===============================================

$ConfigFile = Join-Path (Split-Path $MyInvocation.MyCommand.Path) "Config.json"

if (-not (Test-Path $ConfigFile)) {
    Write-Error "Configuration file not found: $ConfigFile. Please create it in the same directory."
    exit 1
}

# Read and parse the JSON configuration file
try {
    Write-Host "Reading configuration from $ConfigFile..." -ForegroundColor Yellow
    $ConfigData = Get-Content $ConfigFile | Out-String | ConvertFrom-Json -ErrorAction Stop
}
catch {
    Write-Error "Error parsing configuration file: $($_.Exception.Message)"
    exit 1
}

# Map parameters from the configuration object
$SqlServer = $ConfigData.DatabaseConfig.SqlServer
$Database = $ConfigData.DatabaseConfig.Database
$Schema = $ConfigData.DatabaseConfig.Schema
$OriginalTable = $ConfigData.DatabaseConfig.OriginalTable
$DateColumnName = $ConfigData.DatabaseConfig.DateColumnName
$RetentionDays = $ConfigData.CleanupOptions.RetentionDays

# Validate required parameters (basic check)
if (-not $SqlServer -or -not $Database -or -not $OriginalTable -or -not $DateColumnName) {
    Write-Error "Missing required configuration values (Server, Database, Table, or DateColumnName) in Config.json."
    exit 1
}

# Define temporary names
$TempTable = $OriginalTable + "_TEMP" # Temporary table (to hold new data)
$OldTable = $OriginalTable + "_OLD"   # Old table (final backup)

Write-Host "Target Table: [$Schema].[$OriginalTable]" -ForegroundColor Cyan
Write-Host "Retention Period: $RetentionDays days (based on column '$DateColumnName')" -ForegroundColor Cyan


# ===============================================
# 1. Check and Load SMO Library
# ===============================================
try {
    Write-Host "Loading SMO assemblies..." -ForegroundColor Yellow
    # Attempt to load necessary SMO assemblies
    Add-Type -AssemblyName "Microsoft.SqlServer.Smo, Version=16.0.0.0, Culture=neutral, PublicKeyToken=89845dcd8080cc91" -ErrorAction Stop
}
catch {
    Write-Error "Could not load SMO assemblies. Ensure the SqlServer PowerShell module or SQL Server client tools are installed."
    exit 1
}

# ===============================================
# 2. Connect to Database and Script Table Structure
# ===============================================
Write-Host "Connecting to [$SqlServer].[$Database] and fetching table structure..." -ForegroundColor Cyan
try {
    # Create SMO connection objects
    $srv = New-Object Microsoft.SqlServer.Management.SMO.Server $SqlServer
    $db = $srv.Databases[$Database]
    $table = $db.Tables | Where-Object { $_.Name -eq $OriginalTable -and $_.Schema -eq $Schema }

    if (-not $table) {
        throw "Could not find table [$Schema].[$OriginalTable] in database [$Database]"
    }

    # Configure SMO scripting options (ensure all constraints and indexes are included)
    $ScriptOptions = New-Object Microsoft.SqlServer.Management.SMO.ScriptingOptions
    $ScriptOptions.DriAllConstraints = $true # Include all constraints (PK, FK, Check)
    $ScriptOptions.Indexes = $true           # Include all indexes
    $ScriptOptions.NoCommandTerminator = $true # Facilitate single SQL string transmission
    $ScriptOptions.SchemaQualify = $true     # Include Schema name
    $ScriptOptions.Permissions = $false      # Exclude permission scripts

    # Generate CREATE TABLE script (includes PK/Index/Computed Column definitions)
    $CreateTableScriptCollection = $table.Script($ScriptOptions)
    $CreateTableScript = ($CreateTableScriptCollection -join "`r`n")

    # Replace the original table name with the temporary table name in the script
    $CreateTableScript = $CreateTableScript -replace "\[$Schema\]\.\[$OriginalTable\]", "[$Schema].[$TempTable]"
    $CreateTableScript = $CreateTableScript -replace "$Schema\.$OriginalTable", "$Schema.$TempTable" # Handle unbracketed cases

    # ---------------------------------------------
    # Dynamically Generate INSERT Column List (Excluding Computed Columns)
    # ---------------------------------------------
    $DataColumns = @()
    foreach ($col in $table.Columns) {
        # Only include non-computed columns
        if (-not $col.IsComputed) {
            $DataColumns += "[$($col.Name)]"
        }
    }
    $ColumnList = $DataColumns -join ", "
    
    if (-not $ColumnList) {
        throw "Table contains no insertable data columns (All columns are computed?)"
    }

}
catch {
    Write-Error "SMO Scripting Error: $($_.Exception.Message)"
    exit 1
}


# ===============================================
# 3. Compile and Execute T-SQL Script
# ===============================================
Write-Host "Generating T-SQL script..." -ForegroundColor Cyan

# Use Here-String ( @"..."@ ) to pass multi-line SQL statements
$SQLCommand = @"
USE [$Database];

-- Start Transaction (ensures atomicity: all or nothing)
BEGIN TRANSACTION;
BEGIN TRY

    -- 1. Drop the old temporary table (if it exists)
    IF OBJECT_ID('[$Schema].[$TempTable]') IS NOT NULL
        DROP TABLE [$Schema].[$TempTable];

    -- 2. Execute the auto-generated CREATE TABLE script 
    -- (Includes all PK, Indexes, Computed Column definitions)
    $CreateTableScript

    -- 3. Insert $RetentionDays days of data into the temporary table (only non-computed columns)
    DECLARE @RetentionDate DATETIME = DATEADD(day, -$RetentionDays, GETDATE());

    -- Use TABLOCK hint for improved bulk insert performance
    INSERT INTO [$Schema].[$TempTable] ($ColumnList) WITH (TABLOCK)
    SELECT $ColumnList
    FROM [$Schema].[$OriginalTable]
    WHERE [$DateColumnName] >= @RetentionDate;

    -- 4. Rename: ORG -> OLD (Atomic rename operation)
    IF OBJECT_ID('[$Schema].[$OriginalTable]') IS NOT NULL
    BEGIN
        -- Use temp name to safely transition
        EXEC sp_rename '[$Schema].[$OriginalTable]', '$OriginalTable' + '_TEMP_NAME', 'OBJECT';
        EXEC sp_rename '[$Schema].' + '$OriginalTable' + '_TEMP_NAME', '$OldTable', 'OBJECT';
    END
    ELSE
    BEGIN
        THROW 50000, 'Original table does not exist, cannot proceed with rename!', 1;
    END

    -- 5. Rename: TEMP -> ORG
    IF OBJECT_ID('[$Schema].[$TempTable]') IS NOT NULL
        EXEC sp_rename '[$Schema].[$TempTable]', '$OriginalTable', 'OBJECT';
    ELSE
    BEGIN
        THROW 50000, 'Temporary table does not exist, cannot rename to original table!', 1;
    END

    -- Commit transaction
    COMMIT TRANSACTION;

    SELECT 'Data cleanup completed successfully!' AS Status, 
           'New Table: ' + '[$Schema].[$OriginalTable]' AS NewTable, 
           'Backup Table: ' + '[$Schema].[$OldTable]' AS BackupTable;

END TRY
BEGIN CATCH
    -- Rollback transaction on error
    IF @@TRANCOUNT > 0
        ROLLBACK TRANSACTION;

    -- Re-throw the error
    THROW;
END CATCH
GO
"@

# Execute the SQL script
Write-Host "Executing T-SQL data cleanup script..." -ForegroundColor Green
try {
    Invoke-Sqlcmd -ServerInstance $SqlServer -Database $Database -Query $SQLCommand -ErrorAction Stop
}
catch {
    Write-Error "T-SQL Execution Error: $($_.Exception.Message)"
    Write-Host "Transaction rolled back. Database state remains unchanged." -ForegroundColor Red
    exit 1
}

Write-Host "==============================================" -ForegroundColor Green
Write-Host "✅ Data cleanup and table renaming completed successfully!" -ForegroundColor Green
Write-Host "New Table (retained data): [$Schema].[$OriginalTable]" -ForegroundColor Green
Write-Host "Old Table Backup: [$Schema].[$OldTable]" -ForegroundColor Green
Write-Host "==============================================" -ForegroundColor Green
