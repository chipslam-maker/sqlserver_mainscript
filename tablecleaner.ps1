<#
.SYNOPSIS
    Performs data cleaning and table renaming operations for a SQL Server table.
    It uses SMO with TrustServerCertificate=True for secure, flexible connection.

.DESCRIPTION
    This script reads configuration from Config.json, utilizes SMO to script the table structure,
    and executes a safe T-SQL transaction:
    1. Creates a structurally identical temporary table.
    2. Inserts retained data (e.g., last 100 days), excluding computed columns.
    3. Renames the original table to backup ([OriginalTable]_OLD).
    4. Renames the temporary table to the new original table.
    5. Performs structural and data integrity checks.

.NOTES
    - Requires the SqlServer PowerShell module to be installed.
    - Assumes Windows Authentication is used for the connection.
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

# Validate required parameters
if (-not $SqlServer -or -not $Database -or -not $OriginalTable -or -not $DateColumnName) {
    Write-Error "Missing required configuration values (Server, Database, Table, or DateColumnName) in Config.json."
    exit 1
}

# Define temporary names
$TempTable = $OriginalTable + "_TEMP" 
$OldTable = $OriginalTable + "_OLD"   

Write-Host "Target Table: [$Schema].[$OriginalTable]" -ForegroundColor Cyan
Write-Host "Retention Period: $RetentionDays days (based on column '$DateColumnName')" -ForegroundColor Cyan


# ===============================================
# 1. Check and Load SMO Library (Required for Structure Scripting AND Validation)
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
# 2. Connect (with TrustServerCertificate) and Script Structure
# ===============================================
Write-Host "Connecting to [$SqlServer] (TrustServerCertificate=True)..." -ForegroundColor Cyan

try {
    # Establish ServerConnection object for advanced connection settings
    $sc = New-Object Microsoft.SqlServer.Management.Common.ServerConnection($SqlServer)
    
    # --- Set TrustServerCertificate properties ---
    $sc.Encrypt = $true  
    $sc.TrustServerCertificate = $true 
    $sc.LoginSecure = $true # Use Windows Authentication
    # --- End TrustServerCertificate properties ---

    # Create SMO Server object and test connection
    $srv = New-Object Microsoft.SqlServer.Management.SMO.Server($sc)
    $srv.ConnectionContext.Connect()
    
    if (-not $srv.ConnectionContext.IsConnected) {
        throw "Failed to establish connection to $SqlServer."
    }

    $db = $srv.Databases[$Database]
    $OriginalTableSMO = $db.Tables | Where-Object { $_.Name -eq $OriginalTable -and $_.Schema -eq $Schema }

    if (-not $OriginalTableSMO) {
        throw "Could not find table [$Schema].[$OriginalTable] in database [$Database]"
    }

    # --- Scripting Options and CREATE TABLE Script Generation ---
    $ScriptOptions = New-Object Microsoft.SqlServer.Management.SMO.ScriptingOptions
    $ScriptOptions.DriAllConstraints = $true 
    $ScriptOptions.Indexes = $true           
    $ScriptOptions.NoCommandTerminator = $true
    $ScriptOptions.SchemaQualify = $true
    $ScriptOptions.Permissions = $false

    $CreateTableScriptCollection = $OriginalTableSMO.Script($ScriptOptions)
    $CreateTableScript = ($CreateTableScriptCollection -join "`r`n")

    # Replace the original table name with the temporary name for the creation script
    $CreateTableScript = $CreateTableScript -replace "\[$Schema\]\.\[$OriginalTable\]", "[$Schema].[" + $OriginalTable + "_TEMP]"
    $CreateTableScript = $CreateTableScript -replace "$Schema\.$OriginalTable", "$Schema." + $OriginalTable + "_TEMP"

    # --- Dynamically Generate INSERT Column List (Excluding Computed Columns) ---
    $DataColumns = @()
    foreach ($col in $OriginalTableSMO.Columns) {
        if (-not $col.IsComputed) {
            $DataColumns += "[$($col.Name)]"
        }
    }
    $ColumnList = $DataColumns -join ", "
    
    if (-not $ColumnList) {
        throw "Table contains no insertable data columns."
    }

}
catch {
    Write-Error "SMO Connection or Scripting Error: $($_.Exception.Message)"
    exit 1
}


# ===============================================
# 3. Compile and Execute T-SQL Script (Cleanup & Rename)
# ===============================================
Write-Host "Executing T-SQL cleanup and rename script..." -ForegroundColor Green

# The T-SQL uses $OriginalTable + "_TEMP" for the creation, matching $CreateTableScript's content.
$SQLCommand = @"
USE [$Database];

-- Start Transaction (ensures atomicity: all or nothing)
BEGIN TRANSACTION;
BEGIN TRY

    -- 1. Drop the old temporary table (if it exists)
    IF OBJECT_ID('[$Schema].[$OriginalTable' + '_TEMP' + ']') IS NOT NULL
        DROP TABLE [$Schema].[$OriginalTable' + '_TEMP' + '];

    -- 2. Execute the auto-generated CREATE TABLE script 
    $CreateTableScript

    -- 3. Insert $RetentionDays days of data into the temporary table
    DECLARE @RetentionDate DATETIME = DATEADD(day, -$RetentionDays, GETDATE());

    INSERT INTO [$Schema].[$OriginalTable' + '_TEMP' + '] ($ColumnList) WITH (TABLOCK)
    SELECT $ColumnList
    FROM [$Schema].[$OriginalTable]
    WHERE [$DateColumnName] >= @RetentionDate;

    -- 4. Rename: ORG -> OLD
    IF OBJECT_ID('[$Schema].[$OriginalTable]') IS NOT NULL
    BEGIN
        EXEC sp_rename '[$Schema].[$OriginalTable]', '$OriginalTable' + '_TEMP_NAME', 'OBJECT';
        EXEC sp_rename '[$Schema].' + '$OriginalTable' + '_TEMP_NAME', '$OldTable', 'OBJECT';
    END
    ELSE
    BEGIN
        THROW 50000, 'Original table does not exist!', 1;
    END

    -- 5. Rename: TEMP -> ORG
    IF OBJECT_ID('[$Schema].[$OriginalTable' + '_TEMP' + ']') IS NOT NULL
        EXEC sp_rename '[$Schema].[$OriginalTable' + '_TEMP' + ']', '$OriginalTable', 'OBJECT';
    ELSE
    BEGIN
        THROW 50000, 'Temporary table does not exist!', 1;
    END

    -- Commit transaction
    COMMIT TRANSACTION;

    SELECT 'Data cleanup completed successfully!' AS Status;

END TRY
BEGIN CATCH
    IF @@TRANCOUNT > 0
        ROLLBACK TRANSACTION;
    THROW;
END CATCH
GO
"@

# Execute the SQL script
try {
    Invoke-Sqlcmd -ServerInstance $SqlServer -Database $Database -Query $SQLCommand -ErrorAction Stop | Out-Null
    Write-Host "T-SQL Cleanup Phase: Completed." -ForegroundColor Green
}
catch {
    Write-Error "T-SQL Execution Error: $($_.Exception.Message)"
    Write-Host "Transaction rolled back. Database state remains unchanged." -ForegroundColor Red
    exit 1
}

# ===============================================
# 4. Verification and Comparison Phase
# ===============================================
Write-Host ""
Write-Host "==============================================" -ForegroundColor DarkGreen
Write-Host "4. Starting Verification Checks..." -ForegroundColor DarkGreen
Write-Host "==============================================" -ForegroundColor DarkGreen

# Re-fetch SMO objects after rename
# Disconnect and re-establish connection to refresh SMO objects (best practice after rename)
$srv.ConnectionContext.Disconnect()
$srv.ConnectionContext.Connect()
$NewTableSMO = $db.Tables | Where-Object { $_.Name -eq $OriginalTable -and $_.Schema -eq $Schema }
$OldTableSMO = $db.Tables | Where-Object { $_.Name -eq $OldTable -and $_.Schema -eq $Schema }

if (-not $NewTableSMO -or -not $OldTableSMO) {
    Write-Error "Verification failed: Could not locate New or Old tables after rename."
    exit 1
}

### 4.1 Structure, Computed Columns, and Indexes Comparison (SMO Metadata)

# Function to get key structural properties from a table object
function Get-TableStructureObject {
    param(
        [Parameter(Mandatory=$true)]
        [Microsoft.SqlServer.Management.SMO.Table]$Table
    )
    
    # Get Column Properties (Structure, Computed status, Data Type)
    $Columns = $Table.Columns | Select Name, DataType, IsComputed, IsPersisted, Formula -ExpandProperty DataType
    
    # Get Index Properties (Index Name, Is Unique, Columns)
    $Indexes = @()
    foreach ($index in $Table.Indexes) {
        $IndexColumns = $index.IndexedColumns | Select Name, IsIncluded, IsDescending
        $Indexes += [PSCustomObject]@{
            Name = $index.Name
            IsUnique = $index.IsUnique
            IndexColumns = $IndexColumns
        }
    }
    
    return [PSCustomObject]@{
        TableName = $Table.Name
        Columns = $Columns
        Indexes = $Indexes
    }
}

$NewStruct = Get-TableStructureObject $NewTableSMO
$OldStruct = Get-TableStructureObject $OldTableSMO

# 1. Compare Columns (Structure & Computed Columns)
Write-Host "  -> 1. Comparing Column Structure and Computed Columns..."
$ColCompare = Compare-Object $NewStruct.Columns $OldStruct.Columns -Property Name, DataType, IsComputed, IsPersisted, Formula -IncludeEqual
$ColDiff = $ColCompare | Where-Object { $_.SideIndicator -ne '==' }
if ($ColDiff.Count -eq 0) {
    Write-Host "     ✅ Column Structure (including Computed Columns) matches." -ForegroundColor Green
} else {
    Write-Host "     ❌ Column Structure Mismatch found:" -ForegroundColor Red
    $ColDiff | Format-List
}

# 2. Compare Indexes
Write-Host "  -> 2. Comparing Indexes..."
$IndexCompare = Compare-Object $NewStruct.Indexes $OldStruct.Indexes -Property Name, IsUnique -IncludeEqual
$IndexDiff = $IndexCompare | Where-Object { $_.SideIndicator -ne '==' }
if ($IndexDiff.Count -eq 0) {
    Write-Host "     ✅ Index Names and Uniqueness matches." -ForegroundColor Green
} else {
    Write-Host "     ❌ Index Mismatch found:" -ForegroundColor Red
    $IndexDiff | Format-List
}


### 4.2 Data Integrity Check (T-SQL)

Write-Host "  -> 3. Verifying Data Retention in new table (Last $RetentionDays days)..."

$DataCheckSQL = @"
    SELECT 
        MIN([$DateColumnName]) AS MinDate, 
        MAX([$DateColumnName]) AS MaxDate,
        DATEDIFF(day, MIN([$DateColumnName]), GETDATE()) AS DaysSinceMinDate
    FROM [$Schema].[$OriginalTable]
"@

$DataResult = Invoke-Sqlcmd -ServerInstance $SqlServer -Database $Database -Query $DataCheckSQL -ErrorAction Stop

$MinDate = $DataResult.MinDate
$DaysSinceMinDate = $DataResult.DaysSinceMinDate

# Allow for a small buffer (e.g., +/- 2 days) for safety margin
if (($DaysSinceMinDate -le $RetentionDays + 2) -and ($DaysSinceMinDate -ge $RetentionDays - 2)) {
    Write-Host "     ✅ Data Range check passed. Min Date: $MinDate ($DaysSinceMinDate days ago)." -ForegroundColor Green
} elseif ($DaysSinceMinDate -gt $RetentionDays + 2) {
    Write-Host "     ⚠️ Data Range check failed (Too much data). Min Date: $MinDate ($DaysSinceMinDate days ago)." -ForegroundColor Yellow
} else {
    Write-Host "     ⚠️ Data Range check failed (Not enough data). Min Date: $MinDate ($DaysSinceMinDate days ago)." -ForegroundColor Yellow
}

Write-Host ""
Write-Host "==============================================" -ForegroundColor Green
Write-Host "✅ ALL OPERATIONS AND VERIFICATIONS COMPLETED." -ForegroundColor Green
Write-Host "New Table: [$Schema].[$OriginalTable]" -ForegroundColor Green
Write-Host "Old Backup: [$Schema].[$OldTable]" -ForegroundColor Green
Write-Host "==============================================" -ForegroundColor Green
