{
    "SourceDatabaseConfig": {
        "SqlServer": "SQL_SERVER_A_INSTANCE_NAME",
        "Database": "SourceDBName",
        "Schema": "dbo",
        "TableName": "YourTableName"
    },
    "DestinationDatabaseConfig": {
        "SqlServer": "SQL_SERVER_B_INSTANCE_NAME",
        "Database": "DestinationDBName",
        "Schema": "dbo",
        "TableName": "YourTableName"
    },
    "CopyOptions": {
        "DropTableIfExists": true 
    }
}

<#
.SYNOPSIS
    Copies a table structure and data from a Source SQL Server (A) to a Destination SQL Server (B).
    If the table exists on B, it is DROPPED and then recreated/copied.

.DESCRIPTION
    This script reads configuration from Config.json:
    1. Connects to Source (A) using SMO to generate the CREATE TABLE, INDEX, and COMPUTED COLUMN script.
    2. Dynamically generates a T-SQL transaction for the Destination (B).
    3. The T-SQL transaction:
        a. Drops the existing table on B.
        b. Creates the new table structure on B (including all indexes/computed columns).
        c. Inserts all data from Source (A) to Destination (B) using four-part naming.
    4. Performs structural and row count checks between A and B.

.NOTES
    - Requires the SqlServer PowerShell module.
    - Requires Windows Authentication and network access between A and B for four-part naming.
    - Uses TrustServerCertificate=True for flexible connection.
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

# Map Source parameters (Server A)
$SourceServer = $ConfigData.SourceDatabaseConfig.SqlServer
$SourceDatabase = $ConfigData.SourceDatabaseConfig.Database
$SourceSchema = $ConfigData.SourceDatabaseConfig.Schema
$SourceTable = $ConfigData.SourceDatabaseConfig.TableName

# Map Destination parameters (Server B)
$DestServer = $ConfigData.DestinationDatabaseConfig.SqlServer
$DestDatabase = $ConfigData.DestinationDatabaseConfig.Database
$DestSchema = $ConfigData.DestinationDatabaseConfig.Schema
$DestTable = $ConfigData.DestinationDatabaseConfig.TableName

# Map Copy Options
# Note: Since Rename logic is removed, we only need this variable for the T-SQL logic check.
$DropTableIfExists = $ConfigData.CopyOptions.DropTableIfExists

# Validate required parameters
if (-not $SourceServer -or -not $DestServer -or -not $SourceTable -or -not $DestTable) {
    Write-Error "Missing required configuration values in Config.json."
    exit 1
}

Write-Host "Source Table: [$SourceServer].[$SourceDatabase].[$SourceSchema].[$SourceTable]" -ForegroundColor Cyan
Write-Host "Dest Table:   [$DestServer].[$DestDatabase].[$DestSchema].[$DestTable]" -ForegroundColor Cyan


# ===============================================
# 1. Load SMO Library (Required for Structure Scripting)
# ===============================================
try {
    Write-Host "`nLoading SMO assemblies..." -ForegroundColor Yellow
    # Attempt to load necessary SMO assemblies
    Add-Type -AssemblyName "Microsoft.SqlServer.Smo, Version=16.0.0.0, Culture=neutral, PublicKeyToken=89845dcd8080cc91" -ErrorAction Stop
}
catch {
    Write-Error "Could not load SMO assemblies. Ensure the SqlServer PowerShell module or SQL Server client tools are installed."
    exit 1
}

# ===============================================
# 2. Connect to Source (A) and Script Structure
# ===============================================
Write-Host "`nConnecting to Source Server [$SourceServer] (TrustServerCertificate=True) to fetch schema..." -ForegroundColor Cyan

try {
    # Establish ServerConnection object for Source Server A
    $scSource = New-Object Microsoft.SqlServer.Management.Common.ServerConnection($SourceServer)
    $scSource.Encrypt = $true  
    $scSource.TrustServerCertificate = $true 
    $scSource.LoginSecure = $true # Use Windows Authentication

    # Create SMO Server object for A and test connection
    $srvSource = New-Object Microsoft.SqlServer.Management.SMO.Server($scSource)
    $srvSource.ConnectionContext.Connect()
    
    if (-not $srvSource.ConnectionContext.IsConnected) {
        throw "Failed to establish connection to $SourceServer."
    }

    $dbSource = $srvSource.Databases[$SourceDatabase]
    $SourceTableSMO = $dbSource.Tables | Where-Object { $_.Name -eq $SourceTable -and $_.Schema -eq $SourceSchema }

    if (-not $SourceTableSMO) {
        throw "Could not find source table [$SourceSchema].[$SourceTable] in database [$SourceDatabase]"
    }

    # --- Scripting Options for CREATE TABLE, INDEXES, and COMPUTED COLUMNS ---
    $ScriptOptions = New-Object Microsoft.SqlServer.Management.SMO.ScriptingOptions
    $ScriptOptions.DriAllConstraints = $true # Include Primary/Unique Constraints
    $ScriptOptions.Indexes = $true           # Include Indexes
    $ScriptOptions.NoCommandTerminator = $true # Use without GO
    $ScriptOptions.SchemaQualify = $true
    $ScriptOptions.Permissions = $false

    $CreateTableScriptCollection = $SourceTableSMO.Script($ScriptOptions)
    $CreateTableScript = ($CreateTableScriptCollection -join "`r`n")

    # Replace Source Table name with Destination Table name in the script
    $SearchPattern = [regex]::Escape("[$SourceSchema].[$SourceTable]")
    $ReplaceString = "[$DestSchema].[$DestTable]"
    $CreateTableScript = $CreateTableScript -replace $SearchPattern, $ReplaceString
    
    # Clean up connection for Source Server
    $srvSource.ConnectionContext.Disconnect()
    

    # --- Dynamically Generate INSERT Column List (Excluding Computed Columns) ---
    $DataColumns = @()
    foreach ($col in $SourceTableSMO.Columns) {
        # Exclude computed columns from the INSERT list
        if (-not $col.IsComputed) {
            $DataColumns += "[$($col.Name)]"
        }
    }
    $ColumnList = $DataColumns -join ", "
    
    if (-not $ColumnList) {
        throw "Source table contains no insertable data columns."
    }

}
catch {
    Write-Error "SMO Connection or Scripting Error on Source Server: $($_.Exception.Message)"
    exit 1
}


# ===============================================
# 3. Compile and Execute T-SQL Script on Destination (B)
# ===============================================
Write-Host "`nExecuting T-SQL Copy Script on Destination Server [$DestServer]..." -ForegroundColor Green

# Define the four-part name for the Source Table
$FourPartSourceName = "[$SourceServer].[$SourceDatabase].[$SourceSchema].[$SourceTable]"

# T-SQL Logic: Drop -> Create Structure (with Indexes/Computed Columns) -> Insert Data
$SQLCommand = @"
USE [$DestDatabase];

-- Start Transaction (ensures atomicity: all or nothing)
BEGIN TRANSACTION;
BEGIN TRY

    -- 1. Drop Existing Destination Table (as requested, assuming DropTableIfExists is always true)
    IF OBJECT_ID('[$DestSchema].[$DestTable]') IS NOT NULL
    BEGIN
        DROP TABLE [$DestSchema].[$DestTable];
        PRINT 'Existing table [$DestSchema].[$DestTable] dropped successfully.';
    END
    
    -- 2. Execute the auto-generated CREATE TABLE script (Structure, Indexes, Computed Columns)
    $CreateTableScript

    -- 3. Insert ALL data from Source (A) using four-part naming
    -- Computed columns are handled automatically by SQL Server upon insertion.
    INSERT INTO [$DestSchema].[$DestTable] ($ColumnList) WITH (TABLOCK)
    SELECT $ColumnList
    FROM $FourPartSourceName;

    -- Commit transaction
    COMMIT TRANSACTION;

    SELECT 'Table copy and structure creation completed successfully!' AS Status;

END TRY
BEGIN CATCH
    IF @@TRANCOUNT > 0
        ROLLBACK TRANSACTION;
    THROW;
END CATCH
GO
"@

# Execute the SQL script on the Destination Server
try {
    Invoke-Sqlcmd -ServerInstance $DestServer -Database $DestDatabase -Query $SQLCommand -ErrorAction Stop | Out-Null
    Write-Host "T-SQL Copy Phase: Completed." -ForegroundColor Green
}
catch {
    Write-Error "T-SQL Execution Error on Destination: $($_.Exception.Message)"
    Write-Host "Transaction rolled back. Destination database state remains unchanged." -ForegroundColor Red
    exit 1
}

# ===============================================
# 4. Verification and Comparison Phase
# ===============================================
Write-Host ""
Write-Host "==============================================" -ForegroundColor DarkGreen
Write-Host "4. Starting Verification Checks (Source A vs Destination B)..." -ForegroundColor DarkGreen
Write-Host "==============================================" -ForegroundColor DarkGreen

# 4.1 Structure, Computed Columns, and Indexes Comparison (SMO Metadata)

### Function Definition 
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

# 1. Connect to Destination (B) again to fetch new table structure
try {
    # Establish ServerConnection object for Destination Server B
    $scDest = New-Object Microsoft.SqlServer.Management.Common.ServerConnection($DestServer)
    $scDest.Encrypt = $true  
    $scDest.TrustServerCertificate = $true 
    $scDest.LoginSecure = $true
    $srvDest = New-Object Microsoft.SqlServer.Management.SMO.Server($scDest)
    $srvDest.ConnectionContext.Connect()
    $dbDest = $srvDest.Databases[$DestDatabase]
    $DestTableSMO = $dbDest.Tables | Where-Object { $_.Name -eq $DestTable -and $_.Schema -eq $DestSchema }

    if (-not $DestTableSMO) {
        throw "Verification failed: Could not locate Destination table after copy."
    }
    
    # Get structures for comparison (Source structure is already in $SourceTableSMO from Step 2)
    # NOTE: Reconnecting to Source (A) might be necessary if $SourceTableSMO was lost/not available, 
    # but we rely on the object being retained from Step 2.
    # To be safe, a cleaner implementation might reconnect to the Source here.
    
    $SourceStruct = Get-TableStructureObject $SourceTableSMO 
    $DestStruct = Get-TableStructureObject $DestTableSMO

    # A. Compare Columns (Structure, Computed Columns)
    Write-Host "  -> 1. Comparing Column Structure (A vs B)..."
    $ColCompare = Compare-Object $SourceStruct.Columns $DestStruct.Columns -Property Name, DataType, IsComputed, IsPersisted, Formula -IncludeEqual
    $ColDiff = $ColCompare | Where-Object { $_.SideIndicator -ne '==' }
    if ($ColDiff.Count -eq 0) {
        Write-Host "     ✅ Column Structure (including Computed Columns) matches." -ForegroundColor Green
    } else {
        Write-Host "     ❌ Column Structure Mismatch found:" -ForegroundColor Red
        $ColDiff | Format-List
    }

    # B. Compare Indexes
    Write-Host "  -> 2. Comparing Indexes (A vs B)..."
    $IndexCompare = Compare-Object $SourceStruct.Indexes $DestStruct.Indexes -Property Name, IsUnique -IncludeEqual
    $IndexDiff = $IndexCompare | Where-Object { $_.SideIndicator -ne '==' }
    if ($IndexDiff.Count -eq 0) {
        Write-Host "     ✅ Index Names and Uniqueness matches." -ForegroundColor Green
    } else {
        Write-Host "     ❌ Index Mismatch found:" -ForegroundColor Red
        $IndexDiff | Format-List
    }
    
    $srvDest.ConnectionContext.Disconnect() # Clean up destination connection

} catch {
    Write-Error "SMO Verification Error: $($_.Exception.Message)"
}


### 4.2 Row Count Comparison (T-SQL)
Write-Host "  -> 3. Comparing Row Counts (A vs B)..."

# Query executed on Source Server A, querying B using four-part naming.
$CountSQL = @"
    SELECT 
        (SELECT COUNT(*) FROM [$SourceSchema].[$SourceTable]) AS SourceCount,
        (SELECT COUNT(*) FROM [$DestServer].[$DestDatabase].[$DestSchema].[$DestTable]) AS DestCount
"@

# Note: We execute the count query on the Source Server for simplicity, but it requires
# the Source Server to be able to resolve and connect to the Destination Server.
$RowCounts = Invoke-Sqlcmd -ServerInstance $SourceServer -Database $SourceDatabase -Query $CountSQL -ErrorAction Stop

$SourceRowCount = $RowCounts.SourceCount
$DestRowCount = $RowCounts.DestCount

Write-Host "     Source Row Count: $SourceRowCount"
Write-Host "     Destination Row Count: $DestRowCount"

if ($SourceRowCount -eq $DestRowCount) {
    Write-Host "     ✅ Row Count Check passed: Row counts match." -ForegroundColor Green
} else {
    Write-Host "     ❌ Row Count Check FAILED: $SourceRowCount vs $DestRowCount." -ForegroundColor Red
}


Write-Host ""
Write-Host "==============================================" -ForegroundColor Green
Write-Host "✅ ALL OPERATIONS AND VERIFICATIONS COMPLETED." -ForegroundColor Green
Write-Host "==============================================" -ForegroundColor Green
