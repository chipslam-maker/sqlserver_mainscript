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
