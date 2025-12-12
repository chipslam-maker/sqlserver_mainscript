{
    "Source": {
        "InstanceName": "SQL_SERVER_A_INSTANCE_NAME",
        "DatabaseName": "SourceDBName"
    },
    "Destination": {
        "InstanceName": "SQL_SERVER_B_INSTANCE_NAME",
        "DatabaseName": "DestinationDBName"
    },
    "TableToCopy": "dbo.YourTableName",
    "Options": {
        "UseIntegratedSecurity": true,
        "DropTableIfExists": true,
        "IncludeForeignKeys": false,
        "IncludeTriggers": false
    }
}

# Requires the dbatools module for SQL Server operations
# Check and install the dbatools module
if (-not (Get-Module -ListAvailable -Name 'dbatools')) {
    Write-Host "dbatools module is not installed. Attempting to install..."
    try {
        Install-Module dbatools -Scope CurrentUser -Force
    } catch {
        Write-Error "Failed to install the dbatools module. Please install it manually and re-run the script."
        exit 1
    }
}

Import-Module dbatools

# --- Configuration Reading ---

# Define configuration file path
$ConfigFile = ".\Config.json"

# Check if the configuration file exists
if (-not (Test-Path $ConfigFile)) {
    Write-Error "Configuration file not found: $ConfigFile"
    exit 1
}

# Read the JSON configuration
$Config = Get-Content $ConfigFile | ConvertFrom-Json

# Simplify variables
$Source = $Config.Source
$Dest = $Config.Destination
$Table = $Config.TableToCopy
$Options = $Config.Options

Write-Host "--- Starting Table Copy Script ---"
Write-Host "Source: $($Source.InstanceName) | DB: $($Source.DatabaseName)"
Write-Host "Destination: $($Dest.InstanceName) | DB: $($Dest.DatabaseName)"
Write-Host "Table: $Table"
Write-Host "----------------------------------"

# --- Step 3: Check and Drop Existing Table (if configured) ---
Write-Host "`n>> Step 3: Checking for existing table on Destination..."

if (Test-DbaTable -SqlInstance $Dest.InstanceName -Database $Dest.DatabaseName -Table $Table) {
    Write-Warning "Destination Table '$Table' already exists on $($Dest.InstanceName)."
    
    if ($Options.DropTableIfExists) {
        Write-Warning "Configuration requires dropping the existing table. Executing DROP TABLE..."
        try {
            Remove-DbaTable -SqlInstance $Dest.InstanceName -Database $Dest.DatabaseName -Table $Table -Force -Confirm:$false
            Write-Host "Table '$Table' dropped successfully."
        } catch {
            Write-Error "Failed to drop Table '$Table': $($_.Exception.Message)"
            exit 1
        }
    } else {
        Write-Error "Destination Table exists, but configuration does not allow dropping it. Script terminated."
        exit 1
    }
} else {
    Write-Host "Destination Table does not exist. Proceeding with creation."
}

# --- Step 4: Copy Table Structure (Schema, Indexes, Computed Columns) ---
Write-Host "`n>> Step 4: Copying Table Schema (including Indexes and Computed Columns)..."

try {
    # Define items to exclude during schema copy
    $ExcludeItems = @("Data") # Always exclude data during schema-only copy
    if (-not $Options.IncludeForeignKeys) { $ExcludeItems += "ForeignKeys" }
    if (-not $Options.IncludeTriggers) { $ExcludeItems += "Triggers" }
    
    # Copy structure only
    Copy-DbaTable -Source $Source.InstanceName -SourceDatabase $Source.DatabaseName -Table $Table `
                  -Destination $Dest.InstanceName -DestinationDatabase $Dest.DatabaseName `
                  -CopyData $false -Exclude $ExcludeItems -PassThru | Out-Null
                  
    Write-Host "Table schema (including INDEXES and COMPUTED COLUMNS definitions) created successfully."
} catch {
    Write-Error "Schema copy failed: $($_.Exception.Message)"
    exit 1
}

# --- Step 5: Copy Data ---
Write-Host "`n>> Step 5: Copying data (Computed Columns will be recalculated by SQL Server)..."

try {
    # Copy data only, using the existing schema
    Copy-DbaTable -Source $Source.InstanceName -SourceDatabase $Source.DatabaseName -Table $Table `
                  -Destination $Dest.InstanceName -DestinationDatabase $Dest.DatabaseName `
                  -CopyData $true -NoSchema -PassThru | Out-Null
                  
    Write-Host "Data copied successfully."
} catch {
    Write-Error "Data copy failed: $($_.Exception.Message)"
    exit 1
}

# --- Step 6: Final Verification (Row Count & Column Structure) ---
Write-Host "`n>> Step 6: Final Verification..."

# 6A. Check Row Count
Write-Host "--- 6A: Checking Row Count ---"
$SourceCount = Get-DbaRowCount -SqlInstance $Source.InstanceName -Database $Source.DatabaseName -Table $Table
$DestCount = Get-DbaRowCount -SqlInstance $Dest.InstanceName -Database $Dest.DatabaseName -Table $Table

Write-Host "Source Row Count: $SourceCount"
Write-Host "Destination Row Count: $DestCount"

if ($SourceCount -eq $DestCount) {
    Write-Host "✅ Row Count Check SUCCESS: Row counts are identical."
} else {
    Write-Error "❌ Row Count Check FAILED: Source ($SourceCount) is different from Destination ($DestCount)!"
}

# 6B. Check Column Structure
Write-Host "--- 6B: Checking Column Structure ---"
$SourceColumns = Get-DbaTableColumn -SqlInstance $Source.InstanceName -Database $Source.DatabaseName -Table $Table
$DestColumns = Get-DbaTableColumn -SqlInstance $Dest.InstanceName -Database $Dest.DatabaseName -Table $Table

# Compare the critical column properties: ColumnName, DataType, IsNullable
$Differences = Compare-Object -ReferenceObject $SourceColumns -DifferenceObject $DestColumns -Property ColumnName, DataType, IsNullable

if ($null -eq $Differences) {
    Write-Host "✅ Column Structure Check SUCCESS: All critical column properties match."
} else {
    Write-Error "❌ Column Structure Check FAILED: Found the following discrepancies:"
    $Differences | Format-Table -AutoSize
}

Write-Host "`n--- Script Execution Completed ---"
