# ==============================================================================
# Script: SQL Table Data Comparator
# Description: Compare two tables column by column using a Primary Key.
# Optimized for: PowerShell 5.0
# ==============================================================================

# --- 1. Configuration ---
$serverA = "Your_Server_A"
$dbA     = "Your_Database_A"
$serverB = "Your_Server_B"
$dbB     = "Your_Database_B"

$tableName = "YourTableName"
$pkColumn  = "GlobalStateId"  # 確保這是你的 Primary Key 欄位名稱
$errorLog  = "C:\Temp\TableCompare_Error.txt"

# --- 2. SQL Data Access Function ---
function Get-SqlData {
    param($server, $db, $sql)
    $connString = "Server=$server;Database=$db;Integrated Security=True;TrustServerCertificate=True;"
    try {
        $conn = New-Object System.Data.SqlClient.SqlConnection($connString)
        $adapter = New-Object System.Data.SqlClient.SqlDataAdapter($sql, $conn)
        $table = New-Object System.Data.DataTable
        $adapter.Fill($table) | Out-Null
        return $table
    }
    catch {
        Write-Error "Failed to connect to $server\$db : $($_.Exception.Message)"
    }
}

# --- 3. Fetch Data ---
Write-Host "Connecting to databases and fetching data..." -ForegroundColor Cyan
$query = "SELECT * FROM $tableName"
$dtA = Get-SqlData $serverA $dbA $query
$dtB = Get-SqlData $serverB $dbB $query

if ($null -eq $dtA -or $null -eq $dtB) { 
    Write-Host "Data retrieval failed. Exiting." -ForegroundColor Red
    return 
}

# --- 4. Prepare Hashtable for Database B (Performance Boost) ---
# This converts the Array/DataTable into a Dictionary for instant lookup by PK
Write-Host "Indexing Database B..." -ForegroundColor Gray
$lookupB = @{}
foreach ($row in $dtB) {
    $key = $row.$pkColumn.ToString().Trim()
    if (-not $lookupB.ContainsKey($key)) {
        $lookupB.Add($key, $row)
    }
}

# --- 5. Start Comparison ---
Write-Host "Starting Column-by-Column comparison..." -ForegroundColor Cyan
$report = New-Object System.Collections.Generic.List[string]
$report.Add("Comparison Report - $(Get-Date)")
$report.Add("Table: $tableName | PK: $pkColumn")
$report.Add("-" * 80)

$columns = $dtA.Columns | ForEach-Object { $_.ColumnName }
$diffCount = 0

foreach ($rowA in $dtA) {
    $pkValue = $rowA.$pkColumn.ToString().Trim()
    
    # Quick lookup in Hashtable
    if (-not $lookupB.ContainsKey($pkValue)) {
        $report.Add("[-] MISSING: PK [$pkValue] exists in DB A but is missing in DB B.")
        continue
    }

    $rowB = $lookupB[$pkValue]
    $diffsInRow = @()

    foreach ($col in $columns) {
        # Null-safe value conversion
        $valA = if ($rowA.$col -is [DBNull]) { "<NULL>" } else { $rowA.$col.ToString().Trim() }
        $valB = if ($rowB.$col -is [DBNull]) { "<NULL>" } else { $rowB.$col.ToString().Trim() }

        if ($valA -ne $valB) {
            $diffsInRow += "[$col]:(A:'$valA'|B:'$valB')"
        }
    }

    if ($diffsInRow.Count -gt 0) {
        $report.Add("[!] DIFF: PK [$pkValue] -> " + ($diffsInRow -join " "))
        $diffCount++
    }
}

# --- 6. Save Results ---
$report | Out-File -FilePath $errorLog -Encoding UTF8
Write-Host "Comparison Complete!" -ForegroundColor Green
Write-Host "Total differences found: $diffCount"
Write-Host "Report saved to: $errorLog" -ForegroundColor Yellow
