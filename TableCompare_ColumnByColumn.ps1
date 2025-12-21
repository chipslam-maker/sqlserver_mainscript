# --- Configuration ---
$serverA = "Server_A_Address"
$dbA = "Database_A_Name"
$serverB = "Server_B_Address"
$dbB = "Database_B_Name"

$tableName = "YourTableName"
$pkColumn = "GlobalStateId" # Your PK
$errorLogPath = "C:\Temp\CompareErrors.txt"

# --- SQL Queries ---
$query = "SELECT * FROM $tableName ORDER BY $pkColumn"

function Get-SqlData {
    param($server, $db, $sql)
    $connString = "Server=$server;Database=$db;Integrated Security=True;TrustServerCertificate=True;"
    $adapter = New-Object System.Data.SqlClient.SqlDataAdapter($sql, $connString)
    $table = New-Object System.Data.DataTable
    $adapter.Fill($table) | Out-Null
    return $table
}

# --- Fetch Data ---
Write-Host "Fetching data from both databases..." -ForegroundColor Cyan
$dtA = Get-SqlData $serverA $dbA $query
$dtB = Get-SqlData $serverB $dbB $query

# --- Compare Logic ---
$report = New-Object System.Collections.Generic.List[string]
$report.Add("Comparison Report - $(Get-Date)")
$report.Add("Table: $tableName | PK: $pkColumn")
$report.Add("-" * 50)

$columns = $dtA.Columns | ForEach-Object { $_.ColumnName }

foreach ($rowA in $dtA) {
    $pkValue = $rowA.$pkColumn
    # Find matching row in Table B using PK
    $rowB = $dtB.Select("$pkColumn = '$pkValue'")

    if ($rowB.Count -eq 0) {
        $report.Add("ERROR: PK [$pkValue] missing in Database B.")
        continue
    }

    $targetRowB = $rowB[0]
    $hasDifference = $false
    $diffDetails = "PK [$pkValue] Differences: "

    foreach ($col in $columns) {
        $valA = $rowA.$col.ToString().Trim()
        $valB = $targetRowB.$col.ToString().Trim()

        if ($valA -ne $valB) {
            $hasDifference = $true
            $diffDetails += "Column [$col] (A: '$valA' | B: '$valB') "
        }
    }

    if ($hasDifference) {
        $report.Add($diffDetails)
    }
}

# --- Output to File ---
$report | Out-File -FilePath $errorLogPath
Write-Host "Comparison complete. Errors logged to: $errorLogPath" -ForegroundColor Green
