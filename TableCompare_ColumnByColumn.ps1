# ==============================================================================
# Script: SQL Data Integrity Comparator (PET Optimized Version)
# Target: PowerShell 5.0
# Description: Compares two tables row-by-row and column-by-column.
# ==============================================================================

# --- 1. 設定參數 ---
$serverA    = "Your_Server_A"
$dbA        = "Your_Database_A"
$serverB    = "Your_Server_B"
$dbB        = "Your_Database_B"
$tableName  = "YourTableName"
$pkColumn   = "GlobalStateId" 
$errorLog   = "C:\Temp\Compare_Error_$(Get-Date -Format 'yyyyMMdd_HHmm').txt"

# --- 2. SQL 讀取函數 (含強制類型保護) ---
function Get-SqlData {
    param($server, $db, $sql)
    $connString = "Server=$server;Database=$db;Integrated Security=True;TrustServerCertificate=True;"
    try {
        $conn = New-Object System.Data.SqlClient.SqlConnection($connString)
        $adapter = New-Object System.Data.SqlClient.SqlDataAdapter($sql, $conn)
        $table = New-Object System.Data.DataTable
        $adapter.Fill($table) | Out-Null
        
        # 關鍵點：使用逗號 [,] 確保回傳的是整個 DataTable 物件，而不是 Row 陣列
        return ,$table 
    }
    catch {
        Write-Error "連線失敗 ($server): $($_.Exception.Message)"
        return $null
    }
}

# --- 3. 獲取資料 ---
Write-Host "正在從資料庫讀取資料..." -ForegroundColor Cyan
$sqlQuery = "SELECT * FROM $tableName"

$dtA = Get-SqlData $serverA $dbA $sqlQuery
$dtB = Get-SqlData $serverB $dbB $sqlQuery

# 安全檢查
if ($null -eq $dtA -or $null -eq $dtB) {
    Write-Host "無法獲取資料，請檢查連線設定。" -ForegroundColor Red
    return
}

# --- 4. 結構確認 (Debug 資訊) ---
$columnNames = $dtA.Columns.ColumnName
Write-Host "偵測到欄位數量: $($columnNames.Count)" -ForegroundColor Gray
Write-Host "DB A 筆數: $($dtA.Rows.Count)" -ForegroundColor Gray
Write-Host "DB B 筆數: $($dtB.Rows.Count)" -ForegroundColor Gray

# --- 5. 建立 DB B 的快速索引 (Hashtable) ---
Write-Host "正在建立資料索引以提升效能..." -ForegroundColor Cyan
$lookupB = @{}
foreach ($rowB in $dtB.Rows) {
    $key = $rowB.$pkColumn.ToString().Trim()
    if (-not $lookupB.ContainsKey($key)) {
        $lookupB.Add($key, $rowB)
    }
}

# --- 6. 執行逐欄比對 ---
Write-Host "開始比對作業..." -ForegroundColor Yellow
$report = New-Object System.Collections.Generic.List[string]
$report.Add("=== Table Comparison Report ===")
$report.Add("Generated: $(Get-Date)")
$report.Add("Table: $tableName | PK: $pkColumn")
$report.Add("-" * 100)

$diffCount = 0

foreach ($rowA in $dtA.Rows) {
    $pkValue = $rowA.$pkColumn.ToString().Trim()
    
    # 檢查 PK 是否存在於 B 庫
    if (-not $lookupB.ContainsKey($pkValue)) {
        $report.Add("[-] MISSING: PK [$pkValue] 不存在於 Database B")
        $diffCount++
        continue
    }

    $rowB = $lookupB[$pkValue]
    $rowDiffs = New-Object System.Collections.Generic.List[string]

    foreach ($colName in $columnNames) {
        # 處理 NULL 與字串轉型
        $valA = if ($rowA.$colName -is [DBNull]) { "<NULL>" } else { $rowA.$colName.ToString().Trim() }
        $valB = if ($rowB.$colName -is [DBNull]) { "<NULL>" } else { $rowB.$colName.ToString().Trim() }

        if ($valA -ne $valB) {
            $rowDiffs.Add("[$colName]:(A:'$valA' | B:'$valB')")
        }
    }

    if ($rowDiffs.Count -gt 0) {
        $report.Add("[!] DIFF: PK [$pkValue] -> " + ($rowDiffs -join " "))
        $diffCount++
    }
}

# --- 7. 輸出結果 ---
if ($diffCount -eq 0) {
    $report.Add("結果：兩座資料庫內容完全一致！")
}

# 確保目錄存在
$logDir = [System.IO.Path]::GetDirectoryName($errorLog)
if (-not (Test-Path $logDir)) { New-Item -ItemType Directory -Path $logDir | Out-Null }

$report | Out-File -FilePath $errorLog -Encoding UTF8
Write-Host "比對完成！總共發現 $diffCount 筆差異。" -ForegroundColor Green
Write-Host "錯誤報告已存至: $errorLog" -ForegroundColor White
