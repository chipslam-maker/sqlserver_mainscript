# ============================================================
# SQL Server Query Result Comparison Script v2
# ============================================================

# ── 設定區 ──────────────────────────────────────────────────
$OldServer    = "OLD_SERVER_NAME"       # 舊 Server 名稱
$NewServer    = "NEW_SERVER_NAME"       # 新 Server 名稱
$Database     = "YOUR_DATABASE_NAME"   # 資料庫名稱
$QueryTimeout = 120                    # 執行逾時（秒）
$PrimaryKey   = "ROW_ID"              # 你的 Primary Key 欄位名
$OutputFolder = "C:\QueryCompare"      # 報告輸出資料夾
$SqlFile      = "$PSScriptRoot\query.sql"  # SQL 檔案路徑（同一資料夾）

# ============================================================

# ── 讀取 SQL 檔案 ────────────────────────────────────────────
if (-not (Test-Path $SqlFile)) {
    Write-Host "❌ 找不到 SQL 檔案：$SqlFile" -ForegroundColor Red
    exit 1
}
$SqlQuery = Get-Content -Path $SqlFile -Raw -Encoding UTF8
Write-Host "✅ 已讀取 SQL 檔案：$SqlFile" -ForegroundColor Green

# ── 建立輸出資料夾 ───────────────────────────────────────────
if (-not (Test-Path $OutputFolder)) {
    New-Item -ItemType Directory -Path $OutputFolder | Out-Null
}
$Timestamp = Get-Date -Format "yyyyMMdd_HHmmss"

# ============================================================

# ── 函數：執行 Query 並回傳 DataTable ───────────────────────
function Invoke-SqlQuery {
    param (
        [string]$Server,
        [string]$Database,
        [string]$Query,
        [int]$Timeout
    )

    Write-Host "  連接 $Server ..." -ForegroundColor Cyan
    $ConnStr = "Server=$Server;Database=$Database;Integrated Security=True;TrustServerCertificate=True;"
    $Conn = New-Object System.Data.SqlClient.SqlConnection($ConnStr)

    try {
        $Conn.Open()
        Write-Host "  連接成功，執行 Query..." -ForegroundColor Cyan

        $Cmd                = $Conn.CreateCommand()
        $Cmd.CommandText    = $Query
        $Cmd.CommandTimeout = $Timeout

        # 用 DataSet 確保永遠取到正確的 DataTable，不被 PowerShell 自動轉型
        $Adapter = New-Object System.Data.SqlClient.SqlDataAdapter($Cmd)
        $DataSet = New-Object System.Data.DataSet

        $StartTime = Get-Date
        $Adapter.Fill($DataSet) | Out-Null
        $Elapsed = (Get-Date) - $StartTime

        # 取最後一個 Table（即 Query 最後 SELECT 的結果）
        $Table = $DataSet.Tables[$DataSet.Tables.Count - 1]

        Write-Host "  完成！共 $($Table.Rows.Count) 行，耗時 $($Elapsed.TotalSeconds.ToString('F1')) 秒" -ForegroundColor Green

        # 加逗號強制保持 DataTable 類型，防止 PowerShell unwrap
        return ,$Table
    }
    catch {
        Write-Host "  ❌ 錯誤：$($_.Exception.Message)" -ForegroundColor Red
        throw
    }
    finally {
        $Conn.Close()
    }
}

# ── 函數：DataTable 轉 Hashtable ────────────────────────────
function ConvertTo-Hashtable {
    param (
        [Parameter(Mandatory=$true)]
        [System.Data.DataTable]$Table,
        [string]$KeyColumn
    )

    $Hash = @{}

    foreach ($Row in $Table.Rows) {
        $Key = $Row[$KeyColumn].ToString()

        # 逐欄取值，NULL 值轉成字串 "NULL" 避免比較時出錯
        $Values = ($Table.Columns.ColumnName | ForEach-Object {
            if ($Row[$_] -eq [DBNull]::Value) { "NULL" }
            else { $Row[$_].ToString() }
        }) -join "|"

        $Hash[$Key] = $Values
    }

    return $Hash
}

# ============================================================
# ── 主流程 ───────────────────────────────────────────────────
# ============================================================

Write-Host "`n============================================" -ForegroundColor Yellow
Write-Host " SQL Server Query 比較工具 v2" -ForegroundColor Yellow
Write-Host "============================================`n" -ForegroundColor Yellow

# [1] 執行舊 Server
Write-Host "[1/4] 執行舊 Server Query..." -ForegroundColor Yellow
$OldTable = Invoke-SqlQuery -Server $OldServer -Database $Database -Query $SqlQuery -Timeout $QueryTimeout
$OldTable = [System.Data.DataTable]$OldTable

# [2] 執行新 Server
Write-Host "`n[2/4] 執行新 Server Query..." -ForegroundColor Yellow
$NewTable = Invoke-SqlQuery -Server $NewServer -Database $Database -Query $SqlQuery -Timeout $QueryTimeout
$NewTable = [System.Data.DataTable]$NewTable

# [3] 轉換並比較
Write-Host "`n[3/4] 比較結果中..." -ForegroundColor Yellow
$OldHash = ConvertTo-Hashtable -Table $OldTable -KeyColumn $PrimaryKey
$NewHash = ConvertTo-Hashtable -Table $NewTable -KeyColumn $PrimaryKey

$OnlyInOld = [System.Collections.Generic.List[string]]::new()  # 舊有新無
$OnlyInNew = [System.Collections.Generic.List[string]]::new()  # 新有舊無
$ValueDiff = [System.Collections.Generic.List[string]]::new()  # 值不同

foreach ($Key in $OldHash.Keys) {
    if (-not $NewHash.ContainsKey($Key)) {
        $OnlyInOld.Add($Key)
    } elseif ($OldHash[$Key] -ne $NewHash[$Key]) {
        $ValueDiff.Add($Key)
    }
}

foreach ($Key in $NewHash.Keys) {
    if (-not $OldHash.ContainsKey($Key)) {
        $OnlyInNew.Add($Key)
    }
}

# [4] 輸出結果
Write-Host "`n[4/4] 生成報告..." -ForegroundColor Yellow

$IsMatch = ($OnlyInOld.Count -eq 0 -and $OnlyInNew.Count -eq 0 -and $ValueDiff.Count -eq 0)

Write-Host "`n============================================" -ForegroundColor Yellow
Write-Host " 比較結果摘要" -ForegroundColor Yellow
Write-Host "============================================" -ForegroundColor Yellow
Write-Host "  舊 Server 行數 : $($OldTable.Rows.Count)"
Write-Host "  新 Server 行數 : $($NewTable.Rows.Count)"
Write-Host "  舊有新無       : $($OnlyInOld.Count) 行" -ForegroundColor $(if ($OnlyInOld.Count -gt 0) { "Red" } else { "Green" })
Write-Host "  新有舊無       : $($OnlyInNew.Count) 行" -ForegroundColor $(if ($OnlyInNew.Count -gt 0) { "Red" } else { "Green" })
Write-Host "  值不同         : $($ValueDiff.Count) 行" -ForegroundColor $(if ($ValueDiff.Count -gt 0) { "Red" } else { "Green" })

if ($IsMatch) {
    Write-Host "`n  ✅ 結果完全一致！" -ForegroundColor Green
} else {
    Write-Host "`n  ❌ 結果有差異！詳情請查看 CSV 報告" -ForegroundColor Red
}

# ── CSV 輸出 ─────────────────────────────────────────────────
$CsvPath = "$OutputFolder\Compare_$Timestamp.csv"
$CsvRows = [System.Collections.Generic.List[PSCustomObject]]::new()

foreach ($Key in $OnlyInOld) {
    $CsvRows.Add([PSCustomObject]@{
        差異類型    = "舊有新無"
        PRIMARY_KEY = $Key
        舊Server值  = $OldHash[$Key]
        新Server值  = ""
    })
}

foreach ($Key in $OnlyInNew) {
    $CsvRows.Add([PSCustomObject]@{
        差異類型    = "新有舊無"
        PRIMARY_KEY = $Key
        舊Server值  = ""
        新Server值  = $NewHash[$Key]
    })
}

foreach ($Key in $ValueDiff) {
    $CsvRows.Add([PSCustomObject]@{
        差異類型    = "值不同"
        PRIMARY_KEY = $Key
        舊Server值  = $OldHash[$Key]
        新Server值  = $NewHash[$Key]
    })
}

if ($CsvRows.Count -gt 0) {
    $CsvRows | Export-Csv -Path $CsvPath -NoTypeInformation -Encoding UTF8
    Write-Host "`n  📄 差異報告已儲存：$CsvPath" -ForegroundColor Cyan
} else {
    Write-Host "`n  📄 無差異，不生成 CSV" -ForegroundColor Green
}

Write-Host "`n完成！`n" -ForegroundColor Yellow
