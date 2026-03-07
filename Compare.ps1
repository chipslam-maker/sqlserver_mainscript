# ============================================================
# SQL Server Query Result Comparison Script v4
# ============================================================

# ── 設定區 ──────────────────────────────────────────────────
$OldServer    = "OLD_SERVER_NAME"       # 舊 Server 名稱
$NewServer    = "NEW_SERVER_NAME"       # 新 Server 名稱
$Database     = "YOUR_DATABASE_NAME"   # 資料庫名稱
$QueryTimeout = 120                    # 執行逾時（秒）
$OutputFolder = "C:\QueryCompare"      # 報告輸出資料夾
$SqlFile      = "$PSScriptRoot\query.sql"  # SQL 檔案路徑（同一資料夾）

# ── Composite Key 設定 ──────────────────────────────────────
# 只需改這一行！填入用來組合 Key 的欄位名稱（一個或多個）
# 例子：$KeyColumns = @("ORDER_ID", "PRODUCT_CODE", "DATE")
$KeyColumns = @("COL1", "COL2", "COL3")   # ← 改成你的欄位名

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

        $Adapter = New-Object System.Data.SqlClient.SqlDataAdapter($Cmd)
        $DataSet = New-Object System.Data.DataSet

        $StartTime = Get-Date
        $Adapter.Fill($DataSet) | Out-Null
        $Elapsed = (Get-Date) - $StartTime

        $Table = $DataSet.Tables[$DataSet.Tables.Count - 1]

        Write-Host "  完成！共 $($Table.Rows.Count) 行，耗時 $($Elapsed.TotalSeconds.ToString('F1')) 秒" -ForegroundColor Green

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

# ── 函數：DataTable 轉兩個 Hashtable ────────────────────────
#    $HashByKey  : Key → 整行值（用於快速比較）
#    $RowByKey   : Key → Row 物件（用於逐欄比較）
function ConvertTo-Hashtable {
    param (
        [Parameter(Mandatory=$true)]
        [System.Data.DataTable]$Table,
        [string[]]$KeyColumns
    )

    # 確認所有 Key Column 都存在
    foreach ($Col in $KeyColumns) {
        if (-not $Table.Columns.Contains($Col)) {
            Write-Host "  ❌ 找不到欄位：$Col" -ForegroundColor Red
            Write-Host "  可用欄位：$($Table.Columns.ColumnName -join ', ')" -ForegroundColor Yellow
            exit 1
        }
    }

    $HashByKey = @{}
    $RowByKey  = @{}
    $DuplicateKeys = [System.Collections.Generic.List[string]]::new()

    foreach ($Row in $Table.Rows) {
        $KeyParts = $KeyColumns | ForEach-Object {
            if ($Row[$_] -eq [DBNull]::Value) { "NULL" } else { $Row[$_].ToString() }
        }
        $Key = $KeyParts -join "~"

        if ($HashByKey.ContainsKey($Key)) {
            $DuplicateKeys.Add($Key)
        }

        $Values = ($Table.Columns.ColumnName | ForEach-Object {
            if ($Row[$_] -eq [DBNull]::Value) { "NULL" } else { $Row[$_].ToString() }
        }) -join "|"

        $HashByKey[$Key] = $Values
        $RowByKey[$Key]  = $Row  # 儲存整個 Row 方便逐欄比較
    }

    if ($DuplicateKeys.Count -gt 0) {
        Write-Host "  ⚠️  警告：發現 $($DuplicateKeys.Count) 個重複 Key，建議增加更多 Key Column！" -ForegroundColor Yellow
    }

    return $HashByKey, $RowByKey
}

# ── 函數：逐欄比較兩行，回傳差異欄位清單 ───────────────────
function Get-ColumnDiff {
    param (
        $OldRow,
        $NewRow,
        [System.Data.DataColumnCollection]$Columns
    )

    $DiffColumns = [System.Collections.Generic.List[PSCustomObject]]::new()

    foreach ($Col in $Columns) {
        $ColName = $Col.ColumnName
        $OldVal  = if ($OldRow[$ColName] -eq [DBNull]::Value) { "NULL" } else { $OldRow[$ColName].ToString() }
        $NewVal  = if ($NewRow[$ColName] -eq [DBNull]::Value) { "NULL" } else { $NewRow[$ColName].ToString() }

        if ($OldVal -ne $NewVal) {
            $DiffColumns.Add([PSCustomObject]@{
                Column   = $ColName
                舊Server值 = $OldVal
                新Server值 = $NewVal
            })
        }
    }

    return $DiffColumns
}

# ============================================================
# ── 主流程 ───────────────────────────────────────────────────
# ============================================================

Write-Host "`n============================================" -ForegroundColor Yellow
Write-Host " SQL Server Query 比較工具 v4" -ForegroundColor Yellow
Write-Host "============================================`n" -ForegroundColor Yellow
Write-Host "  使用 Composite Key：$($KeyColumns -join ' + ')" -ForegroundColor Cyan

# [1] 執行舊 Server
Write-Host "`n[1/4] 執行舊 Server Query..." -ForegroundColor Yellow
$OldTable = Invoke-SqlQuery -Server $OldServer -Database $Database -Query $SqlQuery -Timeout $QueryTimeout
$OldTable = [System.Data.DataTable]$OldTable

# [2] 執行新 Server
Write-Host "`n[2/4] 執行新 Server Query..." -ForegroundColor Yellow
$NewTable = Invoke-SqlQuery -Server $NewServer -Database $Database -Query $SqlQuery -Timeout $QueryTimeout
$NewTable = [System.Data.DataTable]$NewTable

# [3] 轉換並比較
Write-Host "`n[3/4] 比較結果中..." -ForegroundColor Yellow
$OldHashByKey, $OldRowByKey = ConvertTo-Hashtable -Table $OldTable -KeyColumns $KeyColumns
$NewHashByKey, $NewRowByKey = ConvertTo-Hashtable -Table $NewTable -KeyColumns $KeyColumns

$OnlyInOld = [System.Collections.Generic.List[string]]::new()
$OnlyInNew = [System.Collections.Generic.List[string]]::new()
$ValueDiff = [System.Collections.Generic.List[string]]::new()

foreach ($Key in $OldHashByKey.Keys) {
    if (-not $NewHashByKey.ContainsKey($Key)) {
        $OnlyInOld.Add($Key)
    } elseif ($OldHashByKey[$Key] -ne $NewHashByKey[$Key]) {
        $ValueDiff.Add($Key)
    }
}

foreach ($Key in $NewHashByKey.Keys) {
    if (-not $OldHashByKey.ContainsKey($Key)) {
        $OnlyInNew.Add($Key)
    }
}

# [4] 輸出結果
Write-Host "`n[4/4] 生成報告..." -ForegroundColor Yellow

$IsMatch = ($OnlyInOld.Count -eq 0 -and $OnlyInNew.Count -eq 0 -and $ValueDiff.Count -eq 0)

Write-Host "`n============================================" -ForegroundColor Yellow
Write-Host " 比較結果摘要" -ForegroundColor Yellow
Write-Host "============================================" -ForegroundColor Yellow
Write-Host "  Composite Key  : $($KeyColumns -join ' + ')"
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

# 舊有新無
foreach ($Key in $OnlyInOld) {
    $CsvRows.Add([PSCustomObject]@{
        差異類型       = "舊有新無"
        COMPOSITE_KEY  = $Key
        差異欄位       = ""
        舊Server值     = $OldHashByKey[$Key]
        新Server值     = ""
    })
}

# 新有舊無
foreach ($Key in $OnlyInNew) {
    $CsvRows.Add([PSCustomObject]@{
        差異類型       = "新有舊無"
        COMPOSITE_KEY  = $Key
        差異欄位       = ""
        舊Server值     = ""
        新Server值     = $NewHashByKey[$Key]
    })
}

# 值不同 → 逐欄比較，每個差異欄位獨立一行
foreach ($Key in $ValueDiff) {
    $OldRow  = $OldRowByKey[$Key]
    $NewRow  = $NewRowByKey[$Key]
    $ColDiffs = Get-ColumnDiff -OldRow $OldRow -NewRow $NewRow -Columns $OldTable.Columns

    foreach ($Diff in $ColDiffs) {
        $CsvRows.Add([PSCustomObject]@{
            差異類型       = "值不同"
            COMPOSITE_KEY  = $Key
            差異欄位       = $Diff.Column      # ← 具體哪個 Column 不同
            舊Server值     = $Diff.舊Server值
            新Server值     = $Diff.新Server值
        })
    }
}

if ($CsvRows.Count -gt 0) {
    $CsvRows | Export-Csv -Path $CsvPath -NoTypeInformation -Encoding UTF8
    Write-Host "`n  📄 差異報告已儲存：$CsvPath" -ForegroundColor Cyan
} else {
    Write-Host "`n  📄 無差異，不生成 CSV" -ForegroundColor Green
}

Write-Host "`n完成！`n" -ForegroundColor Yellow
