# 1. 設定
$serverA = "ServerA"
$serverB = "ServerB"
$dbA = "DB_A"
$dbB = "DB_B"

# --- 【切換模式】 ---
# 如果要查 Table，請設為 $true 並輸入 SQL 語句
$useTableQuery = $true 
$queryText = "SELECT * FROM dbo.YourTableName WHERE Date = '2026-01-16'" # 如果是 Table 查詢
$spName = "YourStoredProcedureName"                                      # 如果是 SP

# 組合鍵設定
$key1 = "KeyColumn1"
$key2 = "KeyColumn2"

$connStrA = "Server=$serverA;Database=$dbA;Integrated Security=True;TrustServerCertificate=True;"
$connStrB = "Server=$serverB;Database=$dbB;Integrated Security=True;TrustServerCertificate=True;"

function Get-SqlData($connStr, $text, $isTable) {
    $conn = New-Object System.Data.SqlClient.SqlConnection($connStr)
    $cmd = New-Object System.Data.SqlClient.SqlCommand($text, $conn)
    
    # 判斷是執行 Table 查詢還是 SP
    if ($isTable) {
        $cmd.CommandType = [System.Data.CommandType]::Text
    } else {
        $cmd.CommandType = [System.Data.CommandType]::StoredProcedure
        # 如果是 SP 且需要參數，請在此加入：
        # $cmd.Parameters.AddWithValue("@ParamName", "2026-01-16") | Out-Null
    }

    $da = New-Object System.Data.SqlClient.SqlDataAdapter($cmd)
    $dt = New-Object System.Data.DataTable
    $da.Fill($dt) | Out-Null
    return $dt
}

try {
    $currentAction = if ($useTableQuery) { "Table 查詢" } else { "SP 執行" }
    Write-Host "⏳ 正在透過 $currentAction 抓取並分析差異..." -ForegroundColor Gray
    
    $finalQuery = if ($useTableQuery) { $queryText } else { $spName }
    $dataA = Get-SqlData $connStrA $finalQuery $useTableQuery
    $dataB = Get-SqlData $connStrB $finalQuery $useTableQuery

    # 2. 獲取欄位清單
    $columnNames = $dataA.Columns | Select-Object -ExpandProperty ColumnName
    
    # 3. 使用 Compare-Object 進行初步比對
    $diffResults = Compare-Object -ReferenceObject $dataA -DifferenceObject $dataB -Property $columnNames -PassThru

    # 4. 分析具體欄位差異
    $finalReport = foreach ($item in $diffResults) {
        $diffCols = New-Object System.Collections.Generic.List[string]
        
        # 組合鍵定位
        $match = { $_.$key1 -eq $item.$key1 -and $_.$key2 -eq $item.$key2 }
        $otherRow = if ($item.SideIndicator -eq "<=") { $dataB | Where-Object $match } else { $dataA | Where-Object $match }

        if ($null -ne $otherRow) {
            foreach ($colName in $columnNames) {
                # 排除 Different_Columns 自身不比對
                if ("$($item.$colName)" -ne "$($otherRow.$colName)") {
                    $diffCols.Add($colName)
                }
            }
        } else {
            $diffCols.Add("!!! 另一伺服器缺失此 Key !!!")
        }

        # 組合輸出物件
        $item | Select-Object @{N='SideIndicator'; E={$_.SideIndicator}}, 
                            @{N='Different_Columns'; E={$diffCols -join ", "}}, 
                            *
    }

    # 5. 輸出結果
    if ($null -eq $finalReport) {
        Write-Host "✅ 數據完全一致！" -ForegroundColor Green
    } else {
        $finalReport | Out-GridView -Title "比對結果 ($currentAction)"
    }
}
catch {
    Write-Error "執行失敗: $($_.Exception.Message)"
}
