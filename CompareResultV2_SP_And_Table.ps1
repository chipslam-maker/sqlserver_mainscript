# ==========================================
# PET 數據比對工具 (支援 SP/Table & 彈性組合鍵)
# ==========================================

# 1. 伺服器連線設定
$serverA = "ServerA"
$serverB = "ServerB"
$dbA = "DB_A"
$dbB = "DB_B"
$connStrA = "Server=$serverA;Database=$dbA;Integrated Security=True;TrustServerCertificate=True;"
$connStrB = "Server=$serverB;Database=$dbB;Integrated Security=True;TrustServerCertificate=True;"

# 2. 查詢模式設定
$useTableQuery = $true   # $true = 使用 SQL 語句, $false = 使用 Stored Procedure
$queryText = "SELECT * FROM dbo.YourTable WHERE Date = '2026-01-16'" # SQL 模式使用
$spName = "YourStoredProcedureName"                                  # SP 模式使用
$testDate = "2026-01-16"                                             # SP 參數值
$paramName = "@YourParamName"                                        # SP 參數名

# 3. 組合鍵設定 (Key2 可為 $null)
$key1 = "KeyColumn1"
$key2 = "KeyColumn2"

function Get-SqlData($connStr, $text, $isTable) {
    $conn = New-Object System.Data.SqlClient.SqlConnection($connStr)
    $cmd = New-Object System.Data.SqlClient.SqlCommand($text, $conn)
    
    if ($isTable) {
        $cmd.CommandType = [System.Data.CommandType]::Text
    } else {
        $cmd.CommandType = [System.Data.CommandType]::StoredProcedure
        $cmd.Parameters.AddWithValue($paramName, $testDate) | Out-Null
    }

    $da = New-Object System.Data.SqlClient.SqlDataAdapter($cmd)
    $dt = New-Object System.Data.DataTable
    $da.Fill($dt) | Out-Null
    return $dt
}

try {
    $action = if ($useTableQuery) { "Table 查詢" } else { "SP 執行" }
    Write-Host "⏳ [PET] 正在從兩台伺服器抓取 $action 數據..." -ForegroundColor Gray
    
    $finalQuery = if ($useTableQuery) { $queryText } else { $spName }
    $dataA = Get-SqlData $connStrA $finalQuery $useTableQuery
    $dataB = Get-SqlData $connStrB $finalQuery $useTableQuery

    # 獲取所有欄位名稱
    $columnNames = $dataA.Columns | Select-Object -ExpandProperty ColumnName
    
    Write-Host "🔍 [PET] 正在比對 100+ 欄位差異..." -ForegroundColor Cyan
    
    # 4. 初步找出有差異的行 (基於內容比對)
    $diffResults = Compare-Object -ReferenceObject $dataA -DifferenceObject $dataB -Property $columnNames -PassThru

    # 5. 分析每一行差異具體發生在哪個 Column
    $finalReport = foreach ($item in $diffResults) {
        $diffCols = New-Object System.Collections.Generic.List[string]
        
        # 彈性組合鍵匹配邏輯 (處理 Key2 為空的情況)
        $matchLogic = { 
            $_.$key1 -eq $item.$key1 -and 
            (
                ($null -ne $item.$key2 -and $_.$key2 -eq $item.$key2) -or 
                ($null -eq $item.$key2 -and $null -eq $_.$key2)
            )
        }

        # 尋找對應伺服器中的同一筆資料
        $otherRow = if ($item.SideIndicator -eq "<=") { 
            $dataB | Where-Object $matchLogic | Select-Object -First 1 
        } else { 
            $dataA | Where-Object $matchLogic | Select-Object -First 1 
        }

        if ($null -ne $otherRow) {
            foreach ($colName in $columnNames) {
                # 轉成字串比對，避免類型不一致導致誤判
                if ("$($item.$colName)" -ne "$($otherRow.$colName)") {
                    $diffCols.Add($colName)
                }
            }
        } else {
            $diffCols.Add("!!! 另一伺服器缺失此組合鍵 !!!")
        }

        # 組合輸出結果，保留 SideIndicator 並將不同欄位放在最前面
        $item | Select-Object @{N='SideIndicator'; E={$_.SideIndicator}}, 
                            @{N='Different_Columns'; E={$diffCols -join ", "}}, 
                            *
    }

    # 6. 輸出結果與互動顯示
    if ($null -eq $finalReport) {
        Write-Host "✅ [PET] 數據完全一致！所有欄位均無差異。" -ForegroundColor Green
    } else {
        Write-Host "❌ [PET] 發現差異！正在開啟 GridView 視窗..." -ForegroundColor Red
        $finalReport | Out-GridView -Title "PET 比對工具 - (<=:ServerA, =>:ServerB)"
    }
}
catch {
    Write-Error "❌ 執行失敗: $($_.Exception.Message)"
}
