# 1. 基本設定 (已加入 TrustServerCertificate)
$testDate = "2026-01-16"
$paramName = "@YourParamName"
$spName = "YourStoredProcedureName"
$key1 = "第一個組合鍵欄位" # 例如 BranchID
$key2 = "第二個組合鍵欄位" # 例如 OrderNo

$connStrA = "Server=ServerA;Database=DB_A;Integrated Security=True;TrustServerCertificate=True;"
$connStrB = "Server=ServerB;Database=DB_B;Integrated Security=True;TrustServerCertificate=True;"

function Get-SpData($connStr, $sp, $pName, $pVal) {
    $conn = New-Object System.Data.SqlClient.SqlConnection($connStr)
    $cmd = New-Object System.Data.SqlClient.SqlCommand($sp, $conn)
    $cmd.CommandType = [System.Data.CommandType]::StoredProcedure
    $cmd.Parameters.AddWithValue($pName, $pVal) | Out-Null
    $da = New-Object System.Data.SqlClient.SqlDataAdapter($cmd)
    $dt = New-Object System.Data.DataTable
    $da.Fill($dt) | Out-Null
    return $dt
}

try {
    Write-Host "⏳ 正在抓取數據並分析差異..." -ForegroundColor Gray
    $dataA = Get-SpData $connStrA $spName $paramName $testDate
    $dataB = Get-SpData $connStrB $spName $paramName $testDate

    # 2. 使用原本的 Compare-Object 獲取 SideIndicator
    # 我們先比對所有欄位內容
    $columnNames = $dataA.Columns | Select-Object -ExpandProperty ColumnName
    $diffResults = Compare-Object -ReferenceObject $dataA -DifferenceObject $dataB -Property $columnNames -PassThru

    # 3. 針對每一筆差異，找出具體是哪個 Column 不同
    $finalReport = foreach ($item in $diffResults) {
        $diffCols = New-Object System.Collections.Generic.List[string]
        
        # 根據組合鍵找到另一台伺服器對應的那一行
        if ($item.SideIndicator -eq "<=") {
            # 當前是 Server A，去 Server B 找對應行
            $otherRow = $dataB | Where-Object { $_.$key1 -eq $item.$key1 -and $_.$key2 -eq $item.$key2 }
        } else {
            # 當前是 Server B，去 Server A 找對應行
            $otherRow = $dataA | Where-Object { $_.$key1 -eq $item.$key1 -and $_.$key2 -eq $item.$key2 }
        }

        # 如果找得到對應行，就比對 100+ 個欄位
        if ($null -ne $otherRow) {
            foreach ($colName in $columnNames) {
                if ("$($item.$colName)" -ne "$($otherRow.$colName)") {
                    $diffCols.Add($colName)
                }
            }
        } else {
            $diffCols.Add("!!! 另一台伺服器缺失此 Key !!!")
        }

        # 4. 將原本的資料加上「差異欄位標註」
        $item | Select-Object @{N='SideIndicator'; E={$_.SideIndicator}}, 
                            @{N='Different_Columns'; E={$diffCols -join ", "}}, 
                            *
    }

    # 5. 輸出結果
    if ($null -eq $finalReport) {
        Write-Host "✅ 恭喜！兩個環境的數據完全一致。" -ForegroundColor Green
    } else {
        Write-Host "❌ 發現差異！已標註不同之處。" -ForegroundColor Red
        $finalReport | Out-GridView -Title "SP 比對結果 (<=:ServerA, =>:ServerB | 請查看 Different_Columns 欄位)"
    }
}
catch {
    Write-Error "執行失敗: $($_.Exception.Message)"
}
