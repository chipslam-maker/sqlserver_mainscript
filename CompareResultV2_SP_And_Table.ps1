# ==========================================
# PET 批次數據比對工具 (支援 JSON Config)
# ==========================================

# 1. 基礎連線設定 (已加入 Cert Trust)
$serverA = "ServerA"
$serverB = "ServerB"
$dbA = "DB_A"
$dbB = "DB_B"
$connStrA = "Server=$serverA;Database=$dbA;Integrated Security=True;TrustServerCertificate=True;"
$connStrB = "Server=$serverB;Database=$dbB;Integrated Security=True;TrustServerCertificate=True;"

# 2. 讀取 Config 檔案
$configPath = "C:\YourPath\config.json" # 請修改為你的實際路徑
$tableConfigs = Get-Content $configPath | ConvertFrom-Json

function Get-SqlData($connStr, $tableName) {
    $conn = New-Object System.Data.SqlClient.SqlConnection($connStr)
    $sql = "SELECT * FROM $tableName"
    $cmd = New-Object System.Data.SqlClient.SqlCommand($sql, $conn)
    $da = New-Object System.Data.SqlClient.SqlDataAdapter($cmd)
    $dt = New-Object System.Data.DataTable
    $da.Fill($dt) | Out-Null
    return $dt
}

# 3. 循環比對所有 Table
foreach ($config in $tableConfigs) {
    $targetTable = $config.TableName
    $key1 = $config.Key1
    $key2 = $config.Key2

    Write-Host "--------------------------------------------" -ForegroundColor Yellow
    Write-Host "🚀 [PET] 正在處理 Table: $targetTable" -ForegroundColor Cyan

    try {
        $dataA = Get-SqlData $connStrA $targetTable
        $dataB = Get-SqlData $connStrB $targetTable

        $columnNames = $dataA.Columns | Select-Object -ExpandProperty ColumnName
        
        # 初步內容比對
        $diffResults = Compare-Object -ReferenceObject $dataA -DifferenceObject $dataB -Property $columnNames -PassThru

        if ($null -eq $diffResults) {
            Write-Host "✅ [一致] $targetTable 在兩個環境完全相同。" -ForegroundColor Green
            continue
        }

        # 分析細節差異
        $finalReport = foreach ($item in $diffResults) {
            $diffCols = New-Object System.Collections.Generic.List[string]
            
            # 彈性組合鍵匹配
            $matchLogic = { 
                $_.$key1 -eq $item.$key1 -and 
                (
                    ([string]::IsNullOrEmpty($key2)) -or 
                    ($null -ne $item.$key2 -and $_.$key2 -eq $item.$key2) -or 
                    ($null -eq $item.$key2 -and $null -eq $_.$key2)
                )
            }

            $otherRow = if ($item.SideIndicator -eq "<=") { 
                $dataB | Where-Object $matchLogic | Select-Object -First 1 
            } else { 
                $dataA | Where-Object $matchLogic | Select-Object -First 1 
            }

            if ($null -ne $otherRow) {
                foreach ($colName in $columnNames) {
                    if ("$($item.$colName)" -ne "$($otherRow.$colName)") {
                        $diffCols.Add($colName)
                    }
                }
            } else {
                $diffCols.Add("!!! 缺失組合鍵 ($key1, $key2) !!!")
            }

            $item | Select-Object @{N='SideIndicator'; E={$_.SideIndicator}}, 
                                @{N='Different_Columns'; E={$diffCols -join ", "}}, 
                                *
        }

        # 顯示該 Table 的差異結果
        Write-Host "❌ [差異] $targetTable 發現差異資料！" -ForegroundColor Red
        $finalReport | Out-GridView -Title "PET 差異比對: $targetTable (<=A, =>B)"

    }
    catch {
        Write-Host "⚠️ [錯誤] 無法處理 Table $targetTable : $($_.Exception.Message)" -ForegroundColor Red
    }
}

Write-Host "--------------------------------------------" -ForegroundColor Yellow
Write-Host "🏁 所有比對任務已完成！" -ForegroundColor Green
