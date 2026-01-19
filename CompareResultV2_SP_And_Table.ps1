# ==========================================
# PET 萬用比對工具 (支援 Table/SP 混合模式)
# ==========================================

$currentFolder = $PSScriptRoot
$configPath = Join-Path -Path $currentFolder -ChildPath "config.json"

if (-not (Test-Path $configPath)) {
    Write-Host "❌ 錯誤: 找不到 config.json！" -ForegroundColor Red
    return
}

# 基礎連線設定
$serverA = "ServerA"
$serverB = "ServerB"
$dbA = "DB_A"
$dbB = "DB_B"
$connStrA = "Server=$serverA;Database=$dbA;Integrated Security=True;TrustServerCertificate=True;"
$connStrB = "Server=$serverB;Database=$dbB;Integrated Security=True;TrustServerCertificate=True;"

# 通用抓取數據函式
function Get-DataFromSource($connStr, $config) {
    $conn = New-Object System.Data.SqlClient.SqlConnection($connStr)
    $cmd = New-Object System.Data.SqlClient.SqlCommand
    $cmd.Connection = $conn
    $cmd.CommandTimeout = 120 # 預防 100+ 欄位的大資料跑太久

    if (-not [string]::IsNullOrEmpty($config.StoreProcedure)) {
        # --- SP 模式 ---
        $cmd.CommandText = $config.StoreProcedure
        $cmd.CommandType = [System.Data.CommandType]::StoredProcedure
        
        # 動態加入 Param1 (如果存在)
        if (-not [string]::IsNullOrEmpty($config.Param1Name)) {
            $cmd.Parameters.AddWithValue($config.Param1Name, $config.Param1Value) | Out-Null
        }
        # 動態加入 Param2 (如果存在)
        if (-not [string]::IsNullOrEmpty($config.Param2Name)) {
            $cmd.Parameters.AddWithValue($config.Param2Name, $config.Param2Value) | Out-Null
        }
    } else {
        # --- Table 模式 ---
        $cmd.CommandText = "SELECT * FROM $($config.TableName)"
        $cmd.CommandType = [System.Data.CommandType]::Text
    }

    $da = New-Object System.Data.SqlClient.SqlDataAdapter($cmd)
    $dt = New-Object System.Data.DataTable
    $da.Fill($dt) | Out-Null
    $conn.Close()
    return $dt
}

# 讀取 JSON 並開始循環
$tableConfigs = Get-Content $configPath -Raw | ConvertFrom-Json

foreach ($config in $tableConfigs) {
    $displayName = if ($config.StoreProcedure) { $config.StoreProcedure } else { $config.TableName }
    Write-Host "`n🚀 [PET] 正在處理: $displayName" -ForegroundColor Cyan

    try {
        $dataA = Get-DataFromSource $connStrA $config
        $dataB = Get-DataFromSource $connStrB $config

        $columnNames = $dataA.Columns | Select-Object -ExpandProperty ColumnName
        $key1 = $config.Key1
        $key2 = $config.Key2

        # 1. 內容比對
        $diffResults = Compare-Object -ReferenceObject $dataA -DifferenceObject $dataB -Property $columnNames -PassThru

        if ($null -eq $diffResults) {
            Write-Host "✅ [一致] $displayName 數據完全相同。" -ForegroundColor Green
            continue
        }

        # 2. 差異欄位標註
        $finalReport = foreach ($item in $diffResults) {
            $diffCols = New-Object System.Collections.Generic.List[string]
            
            # 彈性組合鍵匹配邏輯
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
                $diffCols.Add("!!! 另一伺服器缺失此 Key ($key1, $key2) !!!")
            }

            $item | Select-Object @{N='SideIndicator'; E={$_.SideIndicator}}, 
                                @{N='Different_Columns'; E={$diffCols -join ", "}}, 
                                *
        }

        Write-Host "❌ [差異] $displayName 發現數據不一致！" -ForegroundColor Red
        $finalReport | Out-GridView -Title "PET 差異比對: $displayName"
    }
    catch {
        Write-Host "⚠️ [錯誤] 無法處理 $displayName : $($_.Exception.Message)" -ForegroundColor Red
    }
}

Write-Host "`n🏁 [PET] 所有任務已完成！" -ForegroundColor Yellow
