# 設定資料庫連接資訊
$oldDbConn = "Server=OldServer;Database=MyDB;Integrated Security=True;"
$newDbConn = "Server=NewServer;Database=MyDB;Integrated Security=True;"
$query = "SELECT * FROM YourTable ORDER BY ID" # 建議一定要有 ORDER BY 確保比對順序一致

# 取得資料的函數
function Get-SqlData {
    param($connectionString, $query)
    $connection = New-Object System.Data.SqlClient.SqlConnection($connectionString)
    $command = $connection.CreateCommand()
    $command.CommandText = $query
    $adapter = New-Object System.Data.SqlClient.SqlDataAdapter($command)
    $dataset = New-Object System.Data.DataSet
    $adapter.Fill($dataset) | Out-Null
    $connection.Close()
    return $dataset.Tables[0]
}

# 1. 抓取資料
$oldData = Get-SqlData -connectionString $oldDbConn -query $query
$newData = Get-SqlData -connectionString $newDbConn -query $query

# 2. 檢查 Row Count (筆數)
if ($oldData.Rows.Count -ne $newData.Rows.Count) {
    Write-Host "❌ 筆數不一致！舊的: $($oldData.Rows.Count), 新的: $($newData.Rows.Count)" -ForegroundColor Red
} else {
    Write-Host "✅ 筆數一致: $($oldData.Rows.Count)" -ForegroundColor Green
}

# 3. 完全比對 (包括所有 Column 內容)
$diff = Compare-Object $oldData $newData -Property ( $oldData.Columns.ColumnName ) -PassThru

# 假設 $diff 是你 Compare-Object 的結果
if ($diff) {
    $diff | ForEach-Object {
        $indicator = $_.SideIndicator
        $data = $_
        
        if ($indicator -eq "=>") {
            Write-Host "新增行 (只在新 DB):" -ForegroundColor Green
            $data | Select-Object * -ExcludeProperty SideIndicator | Format-Table
        }
        elseif ($indicator -eq "<=") {
            Write-Host "缺失行 (只在舊 DB):" -ForegroundColor Red
            $data | Select-Object * -ExcludeProperty SideIndicator | Format-Table
        }
    }
}

if ($null -eq $diff) {
    Write-Host "🎉 恭喜！新舊資料庫的 Query 結果完全一致。" -ForegroundColor Cyan
} else {
    Write-Host "⚠️ 發現差異記錄：" -ForegroundColor Yellow
    $diff | Select-Object *, @{N='Source'; E={if($_.SideIndicator -eq '<='){"Old DB"}else{"New DB"}}} | Out-GridView
}
