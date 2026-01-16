# 1. 設定兩邊的連線資訊
$testDate = "2026-01-16"
$paramName = "@YourParamName"
$spName = "YourStoredProcedureName"

# 伺服器 A (來源/舊環境)
$connStrA = "Server=ServerA;Database=DB_A;Integrated Security=True;"
# 伺服器 B (目標/新環境)
$connStrB = "Server=ServerB;Database=DB_B;Integrated Security=True;"

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
    Write-Host "⏳ 正在從 Server A 抓取數據..." -ForegroundColor Gray
    $dataA = Get-SpData $connStrA $spName $paramName $testDate

    Write-Host "⏳ 正在從 Server B 抓取數據..." -ForegroundColor Gray
    $dataB = Get-SpData $connStrB $spName $paramName $testDate

    # 2. 比較數據
    # 注意：Compare-Object 會比較每一行的內容
    Write-Host "🔍 正在對比結果..." -ForegroundColor Cyan
    $diff = Compare-Object -ReferenceObject $dataA -DifferenceObject $dataB -PassThru

    if ($null -eq $diff) {
        Write-Host "✅ 完美！兩個伺服器的結果完全一致。" -ForegroundColor Green
    } else {
        Write-Host "❌ 發現差異！" -ForegroundColor Red
        $diff | Out-GridView -Title "SP 結果差異對照表 (Side Indicator => 代表 Server B, <= 代表 Server A)"
    }
}
catch {
    Write-Error "執行失敗: $($_.Exception.Message)"
}
