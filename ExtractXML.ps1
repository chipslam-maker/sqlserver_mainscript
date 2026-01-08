# 設定
$serverName = "YOUR_SERVER"
$dbName     = "YOUR_DATABASE"
$query      = "SELECT TOP 1 XmlCol FROM YourTable WHERE ... " # 你的查詢
$tempFilePath = Join-Path $env:TEMP "InvItemDetail_$(Get-Date -Format 'yyyyMMddHHmmss').xml"

# 建立連接並取得資料
$connectionString = "Server=$serverName;Database=$dbName;Integrated Security=True;TrustServerCertificate=True"
$connection = New-Object Microsoft.Data.SqlClient.SqlConnection($connectionString)
$command = $connection.CreateCommand()
$command.CommandText = $query

try {
    $connection.Open()
    $xmlContent = $command.ExecuteScalar() # 因為只取一個 Column 的值
    
    if ($null -ne $xmlContent) {
        # 寫入 Temp Folder 中的檔案 (使用 UTF8 確保中文不亂碼)
        $xmlContent | Out-File -FilePath $tempFilePath -Encoding utf8
        Write-Host "檔案已成功寫入：$tempFilePath" -ForegroundColor Green
    }
}
finally {
    $connection.Close()
}

# 載入 XML 檔案
[xml]$xmlData = Get-Content -Path $tempFilePath

# 根據你之前的結構：<InvItem><InvItemDetail><Params><P name="UniqueId">1234</P>...
# 我們來找出 UniqueId 的值

# 方法 A：直接路徑存取
$uniqueId = $xmlData.InvItem.InvItemDetail.Params.P | Where-Object { $_.name -eq "UniqueId" } | Select-Object -ExpandProperty "#text"

Write-Host "分析結果 - UniqueId: $uniqueId" -ForegroundColor Cyan

# 方法 B：使用 XPath (處理複雜層級最快)
$node = $xmlData.SelectSingleNode("//P[@name='UniqueId']")
if ($null -ne $node) {
    $val = $node.'#text'
    Write-Host "XPath 取得的值: $val"
}
