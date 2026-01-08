$query = @"
Select 
TABKD from 
TABLE
WHERE 
Processdate>='2025-01-01'
AND
Processdate<='2025-02-01'
"@

# 定義正則表達式：
# (?<Field>\w+) : 抓取欄位名稱
# (?<Op>[>=<]{1,2}) : 抓取運算子 (如 >=, <=, =)
# '(?<Value>[^']+)' : 抓取單引號內的值
$pattern = "(?i)(?<Field>Processdate)\s*(?<Op>[>=<]{1,2})\s*'(?<Value>[^']+)'"

# 尋找所有符合的項目
$results = [regex]::Matches($query, $pattern)

foreach ($match in $results) {
    $fieldName  = $match.Groups['Field'].Value
    $operator   = $match.Groups['Op'].Value
    $fieldValue = $match.Groups['Value'].Value

    Write-Host "找到條件 ---" -ForegroundColor Cyan
    Write-Host "欄位: $fieldName"
    Write-Host "符號: $operator"
    Write-Host "數值: $fieldValue"
    Write-Host "----------------"
}
