# --- 1. CONFIG.JSON 中仍然是 "localhost" ---
# 確保 $SqlServer 變數在腳本中讀取後是 "localhost"

# --- 2. 修改腳本中建立 ServerConnection 的部分 ---
Write-Host "Connecting to [$SqlServer] via forced TCP/IP (TrustServerCertificate=True)..." -ForegroundColor Cyan

try {
    # 創建 ServerConnection 物件，並強制使用 TCP 協定
    $SqlServerNameWithProtocol = "tcp:" + $SqlServer 
    $sc = New-Object Microsoft.SqlServer.Management.Common.ServerConnection($SqlServerNameWithProtocol)
    
    # 設定加密和信任憑證
    $sc.Encrypt = $true  
    $sc.TrustServerCertificate = $true 
    $sc.LoginSecure = $true # 使用 Windows 驗證
    
    # 建立 SMO Server 物件
    $srv = New-Object Microsoft.SqlServer.Management.SMO.Server($sc)
    
    # 嘗試連線
    $srv.ConnectionContext.Connect()
    
    if (-not $srv.ConnectionContext.IsConnected) {
        throw "Failed to establish connection to $SqlServer."
    }
    # ... (後續的 $db 和 $OriginalTableSMO 邏輯不變) ...
}
catch {
    Write-Error "SMO Connection Error: $($_.Exception.Message)"
    exit 1
}
