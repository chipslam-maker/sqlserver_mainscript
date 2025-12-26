-- 找出 TEST 資料夾下所有變數的定義
SELECT 
    'EXEC [catalog].[create_environment_variable] 
        @variable_name=N''' + v.name + ''', 
        @data_type=N''' + v.type + ''', 
        @environment_name=N''' + e.name + ''', 
        @folder_name=N''' + f.name + ''', 
        @value=' + CASE WHEN v.value IS NULL THEN 'NULL' ELSE 'N''' + CAST(v.value AS NVARCHAR(MAX)) + '''' END + ', 
        @sensitive=' + CAST(v.sensitive AS VARCHAR(1)) + ', 
        @description=N''' + ISNULL(v.description, '') + ''';' AS ScriptToRunOnServerB
FROM [SSISDB].[catalog].[folders] f
JOIN [SSISDB].[catalog].[environments] e ON f.folder_id = e.folder_id
JOIN [SSISDB].[catalog].[environment_variables] v ON e.environment_id = v.environment_id
WHERE f.name IN ('TEST', 'PRO') -- 這裡指定你要的資料夾
