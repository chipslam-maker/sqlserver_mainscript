-- 1. 產生建立 Folder 的腳本
SELECT DISTINCT 
    'IF NOT EXISTS (SELECT 1 FROM [catalog].[folders] WHERE name = N''' + f.name + ''') 
    EXEC [catalog].[create_folder] @folder_name=N''' + f.name + ''';' AS Script
FROM [SSISDB].[catalog].[folders] f
WHERE f.name IN ('TEST', 'PRO')

UNION ALL

-- 2. 產生建立 Environment 的腳本
SELECT DISTINCT 
    'IF NOT EXISTS (SELECT 1 FROM [catalog].[environments] e JOIN [catalog].[folders] f ON e.folder_id = f.folder_id WHERE f.name = N''' + f.name + ''' AND e.name = N''' + e.name + ''') 
    EXEC [catalog].[create_environment] @folder_name=N''' + f.name + ''', @environment_name=N''' + e.name + ''';'
FROM [SSISDB].[catalog].[folders] f
JOIN [SSISDB].[catalog].[environments] e ON f.folder_id = e.folder_id
WHERE f.name IN ('TEST', 'PRO')

UNION ALL

-- 3. 產生建立 Variables 的腳本
SELECT 
    'EXEC [catalog].[create_environment_variable] 
        @variable_name=N''' + v.name + ''', 
        @data_type=N''' + v.type + ''', 
        @environment_name=N''' + e.name + ''', 
        @folder_name=N''' + f.name + ''', 
        @value=' + CASE 
            WHEN v.sensitive = 1 THEN 'NULL -- <!! SENSITIVE VALUE !!>' 
            WHEN v.value IS NULL THEN 'NULL' 
            ELSE 'N''' + REPLACE(CAST(v.value AS NVARCHAR(MAX)), '''', '''''') + '''' 
        END + ', 
        @sensitive=' + CAST(v.sensitive AS VARCHAR(1)) + ';'
FROM [SSISDB].[catalog].[folders] f
JOIN [SSISDB].[catalog].[environments] e ON f.folder_id = e.folder_id
JOIN [SSISDB].[catalog].[environment_variables] v ON e.environment_id = v.environment_id
WHERE f.name IN ('TEST', 'PRO');
