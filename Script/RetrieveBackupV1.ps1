+
#--------------------------------------------------------------Parameters
Param(
    [string]$RestoreInstance,
    [string]$MonitoringServer, 
    [string]$DatabaseReportStore, 
    [string]$DestinationPath,
    [string]$LogFilePath,
    [string]$DataFilePath,
    [string]$ErrorFile
)

$myErrorFile = $ErrorFile # "U:\Databases\Temp\BackupTextResult.txt"
$myRestoreInstance = $RestoreInstance # "DB-BK-DBV02.SAIPACORP.COM\NODE,49149"
$myDestinationPath = $DestinationPath # "\\DB-BK-DBV02\U$\Databases\Backup\"
$myMonitoringServer = $MonitoringServer #"DB-MN-DLV02.SAIPACORP.COM\NODE,49149" 
$myDataFilePath = $DataFilePath # "F:\Data01\Databases\Data"
$myLogFilePath = $LogFilePath # "F:\Log01\Databases\Log"
$DatabaseReportStore = $DatabaseReportStore #"SqlDeep"
$myMaximumTryCountToFindUncheckedBackup = 5

#--------------------------------------------------------------Functions
#>
# This function generated random date time 
Function GenerateRandomDate {
    Param
    (
        [parameter (Mandatory = $True)][int32]$MinNumber ,
        [parameter (Mandatory = $True)][int32]$MaxNumber 
    )
    [Datetime]$RandomDateTime = (Get-Date).AddDays( - (Get-Random -Minimum($MinNumber) -Maximum($MaxNumber))) 
    Return $RandomDateTime
}
# Function For Check Randoum Date
Function IsTested {
    param 
    (
        [parameter(Mandatory = $true)][string]$InstanceName,
        [parameter(Mandatory = $true)][Datetime]$RecoveryDateTime,
        [parameter(Mandatory = $true)][string]$DatabaseName,
        [parameter(Mandatory = $true)][string]$RestoreInstance,
        [parameter(Mandatory = $true)][string]$DatabaseReportStore
        
    )
    $myQuery = 
    "
    DECLARE @myHashValue AS INT
    DECLARE @myRecoveryDateTime AS DateTime
    DECLARE @myDBName AS NVARCHAR(50)

	SET @myHashValue = BINARY_CHECKSUM('"+ $DatabaseName + "','"+ $InstanceName + "')
    SET @myRecoveryDateTime = CAST('" + $RecoveryDateTime + "' AS DATETIME)

    SELECT COUNT(1) As myResult
    FROM [dbo].[BackupTestResult]
    Where [HashValue] = @myHashValue 
    AND @myRecoveryDateTime BETWEEN [BackupStartTime] AND [BackupRestoredTime]
    "
    $myResultCheckDate = Invoke-Sqlcmd -ServerInstance $InstanceName -Database $DatabaseReportStore -Query $myQuery -OutputSqlErrors $true -OutputAs DataRows
    if ($myResultCheckDate[0] -eq 0 ) {
        $myResult = $false
    }
    else {
        $myResult = $true
    }
    return $myResult
}
# Function For Test Path Directory
Function TestPath ([parameter(Mandatory = $true)][string]$BackupPath) {
    $pathExists = Test-Path -Path $BackupPath.myPath.Substring(0, ($BackupPath.myPath.Length - 4))
    return $pathExists
}
# This function Find Path Backup

Function GetBackupFiles {
    Param (
        [parameter (Mandatory = $True)][string]$InstanceName,
        [parameter (Mandatory = $True)][string]$DatabaseName,
        [parameter (Mandatory = $True)][Datetime]$BackupDate,
        [parameter (Mandatory = $True)][string]$LogFilePath,
        [parameter (Mandatory = $True)][string]$DataFilePath,
        [parameter (Mandatory = $True)][string]$DomainName,
        [parameter (Mandatory = $True)][int32]$ExecutionId
    )
    $myFullBackupQuery = "IF OBJECT_ID('tempdb.dbo.#myResult', 'U') IS NOT NULL
    DROP TABLE #myResult;;

    DECLARE @myRecoveryDate AS NVARCHAR(50);
    DECLARE @myDBName AS NVARCHAR(50);
    DECLARE @myStartLsn NUMERIC(25, 0);
    DECLARE @LogicalName NVARCHAR(100);
    DECLARE @myUNCPath NVARCHAR(100);
    DECLARE @physicalLogAddress NVARCHAR(50);
    DECLARE @physicalDataAddress NVARCHAR(50);
    DECLARE @myMoveCommand NVARCHAR(MAX);
    DECLARE @myCompleteCommand NVARCHAR(MAX);

    SET @myRecoveryDate = CAST('" + $BackupDate.ToString() + "' AS DATETIME);
    SET @myDBName = N'"+ $DatabaseName + "';
    SET @physicalLogAddress = N'"+ $LogFilePath + "\';
    SET @physicalDataAddress = N'"+ $DataFilePath + "\';
    SET @myMoveCommand = CAST(N'' AS NVARCHAR(MAX));
    CREATE TABLE #myResult
    (
        ID INT IDENTITY,
        DatabaseName NVARCHAR(50),
        FILEPATH NVARCHAR(255),
        Position INT,
        BackupStartTime DATETIME,
        BackupFinishTime DATETIME,
        FirstLsn NUMERIC(25, 0),
        LastLsn NUMERIC(25, 0),
        BackupType CHAR(1),
        MediaSetId INT,
        BackupSourcePath NVARCHAR(MAX)
    );
    INSERT INTO #myResult
    ( DatabaseName, FILEPATH, Position, BackupStartTime, BackupFinishTime, FirstLsn, LastLsn, BackupType, MediaSetId)
    SELECT TOP 1 WITH TIES
        myDatabase.name AS DatabaseName,
        myBackupFamily.physical_device_name AS myLogPath,
        myBackupset.position,
        myBackupset.backup_start_date,
        myBackupset.backup_finish_date,
        myBackupset.first_lsn,
        myBackupset.last_lsn,
        myBackupset.[type],
        myBackupset.[media_set_id]
    FROM
        master.sys.databases AS myDatabase WITH (READPAST)
		INNER JOIN msdb.dbo.backupset AS myBackupset WITH (READPAST) ON myBackupset.database_name = myDatabase.name
        INNER JOIN msdb.dbo.backupmediafamily AS myBackupFamily WITH (READPAST) ON myBackupFamily.media_set_id = myBackupset.media_set_id
    WHERE myBackupset.is_copy_only = 0
        AND myBackupset.[type] = 'D'
        AND myDatabase.[name] = @myDBName 
        AND @myRecoveryDate >= myBackupset.backup_start_date 
		AND @myRecoveryDate >= myBackupset.backup_finish_date
    ORDER BY myBackupset.backup_start_date DESC;
    -----------------------------------
    SET @myStartLsn =
    (
        SELECT MAX(FirstLsn) AS myStartLsn FROM #myResult WHERE BackupType = 'D'
    );
    INSERT INTO #myResult
    (DatabaseName, FILEPATH, Position, BackupStartTime, BackupFinishTime, FirstLsn, LastLsn, BackupType, MediaSetId)
    SELECT TOP 1 WITH TIES
        myDatabase.name AS DatabaseName,
        myBackupFamily.physical_device_name AS myLogPath,
        myBackupset.position,
        myBackupset.backup_start_date,
        myBackupset.backup_finish_date,
        myBackupset.first_lsn,
        myBackupset.last_lsn,
        myBackupset.[type],
        myBackupset.[media_set_id]
    FROM
        master.sys.databases AS myDatabase WITH (READPAST)    INNER JOIN msdb.dbo.backupset AS myBackupset WITH (READPAST)        ON myBackupset.database_name = myDatabase.name
        INNER JOIN msdb.dbo.backupmediafamily AS myBackupFamily WITH (READPAST)        ON myBackupFamily.media_set_id = myBackupset.media_set_id
    WHERE myBackupset.is_copy_only = 0
        AND myBackupset.[type] = 'I'
        AND myDatabase.[name] = @myDBName
        AND myBackupset.backup_finish_date <= @myRecoveryDate
        AND myBackupset.first_lsn >= @myStartLsn
    ORDER BY myBackupset.first_lsn DESC;
    -----------------------------------
    SET @myStartLsn =
    (
        SELECT MAX(FirstLsn) AS myStartLsn
        FROM #myResult
        WHERE BackupType IN ( 'D', 'I' )
    );
    INSERT INTO #myResult
    (DatabaseName,FILEPATH, Position, BackupStartTime, BackupFinishTime, FirstLsn, LastLsn, BackupType, MediaSetId)
    SELECT myDatabase.name AS DatabaseName,
        myBackupFamily.physical_device_name AS myLogPath,
        myBackupset.position,
        myBackupset.backup_start_date,
        myBackupset.backup_finish_date,
        myBackupset.first_lsn,
        myBackupset.last_lsn,
        myBackupset.[type],
        myBackupset.[media_set_id]
    FROM
        master.sys.databases AS myDatabase WITH (READPAST)    INNER JOIN msdb.dbo.backupset AS myBackupset WITH (READPAST)        ON myBackupset.database_name = myDatabase.name
        INNER JOIN msdb.dbo.backupmediafamily AS myBackupFamily WITH (READPAST)        ON myBackupFamily.media_set_id = myBackupset.media_set_id
    WHERE myBackupset.is_copy_only = 0
        AND myBackupset.[type] = 'L'
        AND myDatabase.[name] = @myDBName
        AND myBackupset.backup_start_date <= @myRecoveryDate
        AND myBackupset.first_lsn >= @myStartLsn
    ORDER BY myBackupset.first_lsn ASC;
    ------------------------------------
    DECLARE @myBackupFiles TABLE
    (
        MediaSetId INT,
        BackupSourcePath NVARCHAR(MAX)
    );
    INSERT INTO @myBackupFiles
    SELECT MediaSetId,
        FileList = STUFF(
                    (	SELECT CONCAT(', DISK=''', FILEPATH, '''')
                        FROM #myResult AS myFilteredTable
                        WHERE myFilteredTable.MediaSetId = myTable.MediaSetId
                        FOR XML PATH('')
                    ), 1, 1,'')
    FROM #myResult AS myTable
    GROUP BY MediaSetId;

    SELECT @myMoveCommand
        = CONCAT(@myMoveCommand,'MOVE N''', name,''' TO N''', CONCAT(IIF(type_desc = 'LOG', @physicalLogAddress, @physicalDataAddress),name,'.', RIGHT(physical_name, 3)),''',')
    FROM sys.master_files
    WHERE database_id = DB_ID(@myDBName);
    -------------------------------------
    SELECT 
        CAST("+ $ExecutionId.ToString() + " AS INT) AS ExecutionId,
        myTable.ID,
        myTable.DatabaseName,
        myTable.FILEPATH,
        CASE BackupType
            WHEN 'D' THEN	'Full'
            WHEN 'I' THEN   'Differential'
            WHEN 'L' THEN   'Log'
            ELSE             NULL
        END AS BackupType,
        RIGHT(FILEPATH, CHARINDEX('\', REVERSE(FILEPATH))-1) AS [FileName],
        CONCAT('\\',SUBSTRING(@@SERVERNAME, 1, LEN(@@SERVERNAME) - LEN(@@SERVICENAME) - 1),'\',LEFT(FILEPATH, 1),'$',RIGHT(FILEPATH, (LEN(FILEPATH) - 2))) AS UNCPath,
        myTable.Position,
        myTable.MediaSetId,
        myTable.BackupStartTime,
        myBackupPath.BackupSourcePath,
        CAST(SERVERPROPERTY('MachineName') AS NVARCHAR(128))+'.'+'"+ $myDomainName + "'+'\'+ CAST(SERVERPROPERTY('InstanceName') AS NVARCHAR(128))+',49149' AS InstanceName,
        CONCAT('RESTORE DATABASE [', @myDBName,'] FROM',myBackupPath.BackupSourcePath,' WITH NORECOVERY, ',@myMoveCommand,' STATS = 5') AS myCompleteFullCommand
    FROM
        @myBackupFiles AS myBackupPath    INNER JOIN #myResult AS myTable        ON myTable.MediaSetId = myBackupPath.MediaSetId;"
    $myResultQuery = Invoke-Sqlcmd -ServerInstance $InstanceName -Database $DatabaseName -Query $myFullBackupQuery -OutputSqlErrors $true -OutputAs DataTables
    return $myResultQuery
}
# This function for Restore Command
Function RestoreFullCommand {
    Param (

        [parameter(Mandatory = $true)][string]$DatabaseName,
        [parameter(Mandatory = $true)][string]$DestinationPath,
        [parameter(Mandatory = $true)][string]$RestoreInstance,
        [parameter(Mandatory = $true)][INT]$ExecutionId
    )
    # $UNCPath = $UNCPath.Replace("'","''")


    $myBackupSourcePath = "  
    DECLARE @myDBName AS NVARCHAR(100)
    DECLARE @myNewDeviceName NVARCHAR(100)
    DECLARE @myExecutionId INT

    SET @myExecutionId = CAST('" + $ExecutionId.ToString() + "' AS INT);
    SET @myNewDeviceName = '"+ $DestinationPath + "' --''\\DB-BK-DBV02\U$\Databases\Backup\'
    SET @myDBName = '"+ $DatabaseName + "' 
    DECLARE @myBackupFiles TABLE (MediaSetId INT, BackupSourcePath nvarchar(MAX))
    INSERT INTO @myBackupFiles  
    SELECT
        MediaSetId,
        FileList = STUFF(
        (SELECT  CONCAT(', DISK=''' ,@myNewDeviceName,REVERSE(SUBSTRING(REVERSE(FILEPATH), 1, CHARINDEX('\', REVERSE(FILEPATH)) - 1)) ,'''')  FROM [dbo].[BackupPathResult] AS myFilteredTable WHERE myFilteredTable.MediaSetId = myTable.MediaSetId GROUP BY     FILEPATH,MediaSetId FOR XML PATH(''))
        , 1, 1, '')
    FROM
        [dbo].[BackupPathResult] AS myTable
        WHERE 
        myTable.ExecutionId =  @myExecutionId
        AND myTable.DatabaseName = @myDBName
		AND myTable.[BackupType] ='FULL'
    GROUP BY
        MediaSetId
        ,myTable.backupType

    SELECT BackupSourcePath FROM @myBackupFiles
    "
   
    $BackupSourcePath = Invoke-Sqlcmd -ServerInstance $RestoreInstance -Database "tempdb" -Query $myBackupSourcePath #-OutputAs DataTables
    $myMoveCommand = "RESTORE FILELISTONLY FROM " + $BackupSourcePath.BackupSourcePath + ""
    $myResult = Invoke-Sqlcmd -ServerInstance $RestoreInstance -Database "master" -Query $myMoveCommand -OutputAs DataTables
    Invoke-Sqlcmd -ServerInstance $RestoreInstance -Database "tempdb" -Query "TRUNCATE TABLE [tempdb].dbo.FileListInfo" 
    #  TruncateTable -InstanceName $RestoreInstance -SchemaName "dbo" -TableName "FileListInfo" 
    $myResult | Select-Object -Property LogicalName, Type, physicalname | Write-SqlTableData -ServerInstance $RestoreInstance -DatabaseName "tempdb" -SchemaName "dbo" -TableName "FileListInfo" -Force
    $myLogicalDevice = $BackupSourcePath.BackupSourcePath.replace("'", "''")
    $myRestoreFullCommand = "
    DECLARE @myDBName AS NVARCHAR(100)
    DECLARE @physicalLogAddress NVARCHAR(50)
    DECLARE @physicalDataAddress NVARCHAR(50)
    DECLARE @myMoveCommand NVARCHAR(MAX)
    DECLARE @myCompleteCommand NVARCHAR(MAX)
    DECLARE @myStringDate NVARCHAR(50)
	DECLARE @myExecutionId INT 
    DECLARE @myLogicalDeviceName NVARCHAR(MAX)

    SET @myLogicalDeviceName = N'"+ $myLogicalDevice + "'
    SET @physicalLogAddress = N'F:\Log01\Databases\Log\'
    SET @physicalDataAddress= N'F:\Data01\Databases\Data\'
    SET @myMoveCommand = CAST(N'' AS NVARCHAR(MAX))
    SET @myCompleteCommand = CAST(N'' AS NVARCHAR(MAX))
    SET @myDBName = '"+ $DatabaseName + "' 
	SET @myExecutionId =CAST('" + $ExecutionId.ToString() + "' AS INT); 

    SELECT 
     @myMoveCommand=CONCAT(@myMoveCommand,'MOVE N''',myfileTable.LogicalName,''' TO N''',CONCAT(IIF([type]='L',@physicalLogAddress,@physicalDataAddress),RIGHT(PhysicalName, CHARINDEX('\', REVERSE(PhysicalName))-1)),''',') --Extention
    FROM
	    [dbo].[FileListInfo] AS myfileTable
    --WHERE 
    --	@myExecutionId = ExecutionId


    SELECT DISTINCT CONCAT('RESTORE DATABASE [' ,@myDBName, '] FROM ',@myLogicalDeviceName ,' WITH NORECOVERY, ' , @myMoveCommand,' STATS = 5') AS myCompleteCommand 
    FROM [dbo].[BackupPathResult] AS myTable
    WHERE [BackupType] = N'FULL'

"   
    $RestoreFull = Invoke-Sqlcmd -ServerInstance $RestoreInstance -Database "tempdb" -Query $myRestoreFullCommand -OutputSqlErrors $true -QueryTimeout 0 -OutputAs DataTables 
    return $RestoreFull
}
Function RestoreDiffCommand {
    Param (
        [parameter(Mandatory = $true)][string]$InstanceName,
        [parameter(Mandatory = $true)] [string]$DatabaseName,
        [parameter(Mandatory = $true)][string]$DestinationPath,
        [parameter(Mandatory = $true)][INT]$ExecutionId
    )
    # $UNCPath = $UNCPath.Replace("'","''")
    $myRestoreCommand = "    
    DECLARE @myDBName AS NVARCHAR(100)
    DECLARE @myNewDeviceName NVARCHAR(100)
    DECLARE @myExecutionId INT

	SET @myExecutionId = CAST('" + $ExecutionId.ToString() + "' AS INT);

    -- SET @myLogicalDeviceName = N'" + $UNCPath + "'
    SET @myNewDeviceName = '"+ $DestinationPath + "'
    SET @myDBName = '"+ $DatabaseName + "'

SELECT
	MAX(CONCAT('RESTORE DATABASE [' ,@myDBName, '] FROM',myTableResult.FileList ,' WITH FILE = ', myTableResult.Position,' , NORECOVERY, CHECKSUM ,  STATS = 5')) As  myCompleteCommand
FROM (
		SELECT
            MAX(myTable.Position) AS Position,
            MAX(myTable.Id) AS Id,
            MediaSetId,
            STUFF(
			   (SELECT  CONCAT(', DISK=''' ,@myNewDeviceName,REVERSE(SUBSTRING(REVERSE(FILEPATH), 1, CHARINDEX('\', REVERSE(FILEPATH)) - 1)) ,'''')  FROM [dbo].[BackupPathResult] AS myFilteredTable WHERE myFilteredTable.MediaSetId = myTable.MediaSetId GROUP BY     FILEPATH,MediaSetId FOR XML PATH(''))
			  , 1, 1, '') AS FileList
		FROM
			 [dbo].[BackupPathResult] AS myTable
		WHERE
			myTable.BackupType ='Differential'
			AND myTable.ExecutionId =  @myExecutionId
			AND myTable.DatabaseName = @myDBName
		GROUP BY
			 MediaSetId
			,myTable.backupType
	) AS myTableResult
    GROUP BY
        myTableResult.Id    ORDER BY
        myTableResult.Id
"
    $RestoreDiff = Invoke-Sqlcmd -ServerInstance $InstanceName -Database "tempdb" -Query $myRestoreCommand -OutputSqlErrors $true -QueryTimeout 0 -OutputAs DataTables 
    return $RestoreDiff
}
Function RestoreLogCommand {
    Param (
        [parameter(Mandatory = $true)][string]$InstanceName,
        [parameter(Mandatory = $true)] [string]$DatabaseName,
        [parameter(Mandatory = $true)][string]$DestinationPath,
        [parameter(Mandatory = $true)][Datetime]$RecoveryDate,
        [parameter(Mandatory = $true)][INT]$ExecutionId
    )
    # $UNCPath = $UNCPath.Replace("'","''")
    $myStringDate = $RecoveryDate.ToString("yyyy-MM-ddTHH:mm:ss")

    $myRestoreCommand = "
    DECLARE @myDBName AS NVARCHAR(100)
    DECLARE @myNewDeviceName NVARCHAR(100)
    DECLARE @myMoveCommand NVARCHAR(MAX)
    DECLARE @myCompleteCommand NVARCHAR(MAX)
    DECLARE @myStringDate NVARCHAR(50)
	DECLARE @myExecutionId INT

	SET @myExecutionId = CAST('" + $ExecutionId.ToString() + "' AS INT);
    SET @myNewDeviceName = '"+ $DestinationPath + "'
    SET @myMoveCommand = CAST(N'' AS NVARCHAR(MAX))
    SET @myCompleteCommand = CAST(N'' AS NVARCHAR(MAX))
    SET @myDBName = '"+ $DatabaseName + "'
    SET @myStringDate = '"+ $myStringDate + "'

    SELECT  
        MAX(CONCAT('RESTORE LOG [' ,@myDBName, '] FROM',myTableResult.FileList ,' WITH FILE = ', myTableResult.Position,' , NORECOVERY, CHECKSUM , STOPAT =''',@myStringDate,'''')) As myCompleteCommand
    FROM (
        SELECT 
            MAX(myTable.Position) AS Position,
            MediaSetId,
            STUFF(
            (SELECT  CONCAT(', DISK=''' ,@myNewDeviceName,REVERSE(SUBSTRING(REVERSE(FILEPATH), 1, CHARINDEX('\', REVERSE(FILEPATH)) - 1)) ,'''')  FROM [dbo].[BackupPathResult] AS myFilteredTable WHERE myFilteredTable.MediaSetId = myTable.MediaSetId GROUP BY     FILEPATH,MediaSetId FOR XML PATH(''))
            , 1, 1, '') AS FileList
        FROM
            [dbo].[BackupPathResult] AS myTable
        WHERE 
            myTable.BackupType = 'LOG'
			AND myTable.ExecutionId =  @myExecutionId
			AND myTable.DatabaseName = @myDBName
        GROUP BY
            MediaSetId
            ,myTable.backupType
    ) AS myTableResult
    GROUP BY
        myTableResult.MediaSetId
    ORDER BY myTableResult.MediaSetId
    "
    
    $RestorLog = Invoke-Sqlcmd -ServerInstance $InstanceName -Database "tempdb" -Query $myRestoreCommand -OutputSqlErrors $true -QueryTimeout 0 
    return $RestorLog
}
function Test-FileLock {
    param (
        [parameter(Mandatory = $true)][string]$Path
    )

    $myFile = New-Object System.IO.FileInfo $Path
    if ((Test-Path -Path $Path) -eq $false) { return $false }
    try {
        $myStream = $myFile.Open([System.IO.FileMode]::Open, [System.IO.FileAccess]::ReadWrite, [System.IO.FileShare]::None)
        if ($myStream) { $myStream.Close() }
        $false
    }
    catch {
        # file is locked by a process.
        return $true
    }
}
# This function for get server Infos
Function GetServerInfo {
    Param (
        [parameter(Mandatory = $true)][string]$InstanceName,
        [parameter(Mandatory = $true)][string]$TargetServer
    )
    $QueryInfo = 
    "
        SELECT  Distinct
            myGroups.name AS ServerGroupName
            ,myServer.server_name AS InstanceName
        FROM
            msdb.dbo.sysmanagement_shared_server_groups_internal As myGroups 
            INNER JOIN msdb.dbo.sysmanagement_shared_registered_servers_internal As myServer  ON myGroups.server_group_id = myServer.server_group_id
        WHERE
            myServer.server_name NOT IN('$InstanceName' ,'$TargetServer' , 'DB-SH-DLV01.saipacorp.com\SHAREPOINT,49149','DB-TEST-DTV04.SAIPACORP.COM\NODE,49149')
            AND myGroups.name = 'SQL 2019'
  
    "
    $myServerList = Invoke-Sqlcmd -ServerInstance $InstanceName -Database "master" -Query $QueryInfo -OutputSqlErrors $true -OutputAs DataTables
    return $myServerList
}
Function GetDatabaseInfo {
    Param (
        [parameter(Mandatory = $true)][string]$InstanceName
    )
    $myQueryInfo = 
    "
    SELECT 
        [myDatabase].[name]
    FROM 
        master.sys.databases myDatabase WITH (READPAST)
        LEFT OUTER JOIN master.sys.dm_hadr_availability_replica_states AS myHA WITH (READPAST) on myDatabase.replica_id=myHa.replica_id
    WHERE
        [myDatabase].[name] NOT IN ('master','model','msdb','tempdb','SSISDB','SqlDeep','DWQueue','DWDiagnostics','DWConfiguration') 
        AND [myDatabase].[state] = 0
        AND [myDatabase].[source_database_id] IS NULL -- REAL DBS ONLY (Not Snapshots)
        AND [myDatabase].[is_read_only] = 0
        AND ([myHA].[role]=1 or [myHA].[role] is null)
    "
    $myDatabaseList = Invoke-Sqlcmd -ServerInstance $InstanceName -Database "master" -Query $myQueryInfo -OutputSqlErrors $true -OutputAs DataTables
    return $myDatabaseList
}
# After the DB has been restored, run a DBCC CHECKDB it
Function CheckDB {
    Param (
        [parameter(Mandatory = $true)][string]$InstanceName,
        [parameter(Mandatory = $true)][string]$databaseName
    )
    $query = 
    "IF (SELECT state_desc FROM sys.databases  WHERE name = '$databaseName') = 'RESTORING'
        Restore DATABASE $databaseName WITH RECOVERY ;
        DBCC CHECKDB ($databaseName) WITH  NO_INFOMSGS ;
        "

    $ResultCheckTest = Invoke-Sqlcmd -ServerInstance $InstanceName -Database "master" -Query $query  -OutputSqlErrors $true -OutputAs DataTables
    if ($null -eq $ResultCheckTest ) {
        $resultCheck = $true
    }
    else {
        $resultCheck = $false
    }
    return $resultCheck
}
# Save Result in to the table 
Function SaveResult {
    Param (
        [parameter(Mandatory = $true)][string]$BackupInstance,
        [parameter(Mandatory = $true)][string]$DatabaseName,
        [parameter(Mandatory = $true)][ValidateSet("Succseed", "CopyFail", "RestoreFullBackupFail", "RestoreDiffBackupFail", "RestoreLogBackupFail", "CheckDbFail")][string]$TestResult,
        [parameter(Mandatory = $true)][string]$ErrorFileAddress,
        [parameter(Mandatory = $true)][string]$RestoreInstance,
        [parameter(Mandatory = $true)][string]$DatabaseReportStore,
        [parameter(Mandatory = $true)][Datetime]$BackupStartTime,
        [parameter(Mandatory = $true)][Datetime]$RecoveryDate
    )
    

    $myTestResultCode = switch ($TestResult) {
        "Succseed" { 1 }
        "CopyFail" { -1 }
        "RestoreFullBackupFail" { -2 }
        "RestoreDiffBackupFail" { -3 }
        "RestoreLogBackupFail" { -4 }
        "CheckDbFail" { -5 }
        Default { 0 }
    }
    $myTestResultDescription = switch ($TestResult) {
        "Succseed" { "Succseed" }
        "CopyFail" { "Copy backup file(s) failed" }
        "RestoreFullBackupFail" { "Restore full backup failed" }
        "RestoreDiffBackupFail" { "Restore differential backup failed" }
        "RestoreLogBackupFail" { "Restore log backup failed" }
        "CheckDbFail" { "DBCC checkdb failed" }
        Default { "NON" }
    }
    
    $myInsertCommand = 
    "
        DECLARE @myRecoveryDateTime AS DateTime
        DECLARE @myBackupStartTime AS DateTime

        SET @myBackupStartTime = CAST('" + $BackupStartTime + "' AS DATETIME)
        SET @myRecoveryDateTime = CAST('" + $RecoveryDate + "' AS DATETIME)
        INSERT INTO  [dbo].[BackupTestResult]
        ([InstanceName]
            ,[DatabaseName]
            ,[TestResult]
            ,[TestResultDescription]
            ,[BackupRestoredTime]
            ,[BackupStartTime]
            ,[LogFilePath])
    VALUES
        (N'"+ $BackupInstance + "'
        ,N'"+ $DatabaseName + "'
        ,CAST('"+ $myTestResultCode.ToString() + "' AS INT)
        ,N'"+ $myTestResultDescription + "'
        ,@myRecoveryDateTime
        ,@myBackupStartTime
        ,N'"+ $ErrorFileAddress + "'
        )
        "
    Invoke-Sqlcmd -ServerInstance $RestoreInstance -Database $DatabaseReportStore -Query $myInsertCommand -OutputSqlErrors $true -QueryTimeout 0
    #   return $Result
}
# Copy File To Destination
Function CopyFile {
    Param (
        [parameter(Mandatory = $true)][string]$SourcePath,
        [parameter(Mandatory = $true)][string][string]$DestinationPath 
    )
    Copy-Item -Path $SourcePath -Destination $DestinationPath
}
Function DeleatFile {
    Param (
        [parameter(Mandatory = $true)][string]$FileName,
        [parameter(Mandatory = $true)][string][string]$Path 
    )
    Get-childItem -Path $Path | Remove-Item -Include *$FileName*.bak
}
Function DropDatabase {
    Param (
        [parameter(Mandatory = $true)][string]$InstanceName,
        [parameter(Mandatory = $true)][string]$databaseName
    )

    $myQuery = 
    "
    DROP DATABASE IF EXISTS [$databaseName]
    "
    Invoke-Sqlcmd -ServerInstance $InstanceName -Database "master" -Query $myQuery  -OutputSqlErrors $true -OutputAs DataTables

}
Function TruncateTable {
    Param (
        [parameter(Mandatory = $true)][string]$InstanceName,
        [parameter(Mandatory = $true)][string]$SchemaName,
        [parameter(Mandatory = $true)][string]$TableName,
        [parameter(Mandatory = $true)][int]$ExecutionId
    )

    $myQuery = "
    	DECLARE @myExecutionId INT 
	    SET @myExecutionId =CAST('"+ $ExecutionId.ToString() + "' AS INT); 

        IF EXISTS (SELECT 1 FROM Tempdb.INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '"+ $SchemaName + "' AND TABLE_NAME = '" + $TableName + "')
            DELETE FROM [tempdb].["+ $SchemaName + "].[" + $TableName + "] WHERE ExecutionId=@myExecutionId
        "
    Invoke-Sqlcmd -ServerInstance $InstanceName -Database "tempdb" -Query $myQuery  -OutputSqlErrors $true -OutputAs DataTables
}
# Fill Log file
Function Write-Log {
    Param
    (
        [Parameter(Mandatory = $true)][string]$LogFilePath,
        [Parameter(Mandatory = $true)][string]$Content,
        [Parameter(Mandatory = $false)][ValidateSet("INF", "WRN", "ERR")][string]$Type = "INF",
        [Switch]$Terminate = $false
    )
    $myContent = (Get-Date).ToString() + "`t" + $Type + "`t" + $Content
    $myColor = switch ($Type) {
        "INF" { "White" }
        "WRN" { "Yellow" }
        "ERR" { "Red" }
        Default { "White" }
    }
    Write-Host $myContent -ForegroundColor $myColor
    Add-Content -Path $LogFilePath -Value $myContent
    if ($Terminate) { Exit }
}
# Reload File List
Function LoadData {
    param (
        [parameter(Mandatory = $true)][string]$BackupInstance
    )
    $myList = Invoke-Sqlcmd -ServerInstance $myRestoreInstance -Database "tempdb" -Query "SELECT * FROM [tempdb].[dbo].[BackupPathResult]" -OutputSqlErrors $true -OutputAs DataTables
    return $myList
}
#-----Body
if("" -eq $myErrorFile ) {$myErrorFile = "U:\Databases\Temp\BackupTextResult.txt"}
if("" -eq $myRestoreInstance ) {$myRestoreInstance = "DB-BK-DBV02.SAIPACORP.COM\NODE,49149"}	
if("" -eq $myDestinationPath ) {$myDestinationPath = "\\DB-BK-DBV02\U$\Databases\Backup\"}
if("" -eq $myMonitoringServer ) {$myMonitoringServer = "DB-MN-DLV02.SAIPACORP.COM\NODE,49149"}
if("" -eq $myDataFilePath ) {$myDataFilePath = "F:\Data01\Databases\Data"}
if("" -eq $myLogFilePath ) {$myLogFilePath = "F:\Log01\Databases\Log"}
if("" -eq $DatabaseReportStore ) {$myDatabaseReportStore = "SqlDeep"}

#   Install-Module -Name SqlServer
Write-Log -LogFilePath $myErrorFile -Content "Backup Test Process started" -Type INF
$myDatabaseHashList = @{}
$mySplitter = "#"
$myExecutionId = Get-Random -Minimum 0 -Maximum 1000
$myDomainName = (Get-WmiObject Win32_ComputerSystem).Domain
Write-Log -LogFilePath $myErrorFile -Content ("Execution Id is: " + $myExecutionId.ToString()) -Type INF

Write-Log -LogFilePath $myErrorFile -Content "GetServerInfo started" -Type INF
$myServerList = GetServerInfo -InstanceName $myMonitoringServer -TargetServer $myRestoreInstance # Get Server list from MSX
 
Write-Log -LogFilePath $myErrorFile -Content "Generate untested database list started" -Type INF
foreach ($myServer in $myServerList) {
    #Generate untested database list
    Write-Log -LogFilePath $myErrorFile -Content "Check for Target and Source server equal name." -Type INF
    if ($myRestoreInstance -eq $myServer.InstanceName) { break }
    
    Write-Log -LogFilePath $myErrorFile -Content "GetDatabaseInfo started" -Type INF
    $myDatabaseList = GetDatabaseInfo -InstanceName ($myServer.InstanceName)
    foreach ($myDatabase in $myDatabaseList) {
        $myTryCount = 0
        $myContinue = $true
        while ($myContinue) {
            Write-Log -LogFilePath $myErrorFile -Content "GenerateRandomDate started" -Type INF
            $myRecoveryDate = GenerateRandomDate -MinNumber 1 -MaxNumber 4
            $myTryCount += 1
            if ($myTryCount -eq $myMaximumTryCountToFindUncheckedBackup) { $myContinue = $false }
            Write-Log -LogFilePath $myErrorFile -Content ("IsTested process for database [" + $myDatabase.Name + "] on " + $myRecoveryDate.ToString() + " started") -Type INF
            if ((IsTested -InstanceName $myServer.InstanceName -RestoreInstance $myRestoreInstance -RecoveryDateTime $myRecoveryDate -DatabaseName $myDatabase.Name  -DatabaseReportStore $myDatabaseReportStore) -eq $false) {
                $myDatabaseHashListKey = $myServer.InstanceName + $mySplitter + $myDatabase.Name
                $myDatabaseHashList.Add($myDatabaseHashListKey, [PSCustomObject] @{InstanceName = $myServer.InstanceName; DatabaseName = $myDatabase.Name; RecoveryDate = $myRecoveryDate; HasValidBackupFileList = $false })
                $myDatabaseHashList.GetEnumerator() | Sort-Object {Get-Random}
                $myContinue = $false
                Write-Log -LogFilePath $myErrorFile -Content ("Database [" + $myDatabase.Name + "] and Instance [" + $myServer.InstanceName + "] on time " + $myRecoveryDate.ToString() + " does not have any log record.") -Type INF
            }
            else {
                Write-Log -LogFilePath $myErrorFile -Content ("You have a existed test log record for database [" + $myDatabase.Name + "] and Instance [" + $myServer.InstanceName + "] on time " + $myRecoveryDate.ToString()) -Type WRN
            }
        }
    }
}

Write-Log -LogFilePath $myErrorFile -Content "TruncateTable started" -Type INF
TruncateTable -InstanceName $myRestoreInstance -SchemaName "dbo" -TableName "BackupPathResult" -ExecutionId $myExecutionId

Write-Log -LogFilePath $myErrorFile -Content "Generate list of valid backup files started" -Type INF
foreach ($myDatabase in $myDatabaseHashList.GetEnumerator() | Sort-Object {Get-Random}) {
    #Generate list of valid backup files
    Try {    
        Write-Log -LogFilePath $myErrorFile -Content ("GetBackupFiles from instance " + $myDatabase.Value.InstanceName + ", database [" + $myDatabase.Value.DatabaseName + "] and time " + $myDatabase.Value.RecoveryDate.ToString() + " with ExecutionId " + $myExecutionId.ToString() + " started") -Type INF
        $myBackupFileList = GetBackupFiles -InstanceName ($myDatabase.Value.InstanceName) -DatabaseName ($myDatabase.Value.DatabaseName) -BackupDate ($myDatabase.Value.RecoveryDate) -ExecutionId $myExecutionId -LogFilePath $myLogFilePath -DataFilePath $myDataFilePath -DomainName $myDomainName
        If ($myBackupFileList.BackupType -contains "FULL") {
            Write-Log -LogFilePath $myErrorFile -Content ("Database [" + $myDatabase.Value.DatabaseName + "] on instance " + $myDatabase.Value.InstanceName + " has full backup") -Type INF
            Try {   
                $myBackupFileList | Write-SqlTableData -ServerInstance $myRestoreInstance -Database "tempdb" -SchemaName "dbo" -TableName "BackupPathResult" -Force
                $myDatabase.Value.HasValidBackupFileList = $true
                Write-Log -LogFilePath $myErrorFile -Content ("file list for Database [" + $myDatabase.Value.DatabaseName + "] on instance " + $myDatabase.Value.InstanceName + " imported") -Type INF
            }
            catch [Exception] {
                Write-Log -LogFilePath $myErrorFile -Content $($_.Exception.Message) -Type ERR
            }
        }
    }
    catch [Exception] {
        Write-Log -LogFilePath $myErrorFile -Content $($_.Exception.Message) -Type ERR
    }
}

Write-Log -LogFilePath $myErrorFile -Content "Copy, Restore, CheckDB and Remove each database started" -Type INF
$myBackupFileList = LoadData -BackupInstance $myRestoreInstance

foreach ($myDatabase in ($myDatabaseHashList.GetEnumerator() | Where-Object { $_.Value.HasValidBackupFileList -EQ $true })) {
    #Copy, Restore, CheckDB and Remove each database
    Write-Log -LogFilePath $myErrorFile -Content ("Copy backup files of " + $myDatabase.Value.DatabaseName.ToString() + " from source to destination is started.") -Type INF

    foreach ($myBackupFile in ($myBackupFileList | Where-Object { $_.DatabaseName -EQ $myDatabase.Value.DatabaseName -and $_.ExecutionId -EQ $myExecutionId -and $_.InstanceName -EQ $myDatabase.Value.InstanceName })) {
        try { #Copy backup files
            Write-Log -LogFilePath $myErrorFile -Content ("Copy database [" + $myDatabase.Value.DatabaseName + "] backup files to restore on instance " + $myRestoreInstance + "") -Type INF
            if ($null -ne $myBackupFile.UNCPath -and (Test-Path $myBackupFileList.UNCPath) -eq 'true') {
                # CopyFile -SourcePath  $myBackupFile.UNCPath -DestinationPath $myDestinationPath
                Copy-Item -Path $myBackupFile.UNCPath -Destination $myDestinationPath
                Write-Log -LogFilePath $myErrorFile -Content ($myBackupFile.UNCPath) -Type INF 
            }
        }
        catch [Exception]
        { 
            Write-Log -LogFilePath $myErrorFile -Content $($_.Exception.Message) -Type ERR
            SaveResult -BackupInstance $myDatabase.Value.InstanceName -DatabaseName $myDatabase.Value.DatabaseName -TestResult CopyFail -ErrorFileAddress $myErrorFile -RestoreInstance $myRestoreInstance -BackupStartTime $myBackupFile.BackupStartTime -DatabaseReportStore $DatabaseReportStore -RecoveryDate $myDatabase.Value.RecoveryDate
            $myBackupFileList | Where-Object { $_.DatabaseName -EQ $myDatabase.Value.DatabaseName -and $_.ExecutionId -EQ $myExecutionId -and $_.InstanceName -EQ $myDatabase.Value.InstanceName } | Remove-Item -Path { $myDestinationPath + $_.FileName } -Force -ErrorAction Ignore
            continue
        }
    }

    Write-Log -LogFilePath $myErrorFile -Content ("Restore database " + $myDatabase.Value.DatabaseName.ToString() + " full backup is started.") -Type INF
    try { #Restore full backup
        Write-Log -LogFilePath $myErrorFile -Content ("Restore full backup of " + $myDatabase.Value.DatabaseName + " database from " + $myBackupFile.UNCPath + " path on instance " + $myRestoreInstance + " is started.") -Type INF
        $myRestoreCommand = RestoreFullCommand  -DatabaseName ($myDatabase.Value.DatabaseName) -DestinationPath $myDestinationPath -ExecutionId $myExecutionId -RestoreInstance $myRestoreInstance
        while (Test-FileLock -Path ($myDestinationPath + $myBackupFile.FileName) ) { Start-Sleep -Seconds 2 }
        Invoke-Sqlcmd -ServerInstance $myRestoreInstance -Database "master" -Query $myRestoreCommand.myCompleteCommand -ErrorAction Stop
    }
    catch [Exception] {
        Write-Log -LogFilePath $myErrorFile -Content $($_.Exception.Message) -Type ERR
        SaveResult -BackupInstance $myDatabase.Value.InstanceName -DatabaseName $myDatabase.Value.DatabaseName -TestResult RestoreFullBackupFail -ErrorFileAddress $myErrorFile -RestoreInstance $myRestoreInstance -BackupStartTime $myBackupFile.BackupStartTime -DatabaseReportStore $DatabaseReportStore -RecoveryDate $myDatabase.Value.RecoveryDate
        $myBackupFileList | Where-Object { $_.DatabaseName -EQ $myDatabase.Value.DatabaseName -and $_.ExecutionId -EQ $myExecutionId -and $_.InstanceName -EQ $myDatabase.Value.InstanceName } | Remove-Item -Path { $myDestinationPath + $_.FileName } -Force -ErrorAction Ignore
        continue
    }
    Write-Log -LogFilePath $myErrorFile -Content ("Restore database " + $myDatabase.Value.DatabaseName.ToString() + " differential backup is started.") -Type INF
    try { #Restore diff backup
        Write-Log -LogFilePath $myErrorFile -Content ("Restore diff backup of " + $myDatabase.Value.DatabaseName + " database from " + $myBackupFile.UNCPath + " path on instance " + $myRestoreInstance + " is started.") -Type INF
        $myRestoreCommand = RestoreDiffCommand -InstanceName $myRestoreInstance -DatabaseName ($myDatabase.Value.DatabaseName) -DestinationPath $myDestinationPath -ExecutionId $myExecutionId
        if ($null -ne $myRestoreCommand.myCompleteCommand) {
            while (Test-FileLock -Path ($myDestinationPath + $myBackupFile.FileName) ) { Start-Sleep -Seconds 2 }
            Invoke-Sqlcmd -ServerInstance $myRestoreInstance -Database "master" -Query $myRestoreCommand.myCompleteCommand -ErrorAction Stop
        }
    }
    catch [Exception] {
        Write-Log -LogFilePath $myErrorFile -Content $($_.Exception.Message) -Type ERR
        SaveResult -BackupInstance $myDatabase.Value.InstanceName -DatabaseName $myDatabase.Value.DatabaseName -TestResult RestoreDiffBackupFail -ErrorFileAddress $myErrorFile -RestoreInstance $myRestoreInstance -BackupStartTime $myBackupFile.BackupStartTime -DatabaseReportStore $DatabaseReportStore -RecoveryDate $myDatabase.Value.RecoveryDate
        $myBackupFileList | Where-Object { $_.DatabaseName -EQ $myDatabase.Value.DatabaseName -and $_.ExecutionId -EQ $myExecutionId -and $_.InstanceName -EQ $myDatabase.Value.InstanceName } | Remove-Item -Path { $myDestinationPath + $_.FileName } -Force -ErrorAction Ignore
        continue
    }
    Write-Log -LogFilePath $myErrorFile -Content ("Restore log backup(s) of " + $myDatabase.Value.DatabaseName + " database from " + $myBackupFile.UNCPath + " path on instance " + $myRestoreInstance + " is started.") -Type INF
    try { #Restore log backup(s)
        $myRestoreList = RestoreLogCommand -InstanceName $myRestoreInstance -DatabaseName ($myDatabase.Value.DatabaseName) -DestinationPath $myDestinationPath -RecoveryDate ($myDatabase.Value.RecoveryDate) -ExecutionId $myExecutionId
        foreach ($myRestoreCommand in $myRestoreList) {
            #Restore log backup
            Write-Log -LogFilePath $myErrorFile -Content ("Restore command is " + $myRestoreCommand.myCompleteCommand) -Type INF
            while (Test-FileLock -Path ($myDestinationPath + $myBackupFile.FileName)) { Start-Sleep -Seconds 2 }
            Invoke-Sqlcmd -ServerInstance $myRestoreInstance -Database "master" -Query $myRestoreCommand.myCompleteCommand -ErrorAction Stop
        }
    }
    catch [Exception] {
        Write-Log -LogFilePath $myErrorFile -Content $($_.Exception.Message) -Type ERR
        SaveResult -BackupInstance $myDatabase.Value.InstanceName -DatabaseName $myDatabase.Value.DatabaseName -TestResult RestoreLogBackupFail -ErrorFileAddress $myErrorFile -RestoreInstance $myRestoreInstance -BackupStartTime $myBackupFile.BackupStartTime -DatabaseReportStore $DatabaseReportStore -RecoveryDate $myDatabase.Value.RecoveryDate
        $myBackupFileList | Where-Object { $_.DatabaseName -EQ $myDatabase.Value.DatabaseName -and $_.ExecutionId -EQ $myExecutionId -and $_.InstanceName -EQ $myDatabase.Value.InstanceName } | Remove-Item -Path { $myDestinationPath + $_.FileName } -Force -ErrorAction Ignore
        DropDatabase -InstanceName $myRestoreInstance -databaseName $myDatabase.Value.DatabaseName 
        continue
    } 

    Write-Log -LogFilePath $myErrorFile -Content ("CheckDB On " + $myDatabase.Value.DatabaseName + " is started.") -Type INF
    try {# DBCC CHECKDB Test Database
        Write-Log -LogFilePath $myErrorFile -Content ("CheckDB on database [" + $myDatabase.Value.DatabaseName + "] ") -Type INF
        $myDbccTestResult = CheckDB -InstanceName $myRestoreInstance -DatabaseName $myDatabase.Value.DatabaseName
        if ($myDbccTestResult -eq $flase) {
            SaveResult -BackupInstance $myDatabase.Value.InstanceName -DatabaseName $myDatabase.Value.DatabaseName -TestResult CheckDbFail -ErrorFileAddress $myErrorFile -RestoreInstance $myRestoreInstance -BackupStartTime $myBackupFile.BackupStartTime -DatabaseReportStore $DatabaseReportStore -RecoveryDate $myDatabase.Value.RecoveryDate
            continue
        }
    }
    catch [Exception] {
        Write-Log -LogFilePath $myErrorFile -Content $($_.Exception.Message) -Type ERR
        SaveResult -BackupInstance $myDatabase.Value.InstanceName -DatabaseName $myDatabase.Value.DatabaseName TestResult CheckDbFail -ErrorFileAddress $myErrorFile -RestoreInstance $myRestoreInstance -BckupStartTime $myBackupFile.BackupStartTime -RecoveryDate $myDatabase.Value.RecoveryDate
        continue
    } 

    Write-Log -LogFilePath $myErrorFile -Content ("Seve Test Result On " + $myDatabase.Value.DatabaseName + " to  is started.") -Type INF
    try { #Seve Test Result On Tabale
        Write-Log -LogFilePath $myErrorFile -Content ("Seve Test Result On " + $myDatabase.Value.DatabaseName + "to instance.") -Type INF
        SaveResult -BackupInstance $myDatabase.Value.InstanceName -DatabaseName $myDatabase.Value.DatabaseName -TestResult Succseed -ErrorFileAddress $myErrorFile -RestoreInstance $myRestoreInstance -BackupStartTime $myBackupFile.BackupStartTime -DatabaseReportStore $DatabaseReportStore -RecoveryDate $myDatabase.Value.RecoveryDate
    }

    catch [Exception] {
        Write-Log -LogFilePath $myErrorFile -Content $($_.Exception.Message) -Type ERR
        continue
    }

    Write-Log -LogFilePath $myErrorFile -Content ("Drop database [" + $myDatabase.Value.DatabaseName + "] is Started") -Type INF
    try {
        # Drop Database
        Write-Log -LogFilePath $myErrorFile -Content ("Drop database [" + $myDatabase.Value.DatabaseName + "] ") -Type INF
        DropDatabase -InstanceName $myRestoreInstance -databaseName $myDatabase.Value.DatabaseName 
    }
    catch [Exception] {
        Write-Log -LogFilePath $myErrorFile -Content $($_.Exception.Message) -Type ERR
        continue
    } 

    Write-Log -LogFilePath $myErrorFile -Content ("Deleate backup files of database [" + $myDatabase.Value.DatabaseName + "] is Started") -Type INF
    try {
        # Delete File
        Write-Log -LogFilePath $myErrorFile -Content ("Deleate backup files of database [" + $myDatabase.Value.DatabaseName + "] ") -Type INF
        $myBackupFileList | Where-Object { $_.DatabaseName -EQ $myDatabase.Value.DatabaseName -and $_.ExecutionId -EQ $myExecutionId -and $_.InstanceName -EQ $myDatabase.Value.InstanceName } | Remove-Item -Path { $myDestinationPath + $_.FileName } -Force -ErrorAction Ignore
    }
    catch [Exception] {
        Write-Log -LogFilePath $myErrorFile -Content $($_.Exception.Message) -Type ERR
    } 

}
    
Write-Log -LogFilePath $myErrorFile -Content "Backup Test Process Finished" -Type INF