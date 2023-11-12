Param(
     [Parameter(Mandatory=$true)][string]$BackupRepositoryInstanceConnectionString,
     [Parameter(Mandatory=$true)][int]$ScanFromDaysBeforeNow,
     [Parameter(Mandatory=$true)][int]$ScanToHoursBeforeNow,
     [Parameter(Mandatory=$false)][string]$TransferedSuffix="_Transfered",
     [Parameter(Mandatory=$true)][string]$TargetInstanceConnectionStringSuffix = ",49149",
     [Parameter(Mandatory=$false)][string]$LogInstanceConnectionString,
     [Parameter(Mandatory=$false)][string]$LogTableName="[dbo].[Events]",
     [Parameter(Mandatory=$true)][string]$LogFilePath="C:\Log\TransferBackupStatusCorrection_{Date}.txt"
     )

Function Get-FunctionName ([int]$StackNumber = 1) { #Create Log Table if not exists
    return [string](Get-PSCallStack)[$StackNumber].FunctionName
}
Function UpdateBackupRepository {
    Param
            (
            [Parameter(Mandatory=$true)][int]$Id
            )
    $myQuery="
        DECLARE @myId INT
        SET @myId = "+ $Id.ToString() +"
        UPDATE [dbo].[TransferredFiles] SET [TransferStatus] = 'SUCCEED' WHERE Id = @myId
    "
    return $myQuery
}
Function GetNoneListQuery {
    Param
            (
            [Parameter(Mandatory=$true)][int]$ScanFromDaysBeforeNow,
            [Parameter(Mandatory=$true)][int]$ScanToHoursBeforeNow
            )
    $myQuery="
        DECLARE @myStartDate DATETIME
        DECLARE @myFinishDate DATETIME
        SET @myStartDate = DATEADD(DAY,-"+$ScanFromDaysBeforeNow.ToString()+",CAST(CAST(GETDATE() AS DATE) AS DATETIME))
        SET @myFinishDate = DATEADD(Hour,-"+$ScanToHoursBeforeNow.ToString()+",GETDATE())
        SELECT 
            Id,InstanceName,DatabaseName,media_set_id
        FROM 
            [dbo].[TransferredFiles] AS myBackupLog WITH (READPAST) 
        WHERE 
            [myBackupLog].[TransferStatus] != 'SUCCEED' AND 
            [myBackupLog].[DeleteDate] IS NOT NULL AND 
            [myBackupLog].[EventTimeStamp] BETWEEN @myStartDate AND @myFinishDate
    "
    return $myQuery
}
Function GetMediasetQuery {
    Param
            (
            [Parameter(Mandatory=$true)][string]$DatabaseName,
            [Parameter(Mandatory=$true)][int]$MediasetId,
            [Parameter(Mandatory=$true)][string]$TransferedSuffix
            )
    $myQuery="
        DECLARE @myMediasetId INT
        DECLARE @myDatabaseName sysname
        DECLARE @myTransferedSuffix nvarchar(20)

        SET @myMediasetId = "+$MediasetId+"
        SET @myDatabaseName = N'"+$DatabaseName+"'
        SET @myTransferedSuffix = N'"+$TransferedSuffix+"'
        SELECT 
	        [myBackups].[machine_name],
	        [myBackups].[server_name],
	        [myBackups].[database_name],
	        [myBackups].[type],
	        [myBackups].[name],
	        [myBackups].[media_set_id] 
        FROM 
	        [msdb].[dbo].[backupset] AS myBackups
        WHERE 
	        [myBackups].[media_set_id] = @myMediasetId
            AND [myBackups].[database_name] = @myDatabaseName
	        AND [myBackups].[description] LIKE N'%' + @myTransferedSuffix
    "
    return $myQuery
}
Function Write-Log {    #Fill Log file
    Param
        (
        [Parameter(Mandatory=$false)][string]$LogFilePath = $LogFilePath,
        [Parameter(Mandatory=$true)][string]$Content,
        [Parameter(Mandatory=$false)][ValidateSet("INF","WRN","ERR")][string]$Type="INF",
        [Switch]$Terminate=$false,
        [Switch]$LogToTable=$mySysEventsLogToTableFeature,  #$false
        [Parameter(Mandatory=$false)][string]$LogInstanceConnectionString = $LogInstanceConnectionString,
        [Parameter(Mandatory=$false)][string]$LogTableName = $LogTableName,
        [Parameter(Mandatory=$false)][string]$EventSource
        )
    
    Switch ($Type) {
        "INF" {$myColor="White";$myIsSMS="0"}
        "WRN" {$myColor="Yellow";$myIsSMS="1";$mySysWrnCount+=1}
        "ERR" {$myColor="Red";$myIsSMS="1";$mySysErrCount+=1}
        Default {$myColor="White"}
    }
    $myEventTimeStamp=(Get-Date).ToString()
    $myContent = $myEventTimeStamp + "`t" + $Type + "`t(" + (Get-FunctionName -StackNumber 2) +")`t"+ $Content
    if ($Terminate) { $myContent+=$myContent + "`t" + ". Prcess terminated with " + $mySysErrCount.ToString() + " Error count and " + $mySysWrnCount.ToString() + " Warning count."}
    Write-Host $myContent -ForegroundColor $myColor
    Add-Content -Path $LogFilePath -Value $myContent
    if ($LogToTable) {
        $myCommand=
            "
            INSERT INTO "+ $LogTableName +" ([EventSource],[Module],[EventTimeStamp],[Serverity],[Description],[IsSMS])
            VALUES(N'"+$EventSource+"',N'TransferBackupStatusCorrection',CAST('"+$myEventTimeStamp+"' AS DATETIME),N'"+$Type+"',N'"+$Content+"',"+$myIsSMS+")
            "
            Invoke-Sqlcmd -ConnectionString $LogInstanceConnectionString -Query $myCommand -OutputSqlErrors $true -QueryTimeout 0 -ErrorAction Ignore
        }
    if ($Terminate){Exit}
}
#==================================================Main
#[string]$BackupRepositoryInstanceConnectionString="Data Source=SRV1\NODE,49149;Initial Catalog=EventLog;Integrated Security=True;"
#[int]$ScanFromDaysBeforeNow=2
#[int]$ScanToHoursBeforeNow=2
#[string]$TransferedSuffix="_Transfered"
#[string]$TargetInstanceConnectionStringSuffix = ",49149"
#[string]$LogInstanceConnectionString="Data Source=SRV1\NODE,49149;Initial Catalog=EventLog;Integrated Security=True;"
#[string]$LogTableName="[dbo].[Events]"
#[string]$LogFilePath="U:\Install\TransferBackupsCorrectStatus.log"
[string]$myCurrentHostName=([Environment]::MachineName).ToUpper()
[bool]$mySysEventsLogToTableFeature=$true

Write-Log -Type INF -Content "Backup status correction process started." -EventSource $myCurrentHostName
Write-Log -Type INF -Content "Get list of NONE status repository backups." -EventSource $myCurrentHostName

Try{
    $myCommand=GetNoneListQuery -ScanFromDaysBeforeNow $ScanFromDaysBeforeNow -ScanToHoursBeforeNow $ScanToHoursBeforeNow
    Write-Log -Type INF -Content $myCommand -EventSource $myCurrentHostName
    $myNoneRecords = Invoke-Sqlcmd -ConnectionString $BackupRepositoryInstanceConnectionString -Query $myCommand -OutputSqlErrors $true -QueryTimeout 0 -OutputAs DataRows -ErrorAction Stop
}Catch{
    Write-Log -Type ERR -Content ($_.ToString()).ToString() -EventSource $myCurrentHostName
    Write-Log -Type ERR -Content $myCommand -EventSource $myCurrentHostName -Terminate
}

Write-Log -Type INF -Content "Check for actually transferred backup on source instance." -EventSource "DB-MN-DLV01"
if ($null -ne $myNoneRecords) {
    ForEach ($myRecord in $myNoneRecords) {
        Write-Log -Type INF -Content ("GetMediasetQuery for DatabaseName: " + $myRecord.DatabaseName + ", MediasetId: " + $myRecord.media_set_id.ToString() + ", Target is: " + $myRecord.InstanceName + $TargetInstanceConnectionStringSuffix) -EventSource "DB-MN-DLV01"
        Try{
            $myCommand = GetMediasetQuery -DatabaseName $myRecord.DatabaseName -MediasetId $myRecord.media_set_id -TransferedSuffix $TransferedSuffix
            Write-Log -Type INF -Content $myCommand -EventSource ($myRecord.InstanceName)
            $myMsdbRecord = Invoke-Sqlcmd -ServerInstance ($myRecord.InstanceName + $TargetInstanceConnectionStringSuffix) -Query $myCommand -OutputSqlErrors $true -QueryTimeout 0 -OutputAs DataRows -ErrorAction Stop
            if ($null -ne $myMsdbRecord) #Backup file is transferred
            {
                Try{
                    $myCommand = UpdateBackupRepository -Id ($myRecord.Id)
                    Write-Log -Type INF -Content $myCommand -EventSource $myCurrentHostName
                    Invoke-Sqlcmd -ConnectionString $BackupRepositoryInstanceConnectionString -Query $myCommand -OutputSqlErrors $true -QueryTimeout 0 -ErrorAction Stop
                    Write-Log -Type INF -Content $myCommand -EventSource ($myRecord.InstanceName)
                }Catch{
                    Write-Log -Type ERR -Content ($_.ToString()).ToString() -EventSource ($myRecord.InstanceName)
                    Write-Log -Type ERR -Content $myCommand -EventSource ($myRecord.InstanceName)
                }
            }else{
                Write-Log -Type INF -Content "This file does not transferred yet." -EventSource ($myRecord.InstanceName)
            }
        }Catch{
            Write-Log -Type ERR -Content ($_.ToString()).ToString() -EventSource ($myRecord.InstanceName)
            Write-Log -Type ERR -Content $myCommand -EventSource ($myRecord.InstanceName)
        }
    }
}else{
    Write-Log -Type INF -Content "All repository records are correct." -EventSource "DB-MN-DLV01"
}

Write-Log -Type INF -Content "Backup status correction process finished." -EventSource "DB-MN-DLV01"