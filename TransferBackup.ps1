#---------------------------------------------------------Parameters
 Param(
     [Parameter(Mandatory=$true)][string]$SourceInstanceConnectionString,
     [Parameter(Mandatory=$false)][string][ValidateSet("All_Databases","User_Databases","System_Databases", IgnoreCase=$true)]$DatabasesToTransfer="All_Databases",
     [Parameter(Mandatory=$false)][string]$ExceptedDatabasesForTransfer,
     [Parameter(Mandatory=$false)][string][ValidateSet("ALL","FULL","DIFF","LOG", IgnoreCase=$true)]$BackupTypeToTransfer="ALL",
     [Parameter(Mandatory=$false)][int]$HoursToScanForUntransferredBackups=72,
     [Parameter(Mandatory=$true)][string][ValidateSet("UNC","FTP","SFTP","SCP","LOCAL", IgnoreCase=$true)]$DestinationType,
     [Parameter(Mandatory=$true)][string]$Destination,
     [Parameter(Mandatory=$false)][string]$WinscpPath="C:\Program Files (x86)\WinSCP\WinSCPnet.dll",
     [Parameter(Mandatory=$false)][string]$DestinationFolderStructure="/{Year}/{Month}{Day}/{InstanceName}/{DatabaseName}/",
     [Parameter(Mandatory=$false)][string]$SshHostKeyFingerprint,
     [Parameter(Mandatory=$false)][System.Net.NetworkCredential]$DestinationCredential,
     [Parameter(Mandatory=$false)][string]$DestinationCredentialName,
     [Parameter(Mandatory=$true)][string][ValidateSet("COPY","MOVE", IgnoreCase=$true)]$ActionType="COPY",
     [Parameter(Mandatory=$false)][string]$RetainDaysOnDestination,
     [Parameter(Mandatory=$false)][string]$TransferedSuffix="_Transfered",
     [Parameter(Mandatory=$false)][string]$LogInstanceConnectionString,
     [Parameter(Mandatory=$false)][string]$LogTableName="[dbo].[Events]",
     [Parameter(Mandatory=$true)][string]$LogFilePath
 )
##$SourceInstanceConnectionString = "Data Source=DB-SH-DLV01.SAIPACORP.COM\SHAREPOINT,49149;Initial Catalog=msdb;Integrated Security=True;" #"Data Source=DB-C1-DLV01.sqldeep.local\Node,49149;Initial Catalog=msdb;user=sa;password=Armin1355$;encrypt=true;trustservercertificate=true"
##$DatabasesToTransfer = "All_Databases"
##$ExceptedDatabasesForTransfer = ""
##$BackupTypeToTransfer = "ALL"
##$HoursToScanForUntransferredBackups = 1
###$myCred=New-Object System.Net.NetworkCredential("oracle", "orbk_DB_bkl002")
###$myCred=New-Object System.Net.NetworkCredential("Saipacorp\299294", "Armin1355`$Nasim")
####$myCred=(Get-StoredCredential -Target "SqlDeepBackupCredentialUNC").GetNetworkCredential()
##$myCred=(Get-StoredCredential -Target "SqlDeepBackupCredentialSCP").GetNetworkCredential()
##$DestinationType = "SCP"    #"FTP","SFTP","UNC"
##$Destination = "172.20.5.2"   #"\\DB-C1-DLV18\U$\Install"
##$WinscpPath = "U:\Install\PS\WinSCP\WinSCPnet.dll"
##$DestinationFolderStructure = "/bk_sql/test/{CustomRule01}/{CustomRule02}/{InstanceName}" #"/bk_sql/{InstanceName}/{Year}_{Month}/{Day}/",  "{InstanceName}\{Year}_{Month}\{Day}\"
##$SshHostKeyFingerprint="ssh-ed25519 256 xEkJwBAimRr3rfS3Hm+dnKc5lSTABvDUntt+itokHPw="
##$DestinationCredential = $myCred
##$ActionType = "COPY" #"MOVE"
##$RetainDaysOnSource = 0
##$RetainDaysOnDestination = 0
##$TransferedSuffix = "_Siavash04"
##$LogInstanceConnectionString = "Data Source=DB-MN-DLV02.SAIPACORP.COM\NODE,49149;Initial Catalog=EventLog;Integrated Security=True;"
##$LogTableName="[dbo].[Events]"
##$LogFilePath = "U:\Install\PS\log.txt"
#---------------------------------------------------------How to Define Credential in windows credential manager
#   --Install PS Credential Module
#   Install-Module CredentialManager -force
#   Import-Module CredentialManager
#   --Create Credential
#   New-StoredCredential -Target "SqlDeepFTP" -Type Generic -UserName 'oracle' -Password 'P@$$Word' -Persist LocalMachine
#---------------------------------------------------------FUNCTIONS
Function Get-FunctionName ([int]$StackNumber = 1) { #Create Log Table if not exists
    return [string]$(Get-PSCallStack)[$StackNumber].FunctionName
}
Function EventsTable.Create {   #Create Events Table to Write Logs to a database table if not exists
    Param
        (
        [Parameter(Mandatory=$true)][string]$LogInstanceConnectionString,
        [Parameter(Mandatory=$true)][string]$TableName
        )

        $myAnswer=[bool]$true
        $myCommand="
        DECLARE @myTableName nvarchar(255)
        SET @myTableName=N'"+ $TableName +"'
        
        IF NOT EXISTS (
            SELECT 
                1
            FROM 
                sys.all_objects AS myTable
                INNER JOIN sys.schemas AS mySchema ON myTable.schema_id=mySchema.schema_id
            WHERE 
                mySchema.name + '.' + myTable.name = REPLACE(REPLACE(@myTableName,'[',''),']','')
        ) BEGIN
            CREATE TABLE" + $TableName + "(
                [Id] [bigint] IDENTITY(1,1) NOT NULL,
                [EventSource] [nvarchar](255) NOT NULL,
                [Module] [nvarchar](255) NOT NULL,
                [EventTimeStamp] [datetime] NOT NULL,
                [Serverity] [nvarchar](50) NULL,
                [Description] [nvarchar](max) NULL,
                [InsertTime] [datetime] NOT NULL DEFAULT (getdate()),
                [IsSMS] [bit] NOT NULL DEFAULT (0),
                [IsSent] [bit] NOT NULL DEFAULT (0),
                PRIMARY KEY CLUSTERED ([Id] ASC) WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 85, Data_Compression=Page) ON [PRIMARY]
            ) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
        END
    "
    try{
        Invoke-Sqlcmd -ConnectionString $LogInstanceConnectionString -Query $myCommand -OutputSqlErrors $true -QueryTimeout 0 -ErrorAction Stop
    }Catch{
        Write-Log -Type ERR -Content ($_.ToString()).ToString()
        $myAnswer=[bool]$false
    }
    return $myAnswer
}
Function TransferredFilesTable.Create {  #Create TransferredFiles Table to Write transferred backup files log to a database table if not exists
    Param
        (
        [Parameter(Mandatory=$true)][string]$LogInstanceConnectionString,
        [Parameter(Mandatory=$true)][string]$TableName
        )

        $myAnswer=[bool]$true
        $myCommand="
        DECLARE @myTableName nvarchar(255)
        SET @myTableName=N'"+ $TableName +"'
        
        IF NOT EXISTS (
            SELECT 
                1
            FROM 
                sys.all_objects AS myTable
                INNER JOIN sys.schemas AS mySchema ON myTable.schema_id=mySchema.schema_id
            WHERE 
                mySchema.name + '.' + myTable.name = REPLACE(REPLACE(@myTableName,'[',''),']','')
        ) BEGIN
            CREATE TABLE " + $TableName + "(
                [Id] bigint identity,
                [BatchId] uniqueidentifier NOT NULL,
                [EventTimeStamp] [datetime] DEFAULT(getdate()) NOT NULL,
                [Destination] [nvarchar](128) NOT NULL,
                [DestinationFolder] [nvarchar](4000) NOT NULL,
                [UncBackupFilePath] [nvarchar](4000) NOT NULL,
                [media_set_id] [int] NOT NULL,
                [family_sequence_number] [int] NOT NULL,
                [MachineName] [nvarchar](255) NULL,
                [InstanceName] [nvarchar](255) NOT NULL,
                [DatabaseName] [nvarchar](255) NOT NULL,
                [backup_start_date] [datetime] NOT NULL,
                [backup_finish_date] [datetime] NOT NULL,
                [expiration_date] [datetime] NULL,
                [BackupType] [nvarchar](255) NOT NULL,
                [BackupFirstLSN] [decimal](28) NULL,
                [BackupLastLSN] [decimal](28) NULL,
                [BackupFilePath] [nvarchar](4000) NOT NULL,
                [BackupFileName] [nvarchar](4000) NOT NULL,
                [max_family_sequence_number] [int] NOT NULL,
                [DeleteDate] [datetime] NULL,
                [IsDeleted] [bit] NOT NULL DEFAULT(0),
                PRIMARY KEY CLUSTERED ([Id] ASC) WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 85, Data_Compression=Page) ON [PRIMARY]
            ) ON [PRIMARY]
        END
    "
    try{
        Invoke-Sqlcmd -ConnectionString $LogInstanceConnectionString -Query $myCommand -OutputSqlErrors $true -QueryTimeout 0 -ErrorAction Stop
    }Catch{
        Write-Log -Type ERR -Content ($_.ToString()).ToString()
        Write-Log -Type ERR -Content $myCommand
        $myAnswer=[bool]$false
    }
    return $myAnswer
}
Function TransferredFilesTable.Insert {  #Create TransferredFiles Table to Write transferred backup files log to a database table if not exists
    Param
        (
        [Parameter(Mandatory=$true)][string]$LogInstanceConnectionString,
        [Parameter(Mandatory=$true)][string]$TableName,
        [Parameter(Mandatory=$true)][string]$BatchId,
        [Parameter(Mandatory=$true)][string]$Destination,
        [Parameter(Mandatory=$true)][System.Data.DataRow]$Record
        )

        $myAnswer=[bool]$true
        $myCommand="
        INSERT INTO " + $TableName + "
        (
         [BatchId]
        ,[Destination]
        ,[DestinationFolder]
        ,[UncBackupFilePath]
        ,[media_set_id]
        ,[family_sequence_number]
        ,[MachineName]
        ,[InstanceName]
        ,[DatabaseName]
        ,[backup_start_date]
        ,[backup_finish_date]
        ,[expiration_date]
        ,[BackupType]
        ,[BackupFirstLSN]
        ,[BackupLastLSN]
        ,[BackupFilePath]
        ,[BackupFileName]
        ,[max_family_sequence_number]
        ,[DeleteDate]
        ,[IsDeleted])
  VALUES
        (
         '" + $BatchId + "'
        ,'" + $Destination + "'
        ,'" + $Record.DestinationFolder + "'
        ,'" + $Record.UncBackupFilePath + "'
        ," + $Record.media_set_id.ToString() + "
        ," + $Record.family_sequence_number.ToString() + "
        ,'" + $Record.MachineName + "'
        ,'" + $Record.InstanceName + "'
        ,'" + $Record.DatabaseName + "'
        ,CAST('" + $Record.backup_start_date.ToString() + "' AS DATETIME)
        ,CAST('" + $Record.backup_finish_date.ToString() + "' AS DATETIME)
        ,CAST('" + $Record.expiration_date.ToString() + "' AS DATETIME)
        ,'" + $Record.BackupType + "'
        ," + $Record.BackupFirstLSN.ToString() + "
        ," + $Record.BackupLastLSN.ToString() + "
        ,'" + $Record.BackupFilePath + "'
        ,'" + $Record.BackupFileName + "'
        ," + $Record.max_family_sequence_number.ToString() + "
        ,NULL
        ,0
        )
    "
    try{
        Invoke-Sqlcmd -ConnectionString $LogInstanceConnectionString -Query $myCommand -OutputSqlErrors $true -QueryTimeout 0 -ErrorAction Stop
    }Catch{
        Write-Log -Type ERR -Content ($_.ToString()).ToString()
        Write-Log -Type ERR -Content $myCommand
        $myAnswer=[bool]$false
    }
    return $myAnswer
}
Function TransferredFilesTable.SetDeleteDateOfFiles {  #Set DeleteDate for TransferredFiles
    Param
        (
        [Parameter(Mandatory=$true)][string]$LogInstanceConnectionString,
        [Parameter(Mandatory=$true)][string]$TableName,
        [Parameter(Mandatory=$true)][string]$RetainDaysOnDestination,
        [Parameter(Mandatory=$true)][string]$InstanceNameToFilter
        )

        $myCommandExtension01=""
        if ($RetainDaysOnDestination -eq "CustomRule01") {
                $myCommandExtension01="CASE BackupType WHEN 'L' THEN 2 WHEN 'D' THEN 1 WHEN 'I' THEN 1 ELSE 1 END"
            } elseif (IsNumeric($RetainDaysOnDestination) -eq $true) {
                $myCommandExtension01=$RetainDaysOnDestination
            }


        $myCommand="
        DECLARE @myToday Datetime
        DECLARE @myInstanceName nvarchar(256)
        DECLARE @myRetainDaysOnDestination INT
        SET @myInstanceName=N'"+$InstanceNameToFilter+"'
        SET @myToday=getdate()
        
        UPDATE "+$TableName+" SET 
            DeleteDate = DATEADD(Day,"+$myCommandExtension01+",@myToday)
        WHERE
            DeleteDate IS NULL
            AND IsDeleted = 0
            AND InstanceName = @myInstanceName
    "
    try{
        $myRecord=Invoke-Sqlcmd -ConnectionString $LogInstanceConnectionString -Query $myCommand -OutputSqlErrors $true -QueryTimeout 0 -OutputAs DataRows -ErrorAction Stop
    }Catch{
        Write-Log -Type ERR -Content ($_.ToString()).ToString()
        Write-Log -Type ERR -Content $myCommand
    }
    if ($null -ne $myRecord) {return $myRecord}
}
Function TransferredFilesTable.GetDepricatedFiles {  #Get Depricated TransferredFiles Table to Write transferred backup files log to a database table if not exists
    Param
        (
        [Parameter(Mandatory=$true)][string]$LogInstanceConnectionString,
        [Parameter(Mandatory=$true)][string]$TableName,
        [Parameter(Mandatory=$true)][string]$InstanceNameToFilter
        )

        $myCommand="
        DECLARE @myToday Datetime
        DECLARE @myInstanceName nvarchar(256)
        SET @myInstanceName=N'"+$InstanceNameToFilter+"'
        SET @myToday=getdate()
        
        SELECT
            myLog.Id,
            myLog.DestinationFolder,
            myLog.UncBackupFilePath,
            myLog.MachineName,
            myLog.InstanceName,
            myLog.backup_finish_date,
            myLog.BackupType
        FROM
            "+$TableName+" AS myLog
        WHERE
            myLog.DeleteDate <= @myToday
            AND myLog.IsDeleted = 0
            AND myLog.InstanceName = @myInstanceName
    "
    try{
        $myRecord=Invoke-Sqlcmd -ConnectionString $LogInstanceConnectionString -Query $myCommand -OutputSqlErrors $true -QueryTimeout 0 -OutputAs DataRows -ErrorAction Stop
    }Catch{
        Write-Log -Type ERR -Content ($_.ToString()).ToString()
        Write-Log -Type ERR -Content $myCommand
    }
    if ($null -ne $myRecord) {return $myRecord}
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
        [Parameter(Mandatory=$false)][string]$EventSource = $mySysSourceInstanceName
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
            VALUES(N'"+$EventSource+"',N'TransferBackups',CAST('"+$myEventTimeStamp+"' AS DATETIME),N'"+$Type+"',N'"+$Content+"',"+$myIsSMS+")
            "
            Invoke-Sqlcmd -ConnectionString $LogInstanceConnectionString -Query $myCommand -OutputSqlErrors $true -QueryTimeout 0 -ErrorAction Ignore
        }
    if ($Terminate){Exit}
}
Function IsNumeric ($Value) {  #Check if input value is numeric
    return $Value -match "^[\d\.]+$"
}
Function UNC.IsAlive {  #Check UNC path is alive
    Param
        (
        [Parameter(Mandatory=$true)][string]$UncSharedFolderPath,
        [Parameter(Mandatory=$true)][System.Net.NetworkCredential]$UncCredential,   #New-Object System.Net.NetworkCredential($Env:UncUsername, $Env:UncPassword)
        [Parameter(Mandatory=$false)][char]$TemporalDriveLetter="A"
        )

    [bool]$myResult=$false
    Write-Log -Type INF -Content "Processing Started."
    $myFileName=([Environment]::MachineName + (New-Guid) + ".lck")
    try {
        $myUser = $UncCredential.UserName
        if (!($UncCredential.Domain -eq "")){$myUser=$UncCredential.Domain+"\"+$myUser}
        $myPassword = ConvertTo-SecureString $UncCredential.Password -AsPlainText -Force
        $myCredential = New-Object System.Management.Automation.PSCredential ($myUser, $myPassword)
        New-PSDrive -Name ($TemporalDriveLetter[0]) -PSProvider filesystem -Root $UncSharedFolderPath -Credential $myCredential
        New-Item -ItemType File -Path ($TemporalDriveLetter[0]+":\") -Name $myFileName
        $myResult=Test-Path -PathType Leaf -Path ($TemporalDriveLetter[0]+":\"+$myFileName)
        Remove-Item -Path ($TemporalDriveLetter[0]+":\"+$myFileName)
        Remove-PSDrive -Name ($TemporalDriveLetter[0])
    }
    catch {
        Write-Log -Type ERR -Content ($_.ToString()).ToString()
    }

    return $myResult
}
Function UNC.MKDIR {  #Create Directory on UNC path
    Param
        (
        [Parameter(Mandatory=$true)][string]$UncSharedFolderPath,
        [Parameter(Mandatory=$true)][System.Net.NetworkCredential]$UncCredential,   #New-Object System.Net.NetworkCredential($Env:UncUsername, $Env:UncPassword)
        [Parameter(Mandatory=$false)][char]$TemporalDriveLetter="A",
        [Parameter(Mandatory=$false)][string]$UncDestinationPath
        )

    [bool]$myResult=$false
    Write-Log -Type INF -Content "Processing Started."
    
    try {
        $myUser = $UncCredential.UserName
        if (!($UncCredential.Domain -eq "")){$myUser=$UncCredential.Domain+"\"+$myUser}
        $myPassword = ConvertTo-SecureString $UncCredential.Password -AsPlainText -Force
        $myCredential = New-Object System.Management.Automation.PSCredential ($myUser, $myPassword)
        New-PSDrive -Name ($TemporalDriveLetter[0]) -PSProvider filesystem -Root $UncSharedFolderPath -Credential $myCredential

        # Create the directory and throw on any error
        $myPath=$TemporalDriveLetter[0]+":"
        [array]$myFolders = $UncDestinationPath.Split("\")
        foreach ($myFolder in $myFolders)
        {
            if ($myFolder.ToString().Trim().Length -gt 0) 
            {
                $myPath += "\" + $myFolder
                if ((Test-Path -PathType Container -Path $myPath) -eq $false) {
                    New-Item -ItemType Directory -Path $myPath
                    Write-Log -Type INF -Content ("Create new directory on " + $myPath)
                }
            }
        }
        Remove-PSDrive -Name ($TemporalDriveLetter[0])
        $myResult = Test-Path -PathType Container -Path $UncDestinationPath
    }
    catch {
        Write-Log -Type ERR -Content ($_.ToString()).ToString()
    }

    return $myResult
}
Function UNC.UPLOAD {  #Copy file from source to UNC path
    Param
        (
        [Parameter(Mandatory=$true)][string]$UncSharedFolderPath,
        [Parameter(Mandatory=$true)][System.Net.NetworkCredential]$UncCredential,   #New-Object System.Net.NetworkCredential($Env:UncUsername, $Env:UncPassword)
        [Parameter(Mandatory=$false)][char]$TemporalDriveLetter="A",
        [Parameter(Mandatory=$false)][string]$UncDestinationPath,
        [Parameter(Mandatory=$false)][string]$SourceFilePath,
        [Parameter(Mandatory=$false)][string][ValidateSet("COPY","MOVE", IgnoreCase=$true)]$ActionType="COPY"
        )

    [bool]$myResult=$false
    Write-Log -Type INF -Content "Processing Started."
    
    try {
        $UncDestinationPath = $UncDestinationPath.Replace("\\","\")
        $myUser = $UncCredential.UserName
        if (!($UncCredential.Domain -eq "")){$myUser=$UncCredential.Domain+"\"+$myUser}
        $myPassword = ConvertTo-SecureString $UncCredential.Password -AsPlainText -Force
        $myCredential = New-Object System.Management.Automation.PSCredential ($myUser, $myPassword)
        New-PSDrive -Name ($TemporalDriveLetter[0]) -PSProvider filesystem -Root $UncSharedFolderPath -Credential $myCredential

        # Copy\Move file to destination UNC directory and throw on any error
        $myUncDestinationPath=$TemporalDriveLetter[0] + ":\" + $UncDestinationPath
        switch ($ActionType) {
            "COPY" {Copy-Item -Path $SourceFilePath -Destination $myUncDestinationPath -Force}
            "MOVE" {Move-Item -Path $SourceFilePath -Destination $myUncDestinationPath -Force}
        }
        Write-Log -Type INF -Content ("New file uploaded (" + $ActionType + ") to " + $UncDestinationPath)

        Remove-PSDrive -Name ($TemporalDriveLetter[0])
        $myResult = Test-Path -PathType Leaf -Path $myUncDestinationPath
    }
    catch {
        Write-Log -Type ERR -Content ($_.ToString()).ToString()
    }

    return $myResult
}
Function FtpByWinscp {  #Upload file to FTP path by winscp
    Param
        (
        [Parameter(Mandatory=$true)][string][ValidateSet("UPLOAD","DOWNLOAD","DELETE","MKDIR","DIR","ISALIVE", IgnoreCase=$true)]$Operation,
        [Parameter(Mandatory=$true)][string]$FtpServer,
        [Parameter(Mandatory=$true)][System.Net.NetworkCredential]$FtpCredential,   #New-Object System.Net.NetworkCredential($Env:FtpUsername, $Env:FtpPassword)
        [Parameter(Mandatory=$false)][string]$FtpDestinationPath,
        [Parameter(Mandatory=$false)][string]$SourceFilePath,
        [Parameter(Mandatory=$false)][string]$WinscpPath="C:\Program Files (x86)\WinSCP\WinSCPnet.dll"
        )

    [bool]$myResult=$false
    Write-Log -Type INF -Content "Processing Started."
    # https://winscp.net/eng/docs/library_powershell#example
    # Testing variables
    <#
    $hostname = "test.rebex.net"
    $localPath = "C:\Winscp\"
    $remotePath = "/"
    $filename = "*.txt"
    $protocol = "sftp"
    $user = "demo"
    $password = 'password'
    $option = "list"
    $ssh = ""
    #>
    
    $FtpDestinationPath = $FtpDestinationPath.Replace("//","/")
    try
    {
        # Load WinSCP .NET assembly
        Add-Type -Path $WinscpPath
    }catch{
        Write-Log -Type ERR -Content ($_.ToString()).ToString()
    }

    # Setup session options
    if ($FtpCredential.Domain -eq ""){
        $myGeneratedUser=$FtpCredential.UserName
    }else{
        $myGeneratedUser=$FtpCredential.Domain+"\"+$FtpCredential.UserName
    }
    $mySessionOptions = New-Object WinSCP.SessionOptions -Property @{
        FtpMode = "Passive"
        FtpSecure = "None"
        Protocol = "ftp"
        HostName = $FtpServer
        UserName = $myGeneratedUser
        Password = $FtpCredential.Password
    }

    $mySession = New-Object WinSCP.Session
    if($Operation -eq "ISALIVE")
    {
        try
        {
            # Connect
            $mySession.Open($mySessionOptions)        
            [bool]$myResult=$true
        }catch{
            Write-Log -Type ERR -Content ($_.ToString()).ToString()
        }
        finally
        {
            # Disconnect, clean up
            $mySession.Dispose()
        }
    }
    elseif($Operation -eq "UPLOAD")
    {
        try
        {
            # Connect
            $mySession.Open($mySessionOptions)

            # Upload files
            $myTransferOptions = New-Object WinSCP.TransferOptions
            $myTransferOptions.TransferMode = "Binary"

            $myTransferResult = $mySession.PutFiles(($SourceFilePath),$FtpDestinationPath, $False, $myTransferOptions)
        
            # Throw on any error
            $myTransferResult.Check()
            $mySession.Output
        
            # Print results
            foreach ($myTransfer in $myTransferResult.Transfers)
            {
                Write-Log -Type INF -Content ("Upload of "+($myTransfer.FileName)+" succeeded.")
            }
            [bool]$myResult=$true
        }catch{
            Write-Log -Type ERR -Content ($_.ToString()).ToString()
        }
        finally
        {
            # Disconnect, clean up
            $mySession.Dispose()
        }
    }
    elseif($Operation -eq "DOWNLOAD")
    {
        try
        {
            # Connect
            $mySession.Open($mySessionOptions)
    
            # Download the file and throw on any error
            $mySessionResult = $mySession.GetFiles(($FtpDestinationPath),$SourceFilePath)
            
            # Throw error if found
            $mySessionResult.Check()
            $mySession.Output
            [bool]$myResult=$true
        }catch{
            Write-Log -Type ERR -Content ($_.ToString()).ToString()
        }
        finally
        {
            # Disconnect, clean up
            $mySession.Dispose()
        }    
    }
    elseif($Operation -eq "DIR")
    {
        try
        {
            # Connect
            $mySession.Open($mySessionOptions)
    
            # Download the file and throw on any error
            $mySessionResult = $mySession.ListDirectory($FtpDestinationPath)
            
            # Throw error if found
            $mySession.Output

            [bool]$myResult=$true
        }catch{
            Write-Log -Type ERR -Content ($_.ToString()).ToString()
        }
        finally
        {
            # Disconnect, clean up
            $mySession.Dispose()
        }  
    }
    elseif($Operation -eq "MKDIR")
    {
        try
        {
            # Connect
            $mySession.Open($mySessionOptions)
    
            # Create the directory and throw on any error
            $myPath=""
            [array]$myFolders = $FtpDestinationPath.Split("/")
            foreach ($myFolder in $myFolders)
            {
                if ($myFolder.ToString().Trim().Length -gt 0) 
                {
                    $myPath += "/" + $myFolder
                    if ($mySession.FileExists($myPath) -eq $false) {
                        $mySessionResult = $mySession.CreateDirectory($myPath)
                        Write-Log -Type INF -Content ("Create new directory on " + $myPath)
                    }
                }
            }
            
            # Throw error if found
            $mySession.Output

            [bool]$myResult=$true
        }catch{
            Write-Log -Type ERR -Content ($_.ToString()).ToString()
        }
        finally
        {
            # Disconnect, clean up
            $mySession.Dispose()
        }  
    }
    else 
    {
        Write-Log -Type INF -Content "Option not specified, must be upload/download/list"
    }

    return $myResult
}
Function SftpByWinscp {  #Upload file to SFTP path by winscp
    Param
        (
        [Parameter(Mandatory=$true)][string][ValidateSet("UPLOAD","DOWNLOAD","DELETE","MKDIR","DIR","ISALIVE", IgnoreCase=$true)]$Operation,
        [Parameter(Mandatory=$true)][string]$SftpServer,
        [Parameter(Mandatory=$true)][System.Net.NetworkCredential]$SftpCredential,   #New-Object System.Net.NetworkCredential($Env:FtpUsername, $Env:FtpPassword)
        [Parameter(Mandatory=$false)][string]$SftpSshKeyFingerprint,
        [Parameter(Mandatory=$false)][string]$SftpDestinationPath,
        [Parameter(Mandatory=$false)][string]$SourceFilePath,
        [Parameter(Mandatory=$false)][string]$WinscpPath="C:\Program Files (x86)\WinSCP\WinSCPnet.dll"
        )

    [bool]$myResult=$false
    Write-Log -Type INF -Content "Processing Started."
    # https://winscp.net/eng/docs/library_powershell#example
    # Testing variables
    <#
    $hostname = "test.rebex.net"
    $localPath = "C:\Winscp\"
    $remotePath = "/"
    $filename = "*.txt"
    $protocol = "sftp"
    $user = "demo"
    $password = 'password'
    $option = "list"
    $ssh = ""
    #>
    
    $SftpDestinationPath = $SftpDestinationPath.Replace("//","/")
    try
    {
        # Load WinSCP .NET assembly
        Add-Type -Path $WinscpPath
    }catch{
        Write-Log -Type ERR -Content ($_.ToString()).ToString()
    }

    # Setup session options
    if ($SftpCredential.Domain -eq ""){
        $myGeneratedUser=$SftpCredential.UserName
    }else{
        $myGeneratedUser=$SftpCredential.Domain+"\"+$SftpCredential.UserName
    }
    $mySessionOptions = New-Object WinSCP.SessionOptions -Property @{
        Protocol = "Sftp"
        HostName = $SftpServer
        UserName = $myGeneratedUser
        Password = $SftpCredential.Password
        SshHostKeyFingerprint = $SftpSshKeyFingerprint
    }

    $mySession = New-Object WinSCP.Session
    if($Operation -eq "ISALIVE")
    {
        try
        {
            # Connect
            $mySession.Open($mySessionOptions)        
            [bool]$myResult=$true
        }catch{
            Write-Log -Type ERR -Content ($_.ToString()).ToString()
        }
        finally
        {
            # Disconnect, clean up
            $mySession.Dispose()
        }
    }
    elseif($Operation -eq "UPLOAD")
    {
        try
        {
            # Connect
            $mySession.Open($mySessionOptions)

            # Upload files
            $myTransferOptions = New-Object WinSCP.TransferOptions
            $myTransferOptions.TransferMode = "Binary"

            $myTransferResult = $mySession.PutFiles(($SourceFilePath),$SftpDestinationPath, $False, $myTransferOptions)
        
            # Throw on any error
            $myTransferResult.Check()
            $mySession.Output
        
            # Print results
            foreach ($myTransfer in $myTransferResult.Transfers)
            {
                Write-Log -Type INF -Content ("Upload of "+($myTransfer.FileName)+" succeeded.")
            }
            [bool]$myResult=$true
        }catch{
            Write-Log -Type ERR -Content ($_.ToString()).ToString()
        }
        finally
        {
            # Disconnect, clean up
            $mySession.Dispose()
        }
    }
    elseif($Operation -eq "DOWNLOAD")
    {
        try
        {
            # Connect
            $mySession.Open($mySessionOptions)
    
            # Download the file and throw on any error
            $mySessionResult = $mySession.GetFiles(($SftpDestinationPath),$SourceFilePath)
            
            # Throw error if found
            $mySessionResult.Check()
            $mySession.Output
            [bool]$myResult=$true
        }catch{
            Write-Log -Type ERR -Content ($_.ToString()).ToString()
        }
        finally
        {
            # Disconnect, clean up
            $mySession.Dispose()
        }    
    }
    elseif($Operation -eq "DIR")
    {
        try
        {
            # Connect
            $mySession.Open($mySessionOptions)
    
            # Download the file and throw on any error
            $mySessionResult = $mySession.ListDirectory($SftpDestinationPath)
            
            # Throw error if found
            $mySession.Output

            [bool]$myResult=$true
        }catch{
            Write-Log -Type ERR -Content ($_.ToString()).ToString()
        }
        finally
        {
            # Disconnect, clean up
            $mySession.Dispose()
        }  
    }
    elseif($Operation -eq "MKDIR")
    {
        try
        {
            # Connect
            $mySession.Open($mySessionOptions)
    
            # Create the directory and throw on any error
            $myPath=""
            [array]$myFolders = $SftpDestinationPath.Split("/")
            foreach ($myFolder in $myFolders)
            {
                if ($myFolder.ToString().Trim().Length -gt 0) 
                {
                    $myPath += "/" + $myFolder
                    if ($mySession.FileExists($myPath) -eq $false) {
                        $mySessionResult = $mySession.CreateDirectory($myPath)
                        Write-Log -Type INF -Content ("Create new directory on " + $myPath)
                    }
                }
            }
            
            # Throw error if found
            $mySession.Output

            [bool]$myResult=$true
        }catch{
            Write-Log -Type ERR -Content ($_.ToString()).ToString()
        }
        finally
        {
            # Disconnect, clean up
            $mySession.Dispose()
        }  
    }
    else 
    {
        Write-Log -Type INF -Content "Option not specified, must be upload/download/list"
    }

    return $myResult
}
Function ScpByWinscp {  #Upload file to SCP path by winscp
    Param
        (
        [Parameter(Mandatory=$true)][string][ValidateSet("UPLOAD","DOWNLOAD","DELETE","MKDIR","DIR","ISALIVE", IgnoreCase=$true)]$Operation,
        [Parameter(Mandatory=$true)][string]$ScpServer,
        [Parameter(Mandatory=$true)][System.Net.NetworkCredential]$ScpCredential,   #New-Object System.Net.NetworkCredential($Env:FtpUsername, $Env:FtpPassword)
        [Parameter(Mandatory=$false)][string]$ScpSshKeyFingerprint,
        [Parameter(Mandatory=$false)][string]$ScpDestinationPath,
        [Parameter(Mandatory=$false)][string]$SourceFilePath,
        [Parameter(Mandatory=$false)][string]$WinscpPath="C:\Program Files (x86)\WinSCP\WinSCPnet.dll"
        )

    [bool]$myResult=$false
    Write-Log -Type INF -Content "Processing Started."
    # https://winscp.net/eng/docs/library_powershell#example
    # Testing variables
    <#
    $hostname = "test.rebex.net"
    $localPath = "C:\Winscp\"
    $remotePath = "/"
    $filename = "*.txt"
    $protocol = "sftp"
    $user = "demo"
    $password = 'password'
    $option = "list"
    $ssh = ""
    #>
    
    $ScpDestinationPath = $ScpDestinationPath.Replace("//","/")
    try
    {
        # Load WinSCP .NET assembly
        Add-Type -Path $WinscpPath
    }catch{
        Write-Log -Type ERR -Content ($_.ToString()).ToString()
    }

    # Setup session options
    if ($ScpCredential.Domain -eq ""){
        $myGeneratedUser=$ScpCredential.UserName
    }else{
        $myGeneratedUser=$ScpCredential.Domain+"\"+$ScpCredential.UserName
    }
    $mySessionOptions = New-Object WinSCP.SessionOptions -Property @{
        Protocol = "scp"
        HostName = $ScpServer
        UserName = $myGeneratedUser
        Password = $ScpCredential.Password
        SshHostKeyFingerprint = $ScpSshKeyFingerprint
    }

    $mySession = New-Object WinSCP.Session
    if($Operation -eq "ISALIVE")
    {
        try
        {
            # Connect
            $mySession.Open($mySessionOptions)        
            [bool]$myResult=$true
        }catch{
            Write-Log -Type ERR -Content ($_.ToString()).ToString()
        }
        finally
        {
            # Disconnect, clean up
            $mySession.Dispose()
        }
    }
    elseif($Operation -eq "UPLOAD")
    {
        try
        {
            # Connect
            $mySession.Open($mySessionOptions)

            # Upload files
            $myTransferOptions = New-Object WinSCP.TransferOptions
            $myTransferOptions.TransferMode = "Binary"

            $myTransferResult = $mySession.PutFiles(($SourceFilePath),$ScpDestinationPath, $False, $myTransferOptions)
        
            # Throw on any error
            $myTransferResult.Check()
            $mySession.Output
        
            # Print results
            foreach ($myTransfer in $myTransferResult.Transfers)
            {
                Write-Log -Type INF -Content ("Upload of "+($myTransfer.FileName)+" succeeded.")
            }
            [bool]$myResult=$true
        }catch{
            Write-Log -Type ERR -Content ($_.ToString()).ToString()
        }
        finally
        {
            # Disconnect, clean up
            $mySession.Dispose()
        }
    }
    elseif($Operation -eq "DOWNLOAD")
    {
        try
        {
            # Connect
            $mySession.Open($mySessionOptions)
    
            # Download the file and throw on any error
            $mySessionResult = $mySession.GetFiles(($ScpDestinationPath),$SourceFilePath)
            
            # Throw error if found
            $mySessionResult.Check()
            $mySession.Output
            [bool]$myResult=$true
        }catch{
            Write-Log -Type ERR -Content ($_.ToString()).ToString()
        }
        finally
        {
            # Disconnect, clean up
            $mySession.Dispose()
        }    
    }
    elseif($Operation -eq "DIR")
    {
        try
        {
            # Connect
            $mySession.Open($mySessionOptions)
    
            # Download the file and throw on any error
            $mySessionResult = $mySession.ListDirectory($ScpDestinationPath)
            
            # Throw error if found
            $mySession.Output

            [bool]$myResult=$true
        }catch{
            Write-Log -Type ERR -Content ($_.ToString()).ToString()
        }
        finally
        {
            # Disconnect, clean up
            $mySession.Dispose()
        }  
    }
    elseif($Operation -eq "MKDIR")
    {
        try
        {
            # Connect
            $mySession.Open($mySessionOptions)
    
            # Create the directory and throw on any error
            $myPath=""
            [array]$myFolders = $ScpDestinationPath.Split("/")
            foreach ($myFolder in $myFolders)
            {
                if ($myFolder.ToString().Trim().Length -gt 0) 
                {
                    $myPath += "/" + $myFolder
                    if ($mySession.FileExists($myPath) -eq $false) {
                        $mySessionResult = $mySession.CreateDirectory($myPath)
                        Write-Log -Type INF -Content ("Create new directory on " + $myPath)
                    }
                }
            }
            
            # Throw error if found
            $mySession.Output

            [bool]$myResult=$true
        }catch{
            Write-Log -Type ERR -Content ($_.ToString()).ToString()
        }
        finally
        {
            # Disconnect, clean up
            $mySession.Dispose()
        }  
    }
    else 
    {
        Write-Log -Type INF -Content "Option not specified, must be upload/download/list"
    }

    return $myResult
}
Function SourceInstance.ConnectivityTest {  #Test Source Instance connectivity
    Param
        (
        [Parameter(Mandatory=$true)][string]$SourceInstanceConnectionString
        )

    Write-Log -Type INF -Content "Processing Started."
    $myCommand="
        USE [msdb];
        SELECT 1 AS Result;"
    try{
        $myRecord=Invoke-Sqlcmd -ConnectionString $SourceInstanceConnectionString -Query $myCommand -OutputSqlErrors $true -QueryTimeout 0 -OutputAs DataRows -ErrorAction Stop
    }Catch{
        Write-Log -Type ERR -Content ($_.ToString()).ToString()
    }
    if ($null -ne $myRecord) {return [bool]$true}else {return [bool]$false}
}
Function SourceInstance.GetInstanceName {  #Get Source Instance Name
    Param
        (
        [Parameter(Mandatory=$true)][string]$SourceInstanceConnectionString
        )

    Write-Log -Type INF -Content "Processing Started."
    $myCommand="
        SELECT @@ServerName AS InstanceName;"
    try{
        $myRecord=Invoke-Sqlcmd -ConnectionString $SourceInstanceConnectionString -Query $myCommand -OutputSqlErrors $true -QueryTimeout 0 -OutputAs DataRows -ErrorAction Stop
    }Catch{
        Write-Log -Type ERR -Content ($_.ToString()).ToString()
    }
    if ($null -ne $myRecord) {return $myRecord.InstanceName}else {return ""}
}
Function Destination.ConnectivityTest {  #Test Destination connectivity
    Param
        (
        [Parameter(Mandatory=$true)][string][ValidateSet("UNC","FTP","SFTP", IgnoreCase=$true)]$DestinationType,
        [Parameter(Mandatory=$true)][string]$Destination
        )

    Write-Log -Type INF -Content "Processing Started."
    try{
        $myRecord=Invoke-Sqlcmd -ConnectionString $SourceInstanceConnectionString -Query $myCommand -OutputSqlErrors $true -QueryTimeout 0 -OutputAs DataRows -ErrorAction Stop
    }Catch{
        Write-Log -Type ERR -Content ($_.ToString()).ToString()
    }
    if ($null -ne $myRecord) {return [bool]$true}else {return [bool]$false}
}
Function SourceInstance.GetUntransferredBackups {  #Get list of untransferred backup files list
    Param
        (
        [Parameter(Mandatory=$true)][string]$SourceInstanceConnectionString,
        [Parameter(Mandatory=$true)][string][ValidateSet("All_Databases","User_Databases","System_Databases", IgnoreCase=$true)]$DatabasesToTransfer,
        [Parameter(Mandatory=$false)][string]$ExceptedDatabasesForTransfer,
        [Parameter(Mandatory=$true)][string][ValidateSet("ALL","FULL","DIFF","LOG", IgnoreCase=$true)]$BackupTypeToTransfer,
        [Parameter(Mandatory=$true)][int]$HoursToScanForUntransferredBackups,
        [Parameter(Mandatory=$true)][string]$TransferedSuffix
        )

    Write-Log -Type INF -Content "Processing Started."
    $myCommand="
    DECLARE @DatabasesToTransfer NVARCHAR(4000);
    DECLARE @ExceptedDatabasesForTransfer NVARCHAR(4000);
    DECLARE @BackupTypeToTransfer NVARCHAR(50);
    DECLARE @HoursToScanForUntransferredBackups INT;
    DECLARE @TransferedSuffix NVARCHAR(20);
    DECLARE @myCurrentDateTime DATETIME;
    DECLARE @myDelimiter NVARCHAR(5);
    
    SET @myCurrentDateTime = GETDATE();
    SET @myDelimiter = N',';
    SET @DatabasesToTransfer = UPPER('"+ $DatabasesToTransfer +"');
    SET @ExceptedDatabasesForTransfer = N'"+ $ExceptedDatabasesForTransfer +"';
    SET @BackupTypeToTransfer = UPPER('"+ $BackupTypeToTransfer +"');
    SET @HoursToScanForUntransferredBackups = "+ $HoursToScanForUntransferredBackups.ToString() +";
    SET @TransferedSuffix = N'"+ $TransferedSuffix +"';
    
    --Create list of excepted databases
    CREATE TABLE [#ExceptedDatabasesForTransfer] ([DatabaseName] sysname);
    IF (@ExceptedDatabasesForTransfer IS NOT NULL AND	LEN(RTRIM(LTRIM(@ExceptedDatabasesForTransfer))) > 0)
    BEGIN
        WITH [Pieces] ([Position], [start], [stop]) AS (
            SELECT
                CAST(1 AS BIGINT),
                CAST(1 AS BIGINT),
                CAST(CHARINDEX(@myDelimiter, @ExceptedDatabasesForTransfer) AS BIGINT)
            UNION ALL
            SELECT
                CAST([Pieces].[Position] + 1 AS BIGINT),
                CAST([Pieces].[stop] + 1 AS BIGINT),
                CAST(CHARINDEX(@myDelimiter, @ExceptedDatabasesForTransfer, [Pieces].[stop] + 1) AS BIGINT)
            FROM
                [Pieces]
            WHERE
                [Pieces].[stop] > 0
        )
        INSERT INTO [#ExceptedDatabasesForTransfer] ([DatabaseName])
        SELECT
            CAST(SUBSTRING(
                              @ExceptedDatabasesForTransfer, [Pieces].[start],
                              CASE
                                  WHEN [Pieces].[stop] > 0 THEN [Pieces].[stop] - [Pieces].[start]
                                  ELSE LEN(@ExceptedDatabasesForTransfer)
                              END
                          ) AS sysname) AS [DatabaseName]
        FROM
            [Pieces];
    END;
    
    --Create list of valid databases
    CREATE TABLE [#myDatabasesToTransfer] ([Database_Id]  INT PRIMARY KEY,[DatabaseName] sysname UNIQUE);
    INSERT INTO [#myDatabasesToTransfer] ([Database_Id], [DatabaseName])
    SELECT
        [myDatabases].[database_id],
        [myDatabases].[name]
    FROM
        [master].[sys].[databases] AS [myDatabases] WITH (READPAST)
    WHERE
        @DatabasesToTransfer = UPPER('All_Databases')
        AND [myDatabases].[name] NOT IN ('tempdb')
    UNION ALL
    SELECT
        [myDatabases].[database_id],
        [myDatabases].[name]
    FROM
        [master].[sys].[databases] AS [myDatabases] WITH (READPAST)
    WHERE
        @DatabasesToTransfer = UPPER('User_Databases')
        AND [myDatabases].[database_id] > 4
        AND [myDatabases].[name] NOT IN ('SSISDB', 'tempdb')
    UNION ALL
    SELECT
        [myDatabases].[database_id],
        [myDatabases].[name]
    FROM
        [master].[sys].[databases] AS [myDatabases] WITH (READPAST)
    WHERE
        @DatabasesToTransfer = UPPER('System_Databases')
        AND (
                [myDatabases].[database_id] <= 4
                OR	[myDatabases].[name] IN ('SSISDB')
            );
    DELETE FROM [#myDatabasesToTransfer] WHERE [DatabaseName] IN (SELECT [DatabaseName]	FROM [#ExceptedDatabasesForTransfer]);
    
    --Create list of valid Backup Types
    CREATE TABLE [#myBackupTypeToTransfer] ([BackupType] CHAR(1) PRIMARY KEY,[BackupTypeName] NVARCHAR(50) UNIQUE);
    IF (PATINDEX('%FULL%', @BackupTypeToTransfer) IS NOT NULL AND PATINDEX('%FULL%', @BackupTypeToTransfer) != 0)
        INSERT INTO [#myBackupTypeToTransfer] ([BackupType], [BackupTypeName]) VALUES ('D', 'FULL');
    IF (PATINDEX('%LOG%', @BackupTypeToTransfer) IS NOT NULL AND	PATINDEX('%LOG%', @BackupTypeToTransfer) != 0)
        INSERT INTO [#myBackupTypeToTransfer] ([BackupType], [BackupTypeName]) VALUES ('L', 'LOG');
    IF (PATINDEX('%DIFF%', @BackupTypeToTransfer) IS NOT NULL AND	PATINDEX('%DIFF%', @BackupTypeToTransfer) != 0)
        INSERT INTO [#myBackupTypeToTransfer] ([BackupType], [BackupTypeName]) VALUES ('I', 'DIFF');
    IF (PATINDEX('%ALL%', @BackupTypeToTransfer) IS NOT NULL AND	PATINDEX('%ALL%', @BackupTypeToTransfer) != 0) OR NOT EXISTS (SELECT 1 FROM [#myBackupTypeToTransfer])
        MERGE [#myBackupTypeToTransfer] AS [myTarget]
        USING (SELECT 'D','FULL' UNION SELECT 'L','LOG' UNION SELECT 'I','DIFF') AS [mySource] ([BackupType], [BackupTypeName])
        ON ([myTarget].[BackupType] = [mySource].[BackupType])
        WHEN NOT MATCHED THEN 
        INSERT ([BackupType],[BackupTypeName])
        VALUES ([mySource].[BackupType], [mySource].[BackupTypeName]);
    
    
    SELECT
        [myMediaSet].[media_set_id],																												--PK
        CAST([myMediaSet].[family_sequence_number] AS INT)															 AS [family_sequence_number],	--PK
        UPPER([myUniqueBackupSet].[machine_name])																	 AS [MachineName],
        UPPER([myUniqueBackupSet].[server_name])																	 AS [InstanceName],
        [myUniqueBackupSet].[database_name]																			 AS [DatabaseName],
        [myUniqueBackupSet].[backup_start_date]																		 AS [backup_start_date],
        [myUniqueBackupSet].[backup_finish_date]																	 AS [backup_finish_date],
        [myUniqueBackupSet].[expiration_date]																		 AS [expiration_date],
        UPPER([myUniqueBackupSet].[type])																			 AS [BackupType],
        CAST([myUniqueBackupSet].[first_lsn] AS DECIMAL(25, 0))														 AS [BackupFirstLSN],
        CAST([myUniqueBackupSet].[last_lsn] AS DECIMAL(25, 0))														 AS [BackupLastLSN],
        [myMediaSet].[physical_device_name]																			 AS [BackupFilePath],
        RIGHT([myMediaSet].[physical_device_name], CHARINDEX('\', REVERSE([myMediaSet].[physical_device_name])) - 1) AS [BackupFileName],
        MAX(CAST([myMediaSet].[family_sequence_number] AS INT)) OVER (PARTITION BY [myMediaSet].[media_set_id])		 AS [max_family_sequence_number]
    FROM
        [msdb].[dbo].[backupmediafamily] AS [myMediaSet]
        INNER JOIN (
                       SELECT
                            [myBackupSet].[media_set_id],
                            MAX([myBackupSet].[machine_name])		AS [machine_name],
                            MAX([myBackupSet].[server_name])		AS [server_name],
                            MAX([myBackupSet].[database_name])		AS [database_name],
                            MAX([myBackupSet].[backup_start_date])	AS [backup_start_date],
                            MAX([myBackupSet].[backup_finish_date]) AS [backup_finish_date],
                            MAX([myBackupSet].[expiration_date])	AS [expiration_date],
                            MAX([myBackupSet].[type])				AS [type],
                            MIN([myBackupSet].[first_lsn])			AS [first_lsn],
                            MAX([myBackupSet].[last_lsn])			AS [last_lsn]
                       FROM
                            [msdb].[dbo].[backupset]			AS [myBackupSet]
                            INNER JOIN [#myDatabasesToTransfer] AS [myDatabasesToTransfer] ON [myBackupSet].[database_name] = [myDatabasesToTransfer].[DatabaseName]
                       WHERE
                            [myBackupSet].[backup_finish_date] IS NOT NULL
                            AND [myBackupSet].[backup_start_date] >= DATEADD(
                                                                                HOUR,
                                                                                -1 * @HoursToScanForUntransferredBackups,
                                                                                @myCurrentDateTime
                                                                            )
                            AND [myBackupSet].[server_name] = @@ServerName
                            AND [myBackupSet].[description] NOT LIKE '%' + @TransferedSuffix + '%'
                            AND [myBackupSet].[type] IN (
                                                            SELECT [BackupType]	 FROM [#myBackupTypeToTransfer]
                                                        )
                       GROUP BY
                            [myBackupSet].[media_set_id]
                   )					 AS [myUniqueBackupSet] ON [myUniqueBackupSet].[media_set_id] = [myMediaSet].[media_set_id]
    WHERE
        [myMediaSet].[mirror] = 0
        AND [myMediaSet].[physical_device_name] LIKE '_:%'
    ORDER BY
        [myUniqueBackupSet].[backup_start_date] ASC,
        [myMediaSet].[media_set_id] ASC;
    
    DROP TABLE [#myBackupTypeToTransfer];
    DROP TABLE [#myDatabasesToTransfer];
    DROP TABLE [#ExceptedDatabasesForTransfer];
    "
    try{
        $myRecord=Invoke-Sqlcmd -ConnectionString $SourceInstanceConnectionString -Query $myCommand -OutputSqlErrors $true -QueryTimeout 0 -OutputAs DataRows -ErrorAction Stop
    }Catch{
        Write-Log -Type ERR -Content ($_.ToString()).ToString()
        Write-Log -Type ERR -Content $myCommand
    }
    if ($null -ne $myRecord) {return $myRecord}
}
Function SourceInstance.SetBackupsToTransferred {  #Set backup file(s) to transffered
    Param
        (
        [Parameter(Mandatory=$true)][string]$SourceInstanceConnectionString,
        [Parameter(Mandatory=$true)][int]$MediaSetId,
        [Parameter(Mandatory=$false)][datetime]$BackupFinishDate,
        [Parameter(Mandatory=$true)][string]$TransferedSuffix
        )

    Write-Log -Type INF -Content "Processing Started."
    $myCommand="
    DECLARE @MediaSetId INT;
    DECLARE @BackupFinishDate DATETIME;
    DECLARE @TransferedSuffix NVARCHAR(20);

    SET @MediaSetId = "+ $MediaSetId.ToString() +";
    SET @BackupFinishDate = CAST(N'"+ $BackupFinishDate.ToString() +"' AS DATETIME);
    SET @TransferedSuffix = N'"+ $TransferedSuffix +"';
    
    --Update backup description
    UPDATE [msdb].[dbo].[backupset] SET 
        [description] = [description]+@TransferedSuffix 
    WHERE 
        media_set_id=@MediaSetId 
        AND [backup_finish_date] IS NOT NULL 
        AND [backup_finish_date] <= @BackupFinishDate 
        AND [description] NOT LIKE '%'+@TransferedSuffix + '%'
    "
    try{
        $myRecord=Invoke-Sqlcmd -ConnectionString $SourceInstanceConnectionString -Query $myCommand -OutputSqlErrors $true -QueryTimeout 0 -OutputAs DataRows -ErrorAction Stop
    }Catch{
        Write-Log -Type ERR -Content ($_.ToString()).ToString()
        Write-Log -Type ERR -Content $myCommand
    }
    if ($null -ne $myRecord) {return $true}
}

#---------------------------------------------------------MAIN BODY
#--=======================Initial Log Modules
Write-Log -Type INF -Content "BackupTransfer started..."
$mySysErrCount=0
$mySysWrnCount=0
$mySysTransferredFilesTableName="[dbo].[TransferredFiles]"
$mySysTransferredFilesLogFeature=[bool]$false
$mySysEventsLogToTableFeature=[bool]$false
$mySysBatchId=(New-Guid).ToString()
if (!($DestinationCredential) -and $DestinationCredentialName) {$DestinationCredential = (Get-StoredCredential -Target $DestinationCredentialName).GetNetworkCredential()}

Write-Log -Type INF -Content ("Initializing EventsTable.Create.")
if ($null -ne $LogInstanceConnectionString) {$mySysEventsLogToTableFeature=EventsTable.Create -LogInstanceConnectionString $LogInstanceConnectionString -TableName $LogTableName} else {$mySysEventsLogToTableFeature=[bool]$false}
if ($mySysEventsLogToTableFeature -eq $false)  {Write-Log -Type WRN -Content "Can not initialize a table to save program logs."}

Write-Log -Type INF -Content ("Initializing TransferredFilesTable.Create.")
if ($null -ne $LogInstanceConnectionString) {$mySysTransferredFilesLogFeature=TransferredFilesTable.Create -LogInstanceConnectionString $LogInstanceConnectionString -TableName $mySysTransferredFilesTableName} else {$mySysTransferredFilesLogFeature=[bool]$false}
if ($mySysTransferredFilesLogFeature -eq $false)  {Write-Log -Type ERR -Content ("Can not initialize a table to save file transfer logs on " + $LogInstanceConnectionString + " to " + $mySysTransferredFilesTableName + " table.") -Terminate}

#--=======================Check source connectivity
Write-Log -Type INF -Content ("Check Source Instance Connectivity to " + $SourceInstanceConnectionString)
if ((SourceInstance.ConnectivityTest -SourceInstanceConnectionString $SourceInstanceConnectionString) -eq $false) {
    Write-Log -Type ERR -Content ("Source Instance Connection failure.") -Terminate
} 

Write-Log -Type INF -Content ("Get Source Instance Name of " + $SourceInstanceConnectionString)
$mySysSourceInstanceName=SourceInstance.GetInstanceName -SourceInstanceConnectionString $SourceInstanceConnectionString

#--=======================Load Required Modules
Write-Log -Type INF -Content ("Load Required Modules")
if (!(Get-Module -ListAvailable -Name CredentialManager)) {Install-Module CredentialManager -force -Scope CurrentUser}
Import-Module CredentialManager

#--=======================Check destination connectivity
Write-Log -Type INF -Content ("Check Destination Connectivity with DestinationType of " + $DestinationType + ", Destionation location of " + $Destination + " and DestinationCredential Username of " + $DestinationCredential.UserName)
$myDestinationIsAlive = switch ($DestinationType) 
    {
        "FTP"   {FtpByWinscp -Operation ISALIVE -FtpServer $Destination -FtpCredential $DestinationCredential -WinscpPath $WinscpPath}
        "SFTP"  {SftpByWinscp -Operation ISALIVE -SftpServer $Destination -SftpCredential $DestinationCredential -WinscpPath $WinscpPath -SftpSshKeyFingerprint $SshHostKeyFingerprint}
        "SCP"   {ScpByWinscp -Operation ISALIVE -ScpServer $Destination -ScpCredential $DestinationCredential -WinscpPath $WinscpPath -ScpSshKeyFingerprint $SshHostKeyFingerprint}
        "UNC"   {UNC.IsAlive -UncSharedFolderPath $Destination -UncCredential $DestinationCredential -TemporalDriveLetter "A"}
    }
if ($myDestinationIsAlive -eq $false){
    Write-Log -Type ERR -Content "Destination is not avilable." -Terminate
}

#--=======================Get files to transfer
Write-Log -Type INF -Content ("Get list of untransferred backup files from " + $SourceInstanceConnectionString + " with DatabasesToTransfer=" + $DatabasesToTransfer + ", ExceptedDatabasesForTransfer=" + $ExceptedDatabasesForTransfer + ", BackupTypeToTransfer=" + $BackupTypeToTransfer + ", HoursToScanForUntransferredBackups=" + $HoursToScanForUntransferredBackups + ", TransferedSuffix=" + $TransferedSuffix)
$myUntransferredBackups=SourceInstance.GetUntransferredBackups -SourceInstanceConnectionString $SourceInstanceConnectionString -DatabasesToTransfer $DatabasesToTransfer -ExceptedDatabasesForTransfer $ExceptedDatabasesForTransfer -BackupTypeToTransfer $BackupTypeToTransfer -HoursToScanForUntransferredBackups $HoursToScanForUntransferredBackups -TransferedSuffix $TransferedSuffix
if ($null -eq $myUntransferredBackups) {
    Write-Log -Type INF -Content "There is no file(s) to transfer." -Terminate
}

#--=======================Create folder structure in destination
Write-Log -Type INF -Content ("Create folder structure on destination " + $Destination + " With path structure of " + $DestinationFolderStructure)
$myPersianCalendar=New-Object system.globalization.persiancalendar
$myPersianDaysOfWeekMap=@{6="1";0="2";1="3";2="4";3="5";4="6";5="7"}
$myUnderZeroNumbers=@{}
1..31 | ForEach-Object {$myPrefix=IF ($_ -le 9) {"0"} else {""}; $myUnderZeroNumbers.Add($_,$myPrefix)}
$myUntransferredBackups | ForEach-Object {
    $myDestinationFolder=$DestinationFolderStructure
    $myBackupStartDate=$_.backup_start_date
    $myJalaliMonth=$myPersianCalendar.GetMonth($myBackupStartDate)
    $myJalaliDayOfMonth=$myPersianCalendar.GetDayOfMonth($myBackupStartDate)
    $myJalaliDayOfWeek=$myPersianDaysOfWeekMap.Item($myBackupStartDate.DayOfWeek.value__)
    
    $myDestinationFolder=$myDestinationFolder.
    Replace("{Year}",$myBackupStartDate.ToString("yyyy")).
    Replace("{Month}",$myBackupStartDate.ToString("MM")).
    Replace("{Day}",$myBackupStartDate.ToString("dd")).
    Replace("{DayOfWeek}",([int]$myBackupStartDate.DayOfWeek).ToString()).
    Replace("{JYear}",$myPersianCalendar.GetYear($myBackupStartDate).ToString()).
    Replace("{JMonth}",$myUnderZeroNumbers.Item($myPersianCalendar.GetMonth($myBackupStartDate))+$myPersianCalendar.GetMonth($myBackupStartDate).ToString()).
    Replace("{JDay}",$myUnderZeroNumbers.Item($myPersianCalendar.GetDayOfMonth($myBackupStartDate))+$myPersianCalendar.GetDayOfMonth($myBackupStartDate).ToString()).
    Replace("{JDayOfWeek}",$myPersianDaysOfWeekMap.Item($myBackupStartDate.DayOfWeek.value__)).
    Replace("{InstanceName}",$_.InstanceName.Replace("\","_")).
    Replace("{DatabaseName}",$_.DatabaseName.Replace(" ","_"))
    IF ($myDestinationFolder -like "*{CustomRule01}*") {
        $myRuleTemplate="{CustomRule01}"
        $myTemporalDestinationFolder=""
        $myBackupType=$_.BackupType
        IF ($myBackupType -eq "L") {$myDestinationFolder=$myDestinationFolder.Replace($myRuleTemplate, "disk_only")} ELSE {$myDestinationFolder=$myDestinationFolder.Replace($myRuleTemplate, "tape_only")}
    }
    IF ($myDestinationFolder -like "*{CustomRule02}*") {
        $myRuleTemplate="{CustomRule02}"
        $myTemporalDestinationFolder=""
        IF ($myJalaliMonth -eq 1 -and $myJalaliDayOfMonth -eq 1) {$myTemporalDestinationFolder+=$myDestinationFolder.Replace($myRuleTemplate, "Yearly")+";"}
        ELSEIF ($myJalaliDayOfMonth -eq 1) {$myTemporalDestinationFolder+=$myDestinationFolder.Replace($myRuleTemplate, "Monthly")+";"}
        ELSEIF ($myJalaliDayOfWeek -eq "1") {$myTemporalDestinationFolder+=$myDestinationFolder.Replace($myRuleTemplate, "Weekly")+";"}
        ELSE {$myTemporalDestinationFolder+=$myDestinationFolder.Replace($myRuleTemplate, "Daily")}
        IF ($myTemporalDestinationFolder.Length -gt 0) {$myDestinationFolder=$myTemporalDestinationFolder}
    }
    Add-Member -InputObject $_ -NotePropertyName "DestinationFolder" -NotePropertyValue $myDestinationFolder
}

#$myPathList = $myUntransferredBackups | Group-Object -Property DestinationFolder -NoElement | Select-Object -Property Name | ForEach-Object {$_.Name.Split(";")}
#--Split DestinationFolders with multiple values seperated by ";" to multiple rows
[System.Collections.ArrayList]$myPathList = @()
ForEach ($myPath IN ($myUntransferredBackups | Group-Object -Property DestinationFolder -NoElement | Select-Object -Property Name | ForEach-Object {$_.Name.Split(";")} )) {
    $myItem = [pscustomobject]@{'FolderPath'=$myPath;'date'=(Get-Date)}
    $myPathList.add($myItem) | Out-Null
    $myItem=$null
}

switch ($DestinationType) 
    {
        "FTP"   {$myPathList | ForEach-Object {FtpByWinscp -Operation MKDIR -FtpServer $Destination -FtpCredential $DestinationCredential -WinscpPath $WinscpPath -FtpDestinationPath $_.FolderPath}}
        "SFTP"  {$myPathList | ForEach-Object {SftpByWinscp -Operation MKDIR -SftpServer $Destination -SftpCredential $DestinationCredential -WinscpPath $WinscpPath -SftpDestinationPath $_.FolderPath -SftpSshKeyFingerprint $SshHostKeyFingerprint}}
        "SCP"   {$myPathList | ForEach-Object {ScpByWinscp -Operation MKDIR -ScpServer $Destination -ScpCredential $DestinationCredential -WinscpPath $WinscpPath -ScpDestinationPath $_.FolderPath -ScpSshKeyFingerprint $SshHostKeyFingerprint}}
        "UNC"   {$myPathList | ForEach-Object {UNC.MKDIR -UncSharedFolderPath $Destination -UncCredential $DestinationCredential -UncDestinationPath $_.FolderPath -TemporalDriveLetter "A"}}
        "LOCAL" {$myPathList | ForEach-Object {UNC.MKDIR -UncSharedFolderPath $Destination -UncCredential $DestinationCredential -UncDestinationPath $_.FolderPath -TemporalDriveLetter "A"}}
    }

#--=======================Transfer file(s) to destination
Write-Log -Type INF -Content ("Transfer file(s) from source to destination is started.")
$mySysSourceMachineName=($myUntransferredBackups | Select-Object -Property MachineName -First 1).MachineName.ToUpper()
$mySysCurrentMachineName=([Environment]::MachineName).ToUpper()
$myUseUncSource=[bool]$false
if ($mySysSourceMachineName -ne $mySysCurrentMachineName) {
    Write-Log -Type INF -Content ("File(s) Source machine name ("+$mySysSourceMachineName+") is not same as Current machine name ("+$mySysCurrentMachineName+") then source path will be updated to UNC source path.")
    $myUntransferredBackups | Add-Member -MemberType ScriptProperty -Name "UncBackupFilePath" -Value {"\\"+$this.MachineName+"\"+$this.BackupFilePath.Replace(":","$")}
    $myUseUncSource=$true
}

switch ($DestinationType) 
    {
        "FTP"   {$myUntransferredBackups | ForEach-Object {
                                                            $mySourceFile=if($myUseUncSource -eq $false){$_.BackupFilePath}else{$_.UncBackupFilePath}
                                                            ForEach ($myPath IN $_.DestinationFolder.Split(";"))
                                                            {
                                                                $mySendResult=FtpByWinscp -Operation UPLOAD -FtpServer $Destination -FtpCredential $DestinationCredential -WinscpPath $WinscpPath -FtpDestinationPath ($myPath+"/"+$_.BackupFileName) -SourceFilePath $mySourceFile
                                                                if($mySendResult -eq $true) {
                                                                    SourceInstance.SetBackupsToTransferred -SourceInstanceConnectionString $SourceInstanceConnectionString -MediaSetId ($_.media_set_id) -BackupFinishDate ($_.backup_finish_date) -TransferedSuffix $TransferedSuffix
                                                                    TransferredFilesTable.Insert -LogInstanceConnectionString $LogInstanceConnectionString -TableName $mySysTransferredFilesTableName -BatchId $mySysBatchId -Destination $Destination -Record $_
                                                                }
                                                            }
                                                        }
                }
        "SFTP"  {$myUntransferredBackups | ForEach-Object {
                                                            $mySourceFile=if($myUseUncSource -eq $false){$_.BackupFilePath}else{$_.UncBackupFilePath}
                                                            ForEach ($myPath IN $_.DestinationFolder.Split(";"))
                                                            {
                                                                $mySendResult=SftpByWinscp -Operation UPLOAD -SftpServer $Destination -SftpCredential $DestinationCredential -WinscpPath $WinscpPath -SftpDestinationPath ($myPath+"/"+$_.BackupFileName) -SourceFilePath $mySourceFile -SftpSshKeyFingerprint $SshHostKeyFingerprint
                                                                if($mySendResult -eq $true) {
                                                                    SourceInstance.SetBackupsToTransferred -SourceInstanceConnectionString $SourceInstanceConnectionString -MediaSetId ($_.media_set_id) -BackupFinishDate ($_.backup_finish_date) -TransferedSuffix $TransferedSuffix
                                                                    TransferredFilesTable.Insert -LogInstanceConnectionString $LogInstanceConnectionString -TableName $mySysTransferredFilesTableName -BatchId $mySysBatchId -Destination $Destination -Record $_
                                                                }
                                                            }
                                                        }
                }
        "SCP"   {$myUntransferredBackups | ForEach-Object {
                                                            $mySourceFile=if($myUseUncSource -eq $false){$_.BackupFilePath}else{$_.UncBackupFilePath}
                                                            ForEach ($myPath IN $_.DestinationFolder.Split(";"))
                                                            {
                                                                $mySendResult=ScpByWinscp -Operation UPLOAD -ScpServer $Destination -ScpCredential $DestinationCredential -WinscpPath $WinscpPath -ScpDestinationPath ($myPath+"/"+$_.BackupFileName) -SourceFilePath $mySourceFile -ScpSshKeyFingerprint $SshHostKeyFingerprint
                                                                if($mySendResult -eq $true) {
                                                                    SourceInstance.SetBackupsToTransferred -SourceInstanceConnectionString $SourceInstanceConnectionString -MediaSetId ($_.media_set_id) -BackupFinishDate ($_.backup_finish_date) -TransferedSuffix $TransferedSuffix
                                                                    TransferredFilesTable.Insert -LogInstanceConnectionString $LogInstanceConnectionString -TableName $mySysTransferredFilesTableName  -BatchId $mySysBatchId -Destination $Destination -Record $_
                                                                }
                                                            }
                                                        }
                }
        "UNC"   {$myUntransferredBackups | ForEach-Object {
                                                            $mySourceFile=if($myUseUncSource -eq $false){$_.BackupFilePath}else{$_.UncBackupFilePath}
                                                            ForEach ($myPath IN $_.DestinationFolder.Split(";")) 
                                                            {
                                                                $mySendResult=UNC.UPLOAD -UncSharedFolderPath $Destination -UncCredential $DestinationCredential -UncDestinationPath ($myPath+"\"+$_.BackupFileName) -TemporalDriveLetter "A" -SourceFilePath $mySourceFile -ActionType $ActionType
                                                                if($mySendResult -eq $true) {
                                                                    SourceInstance.SetBackupsToTransferred -SourceInstanceConnectionString $SourceInstanceConnectionString -MediaSetId ($_.media_set_id) -BackupFinishDate ($_.backup_finish_date) -TransferedSuffix $TransferedSuffix
                                                                    TransferredFilesTable.Insert -LogInstanceConnectionString $LogInstanceConnectionString -TableName $mySysTransferredFilesTableName -BatchId $mySysBatchId -Destination $Destination -Record $_
                                                                }
                                                            } 
                                                        }
                }
        "LOCAL" {$myUntransferredBackups | ForEach-Object {
                                                            $mySourceFile=if($myUseUncSource -eq $false){$_.BackupFilePath}else{$_.UncBackupFilePath}
                                                            ForEach ($myPath IN $_.DestinationFolder.Split(";")) 
                                                            {
                                                                $mySendResult=UNC.UPLOAD -UncSharedFolderPath $Destination -UncCredential $DestinationCredential -UncDestinationPath ($myPath+"\"+$_.BackupFileName) -TemporalDriveLetter "A" -SourceFilePath $mySourceFile -ActionType $ActionType
                                                                if($mySendResult -eq $true) {
                                                                    SourceInstance.SetBackupsToTransferred -SourceInstanceConnectionString $SourceInstanceConnectionString -MediaSetId ($_.media_set_id) -BackupFinishDate ($_.backup_finish_date) -TransferedSuffix $TransferedSuffix
                                                                    TransferredFilesTable.Insert -LogInstanceConnectionString $LogInstanceConnectionString -TableName $mySysTransferredFilesTableName -BatchId $mySysBatchId -Destination $Destination -Record $_
                                                                }
                                                            } 
                                                        }
                }
    }

#--=======================Set Delete date for backups
Write-Log -Type INF -Content ("Set Delete date of backups to "+$RetainDaysOnDestination)
$myUpdatedRecords=TransferredFilesTable.SetDeleteDateOfFiles -LogInstanceConnectionString $LogInstanceConnectionString -TableName $mySysTransferredFilesTableName -RetainDaysOnDestination $RetainDaysOnDestination -InstanceNameToFilter $mySysSourceInstanceName

#--=======================Finalize Log Modules
Write-Log -Type INF -Content ("BackupTransfer Finished with " + $mySysErrCount.ToString() + " Error count and " + $mySysWrnCount.ToString() + " Warning count.")