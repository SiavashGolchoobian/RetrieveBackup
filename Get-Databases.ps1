#--------------------------------------------------------------Parameters
Param(
    [Parameter(Mandatory=$true)][string]$Instance,
    [Parameter(Mandatory=$true)][string][ValidateSet("All_Databases","User_Databases","System_Databases", IgnoreCase=$true)]$RetrivalMethod,
    [Parameter(Mandatory=$false)][string]$Login,
    [Parameter(Mandatory=$false)][securestring]$Password,
    [Parameter(Mandatory=$true)][string]$LogFilePath,
    [Parameter(Mandatory=$true)][string][ValidateSet("MINIMUM","NORMAL","VERBOSE", IgnoreCase=$true)]$LogType,
    [Switch]$UseWindowsAuth,
    [Switch]$EncryptConnection,
    [Switch]$RetriveFromMSX
)

function Private:Execute-Query {
    [OutputType([System.Data.DataTable])]
    Param(
    [Parameter(Mandatory=$true)][string]$Instance,
    [Parameter(Mandatory=$true)][string]$Database,
    [Parameter(Mandatory=$false)][string]$Login,
    [Parameter(Mandatory=$false)][securestring]$Password,
    [Switch]$UseWindowsAuth,
    [Switch]$EncryptConnection,
    [Parameter(Mandatory=$true)][string]$Query
    )

    $myBitUseWindowsAuth = if ($UseWindowsAuth){1}else{0}
    $myBitEncryptConnection = if ($EncryptConnection){1}else{0}
    $myBitSum = ([Math]::Pow(2,0)*$myBitUseWindowsAuth) + ([Math]::Pow(2,1)*$myBitEncryptConnection)

    try {
        $myResult = switch ($myBitSum) {
            0 { Invoke-Sqlcmd -ServerInstance $Instance -Database $Database -Query $Query -Username $Login -Password $Password -OutputAs DataTables -ErrorAction Stop}
            1 { Invoke-Sqlcmd -ServerInstance $Instance -Database $Database -Query $Query -OutputAs DataTables -ErrorAction Stop}
            2 { Invoke-Sqlcmd -ServerInstance $Instance -Database $Database -Query $Query -Username $Login -Password $Password -EncryptConnection -OutputAs DataTables -ErrorAction Stop}
            3 { Invoke-Sqlcmd -ServerInstance $Instance -Database $Database -Query $Query -EncryptConnection -OutputAs DataTables -ErrorAction Stop}
            Default { Invoke-Sqlcmd -ServerInstance $Instance -Database $Database -Query $Query -OutputAs DataTables -ErrorAction Stop}
        }   
    }
    catch {
        Write-Host $_.ToString()
    }

    return $myResult
}
function Private:Get-ServerListQuery {  #Retrive Instance names from MSX
    [OutputType([string])]
    
    $myQuery="
    SELECT DISTINCT
        [myServer].[server_name] AS [InstanceName]
    FROM
        [msdb].[dbo].[sysmanagement_shared_registered_servers_internal] AS [myServer]
    "
    return $myQuery
}

function Private:Get-DatabasesQuery {
    [OutputType([string])]
    Param([Switch]$RetriveFromMSX)
}

Exec-Query -Instance $Instance -Database "msdb" -UseWindowsAuth -EncryptConnection -Query "Select getdate() AS myTime"