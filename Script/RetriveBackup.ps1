[CmdletBinding()]
#--------------------------------------------------------------Parameters
Param(
    [Parameter(Mandatory=$true)][string]$SourceInstance,
    [Parameter(Mandatory=$true)][string][ValidateSet("All_Databases","User_Databases","System_Databases","Custom_List", IgnoreCase=$true)]$SourceDatabaseRetrivalMethod,
    [Parameter(Mandatory=$false)][string]$SourceDatabaseName,
    [Parameter(Mandatory=$true)][string][ValidateSet("SCP","UNC", IgnoreCase=$true)]$SourceRepoType,
    [Parameter(Mandatory=$true)][string]$SourceRepPath,
    [Parameter(Mandatory=$false)][string]$SourceRepoUser,
    [Parameter(Mandatory=$false)][securestring]$SourceRepoPassword,
    [Parameter(Mandatory=$true)][string]$DestinationServer,
    [Parameter(Mandatory=$true)][string]$DestinationInstance,
    [Parameter(Mandatory=$false)][string]$DestinationInstanceLogin,
    [Parameter(Mandatory=$false)][securestring]$DestinationInstancePassword,
    [Parameter(Mandatory=$true)][string]$DestinationDatabaseName,
    [Parameter(Mandatory=$true)][string][ValidateSet("SCP","UNC", IgnoreCase=$true)]$DestinationRepoType,
    [Parameter(Mandatory=$true)][string]$DestinationRepoPath,
    [Parameter(Mandatory=$false)][string]$DestinationRepoUser,
    [Parameter(Mandatory=$false)][securestring]$DestinationRepoPassword,
    [Parameter(Mandatory=$true)][string][ValidateSet("FixDateTime","Delayed","DelayedRandomRange", IgnoreCase=$true)]$RecoveryMethod,
    [Parameter(Mandatory=$false)][datetime]$FixDateTime,
    [Parameter(Mandatory=$false)][int]$Delayed,
    [Parameter(Mandatory=$false)][int]$DelayedRandomRangeLowerBound,
    [Parameter(Mandatory=$false)][int]$DelayedRandomRangeUpperBound,
    [Switch]$AllowDatabaseReplacementOnDestination,
    [Switch]$AllowDatabaseRecoveryOnDestination,
    [Switch]$SetDatabaseToReadonlyModeOnDestination,
    [Switch]$HealthCheck,
    [Switch]$DeleteDatabaseOnDestination,
    [Parameter(Mandatory=$true)][string]$LogInstance,
    [Parameter(Mandatory=$false)][string]$LogInstanceLogin,
    [Parameter(Mandatory=$false)][securestring]$LogInstancePassword,
    [Parameter(Mandatory=$true)][string]$LogDatabaseName,
    [Parameter(Mandatory=$true)][string]$LogFilePath,
    [Parameter(Mandatory=$true)][string][ValidateSet("MINIMUM","NORMAL","VERBOSE", IgnoreCase=$true)]$LogType
)

Write-Host $SourceDatabaseRetrivalMethod