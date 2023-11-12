--Backup Transfer Events
DECLARE @myStartDate DATETIME
SET @myStartDate = DATEADD(DAY,-1,CAST(CAST(GETDATE() AS DATE) AS DATETIME))
SELECT * FROM [EventLog].[dbo].[TransferredFiles] AS myBackupLog WITH (READPAST) WHERE [myBackupLog].[TransferStatus] != 'SUCCEED' AND [myBackupLog].[EventTimeStamp]>=@myStartDate ORDER BY [EventTimeStamp] Desc
SELECT * FROM [EventLog].[dbo].[Events] AS myEventLog WITH (READPAST) WHERE [myEventLog].Serverity='ERR' AND [myEventLog].[EventTimeStamp] >= @myStartDate ORDER BY [EventTimeStamp] DESC
SELECT InstanceName,Count(CASE WHEN [myBackupLog].[TransferStatus] = 'SUCCEED' THEN 1 END) AS SucceedCount,Count(CASE WHEN [myBackupLog].[TransferStatus] != 'SUCCEED' THEN 1 END) AS NoneCount FROM [EventLog].[dbo].[TransferredFiles] AS myBackupLog WITH (READPAST) WHERE [myBackupLog].[EventTimeStamp]>=@myStartDate Group By InstanceName ORDER BY NoneCount Desc, InstanceName