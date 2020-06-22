SELECT DISTINCT CONVERT(CHAR(16), DVJ.Id) AS DVJ_Id,
	KVV.StringValue AS ROUTE_NAME,
 	SUBSTRING(CONVERT(CHAR(16), VJT.IsWorkedOnDirectionOfLineGid), 12, 1) AS DIRECTION,
	CONVERT(CHAR(8), DVJ.OperatingDayDate, 112) AS OPERATING_DAY,
	RIGHT('0' + (CONVERT(VARCHAR(2), (DATEDIFF(HOUR, '1900-01-01', PlannedStartOffsetDateTime)))), 2)
		+ ':' + RIGHT('0' + CONVERT(VARCHAR(2), ((DATEDIFF(MINUTE, '1900-01-01', PlannedStartOffsetDateTime))
		- ((DATEDIFF(HOUR, '1900-01-01', PlannedStartOffsetDateTime) * 60)))), 2) + ':00' AS START_TIME,
	JP.Id AS JP_Id
    FROM VAR_OMM_DATABASE_NAME.dbo.DatedVehicleJourney AS DVJ
    LEFT JOIN VAR_OMM_DATABASE_NAME.dbo.TimedJourneyPattern AS TJP ON TJP.Id = DVJ.UsesTimedJourneyPatternId
    LEFT JOIN VAR_OMM_DATABASE_NAME.dbo.JourneyPattern AS JP ON JP.Id = TJP.IsBasedOnJourneyPatternId
    LEFT JOIN VAR_OMM_DATABASE_NAME.dbo.VehicleJourney AS VJ ON (DVJ.IsBasedOnVehicleJourneyId = VJ.Id)
    LEFT JOIN VAR_OMM_DATABASE_NAME.dbo.VehicleJourneyTemplate AS VJT ON (DVJ.IsBasedOnVehicleJourneyTemplateId = VJT.Id)
    LEFT JOIN VAR_OMM_DATABASE_NAME.T.KeyVariantValue AS KVV ON (KVV.IsForObjectId = VJ.Id)
    LEFT JOIN VAR_OMM_DATABASE_NAME.dbo.KeyVariantType AS KVT ON (KVT.Id = KVV.IsOfKeyVariantTypeId)
    LEFT JOIN VAR_OMM_DATABASE_NAME.dbo.KeyType AS KT ON (KT.Id = KVT.IsForKeyTypeId)
    LEFT JOIN VAR_OMM_DATABASE_NAME.dbo.ObjectType AS OT ON (KT.ExtendsObjectTypeNumber = OT.Number)
    WHERE (KT.Name = 'JoreIdentity' OR KT.Name = 'JoreRouteIdentity' OR KT.Name = 'RouteName' )
    AND OT.Name = 'VehicleJourney'
    AND VJT.IsWorkedOnDirectionOfLineGid IS NOT NULL
    AND DVJ.OperatingDayDate >= 'VAR_FROM_DATE'
    AND DVJ.OperatingDayDate <= 'VAR_TO_DATE'
    AND DVJ.IsReplacedById IS NULL
    AND JP.Id IN (VAR_AFFECTED_JP_IDS);