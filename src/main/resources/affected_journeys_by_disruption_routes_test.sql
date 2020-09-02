SELECT DISTINCT CONVERT(CHAR(16), DVJ.Id) AS DVJ_Id,
	KVV.StringValue AS ROUTE_NAME,
 	SUBSTRING(CONVERT(CHAR(16), VJT.IsWorkedOnDirectionOfLineGid), 12, 1) AS DIRECTION,
	CONVERT(CHAR(8), DVJ.OperatingDayDate, 112) AS OPERATING_DAY,
	RIGHT('0' + (CONVERT(VARCHAR(2), (DATEDIFF(HOUR, '1900-01-01', PlannedStartOffsetDateTime)))), 2)
		+ ':' + RIGHT('0' + CONVERT(VARCHAR(2), ((DATEDIFF(MINUTE, '1900-01-01', PlannedStartOffsetDateTime))
		- ((DATEDIFF(HOUR, '1900-01-01', PlannedStartOffsetDateTime) * 60)))), 2) + ':00' AS START_TIME,
	DVJ.OperatingDayDate + VJ.PlannedStartOffsetDateTime AS START_DATE_TIME,
	JP.Id AS JP_Id
    FROM ptDOI4.dbo.doi4_DatedVehicleJourney AS DVJ
    LEFT JOIN ptDOI4.dbo.doi4_TimedJourneyPattern AS TJP ON TJP.Id = DVJ.UsesTimedJourneyPatternId
    LEFT JOIN ptDOI4.dbo.doi4_JourneyPattern AS JP ON JP.Id = TJP.IsBasedOnJourneyPatternId
    LEFT JOIN ptDOI4.dbo.doi4_VehicleJourney AS VJ ON (DVJ.IsBasedOnVehicleJourneyId = VJ.Id)
    LEFT JOIN ptDOI4.dbo.doi4_VehicleJourneyTemplate AS VJT ON (DVJ.IsBasedOnVehicleJourneyTemplateId = VJT.Id)
    LEFT JOIN ptDOI4.dbo.doi4_T_KeyVariantValue AS KVV ON (KVV.IsForObjectId = VJ.Id)
    LEFT JOIN ptDOI4.dbo.doi4_KeyVariantType AS KVT ON (KVT.Id = KVV.IsOfKeyVariantTypeId)
    LEFT JOIN ptDOI4.dbo.doi4_KeyType AS KT ON (KT.Id = KVT.IsForKeyTypeId)
    LEFT JOIN ptDOI4.dbo.doi4_ObjectType AS OT ON (KT.ExtendsObjectTypeNumber = OT.Number)
    JOIN ptDOI4.dbo.doi4_DirectionOfLine AS DL ON (DL.Gid = VJT.IsWorkedOnDirectionOfLineGid)
    JOIN ptDOI4.dbo.doi4_Line AS L ON (L.Id = DL.IsOnLineId)
    WHERE (KT.Name = 'JoreIdentity' OR KT.Name = 'JoreRouteIdentity' OR KT.Name = 'RouteName')
    AND L.Gid IN (VAR_AFFECTED_ROUTE_IDS)
    AND OT.Name = 'VehicleJourney'
    AND VJT.IsWorkedOnDirectionOfLineGid IS NOT NULL
    AND DVJ.OperatingDayDate >= 'VAR_DATE_FROM'
    AND DVJ.OperatingDayDate <= 'VAR_DATE_TO'
    AND (DVJ.OperatingDayDate + VJ.PlannedStartOffsetDateTime) >= 'VAR_MIN_DEP_TIME'
    AND (DVJ.OperatingDayDate + VJ.PlannedStartOffsetDateTime) <= 'VAR_MAX_DEP_TIME'
    AND DVJ.IsReplacedById IS NULL;