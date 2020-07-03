SELECT DISTINCT AD.deviation_case_id,
	ASTOPS.stop_id AS AFFECTED_STOPS_GID,
	CONVERT(CHAR(16), DVJ.Id) AS DVJ_Id,
	KVV.StringValue AS ROUTE_NAME,
	SUBSTRING(CONVERT(CHAR(16), VJT.IsWorkedOnDirectionOfLineGid), 12, 1) AS DIRECTION,
	CONVERT(CHAR(8), DVJ.OperatingDayDate, 112) AS OPERATING_DAY,
	RIGHT('0' + (CONVERT(VARCHAR(2), (DATEDIFF(HOUR, '1900-01-01', PlannedStartOffsetDateTime)))), 2)
		+ ':' + RIGHT('0' + CONVERT(VARCHAR(2), ((DATEDIFF(MINUTE, '1900-01-01', PlannedStartOffsetDateTime))
		- ((DATEDIFF(HOUR, '1900-01-01', PlannedStartOffsetDateTime) * 60)))), 2) + ':00' AS START_TIME,
	JP.Id AS JP_Id
	FROM omm_db.dbo.affected_departures AS AD
	LEFT JOIN omm_db.dbo.affected_stops AS ASTOPS ON ASTOPS.affected_departures_id = AD.affected_departures_id
	LEFT JOIN omm_db.dbo.deviation_cases AS DC ON DC.deviation_case_id = AD.deviation_case_id
	INNER JOIN ptDOI4.dbo.DatedVehicleJourney AS DVJ ON DVJ.Id = AD.departure_id
	LEFT JOIN ptDOI4.dbo.TimedJourneyPattern AS TJP ON TJP.Id = DVJ.UsesTimedJourneyPatternId
	LEFT JOIN ptDOI4.dbo.JourneyPattern AS JP ON JP.Id = TJP.IsBasedOnJourneyPatternId
	LEFT JOIN ptDOI4.dbo.VehicleJourney AS VJ ON (DVJ.IsBasedOnVehicleJourneyId = VJ.Id)
	LEFT JOIN ptDOI4.dbo.VehicleJourneyTemplate AS VJT ON (DVJ.IsBasedOnVehicleJourneyTemplateId = VJT.Id)
	LEFT JOIN ptDOI4.dbo.T_KeyVariantValue AS KVV ON (KVV.IsForObjectId = VJ.Id)
	LEFT JOIN ptDOI4.dbo.KeyVariantType AS KVT ON (KVT.Id = KVV.IsOfKeyVariantTypeId)
	LEFT JOIN ptDOI4.dbo.KeyType AS KT ON (KT.Id = KVT.IsForKeyTypeId)
	LEFT JOIN ptDOI4.dbo.ObjectType AS OT ON (KT.ExtendsObjectTypeNumber = OT.Number)
	WHERE (KT.Name = 'JoreIdentity' OR KT.Name = 'JoreRouteIdentity' OR KT.Name = 'RouteName')
	AND OT.Name = 'VehicleJourney'
	AND VJT.IsWorkedOnDirectionOfLineGid IS NOT NULL
    /* AND DVJ.OperatingDayDate >= 'VAR_FROM_DATE' TODO */
	/* AND DVJ.OperatingDayDate <= 'VAR_TO_DATE' TODO */
	AND DVJ.IsReplacedById IS NULL
	AND ASTOPS.affected_stops_id IS NOT NULL;
