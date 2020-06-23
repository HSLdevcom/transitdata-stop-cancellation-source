SELECT JP.Id AS JP_Id,
    JP.PointCount AS JP_PointCount,
    PIJP.Id AS PJP_Id,
    PIJP.IsInJourneyPatternId AS PIJP_IsInJourneyPatternId,
    PIJP.IsJourneyPatternPointGid AS PIJP_IsJourneyPatternPointGid,
    PIJP.SequenceNumber AS PIJP_SequenceNumber,
    SP.Name AS SP_Name,
    SP.Gid AS SP_Gid,
    JPP.Number AS JPP_Number
    FROM VAR_DOI_DATABASE_NAME.dbo.JourneyPattern AS JP
    LEFT JOIN VAR_DOI_DATABASE_NAME.dbo.doi4_PointInJourneyPattern AS PIJP ON PIJP.IsInJourneyPatternId = JP.Id
    LEFT JOIN VAR_DOI_DATABASE_NAME.dbo.StopPoint AS SP on SP.IsJourneyPatternPointGid = PIJP.IsJourneyPatternPointGid
    LEFT JOIN VAR_DOI_DATABASE_NAME.dbo.JourneyPatternPoint AS JPP ON JPP.Gid = SP.IsJourneyPatternPointGid
    INNER JOIN (
        SELECT JP.Id
            FROM VAR_DOI_DATABASE_NAME.dbo.JourneyPattern AS JP
            LEFT JOIN VAR_DOI_DATABASE_NAME.dbo.PointInJourneyPattern as PIJP ON PIJP.IsInJourneyPatternId = JP.Id
            LEFT JOIN VAR_DOI_DATABASE_NAME.dbo.JourneyPatternPoint AS JPP ON JPP.Gid = PIJP.IsJourneyPatternPointGid
            LEFT JOIN VAR_DOI_DATABASE_NAME.dbo.StopPoint AS SP ON SP.IsJourneyPatternPointGid = JPP.Gid
            INNER JOIN (
                SELECT DISTINCT(JP.Id)
                    FROM VAR_DOI_DATABASE_NAME.dbo.DatedVehicleJourney AS DVJ
                    LEFT JOIN VAR_DOI_DATABASE_NAME.dbo.TimedJourneyPattern AS TJP ON TJP.Id = DVJ.UsesTimedJourneyPatternId
                    LEFT JOIN VAR_DOI_DATABASE_NAME.dbo.JourneyPattern AS JP ON JP.Id = TJP.IsBasedOnJourneyPatternId
                    LEFT JOIN VAR_DOI_DATABASE_NAME.dbo.VehicleJourney AS VJ ON (DVJ.IsBasedOnVehicleJourneyId = VJ.Id)
                    LEFT JOIN VAR_DOI_DATABASE_NAME.dbo.VehicleJourneyTemplate AS VJT ON (DVJ.IsBasedOnVehicleJourneyTemplateId = VJT.Id)
                    LEFT JOIN VAR_DOI_DATABASE_NAME.T.KeyVariantValue AS KVV ON (KVV.IsForObjectId = VJ.Id)
                    LEFT JOIN VAR_DOI_DATABASE_NAME.dbo.KeyVariantType AS KVT ON (KVT.Id = KVV.IsOfKeyVariantTypeId)
                    LEFT JOIN VAR_DOI_DATABASE_NAME.dbo.KeyType AS KT ON (KT.Id = KVT.IsForKeyTypeId)
                    LEFT JOIN VAR_DOI_DATABASE_NAME.dbo.ObjectType AS OT ON (KT.ExtendsObjectTypeNumber = OT.Number)
                    WHERE (KT.Name = 'JoreIdentity' OR KT.Name = 'JoreRouteIdentity' OR KT.Name = 'RouteName' )
                    AND OT.Name = 'VehicleJourney'
                    AND VJT.IsWorkedOnDirectionOfLineGid IS NOT NULL
                    AND DVJ.OperatingDayDate >= 'VAR_DATE_NOW'
                    AND DVJ.OperatingDayDate <= 'VAR_TO_DATE'
                    AND DVJ.IsReplacedById IS NULL
            ) ACTIVE_JPS ON ACTIVE_JPS.Id = JP.Id
            WHERE (JPP.ExistsUptoDate >= 'VAR_DATE_NOW' OR JPP.ExistsUptoDate IS NULL)
            AND (SP.ExistsUptoDate >= 'VAR_DATE_NOW' OR SP.ExistsUptoDate IS NULL)
            AND SP.Gid IN (VAR_AFFECTED_STOP_GIDS)
    ) AFFECTED_ACTIVE_JPS ON AFFECTED_ACTIVE_JPS.Id = JP.Id
    WHERE (SP.ExistsUptoDate >= 'VAR_DATE_NOW' OR SP.ExistsUptoDate IS NULL)
    AND (JPP.ExistsUptoDate >= 'VAR_DATE_NOW' OR JPP.ExistsUptoDate IS NULL)
    ORDER BY JP.Id, PIJP_SequenceNumber;