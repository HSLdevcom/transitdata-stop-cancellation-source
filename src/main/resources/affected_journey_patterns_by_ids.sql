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
    WHERE Jp.Id IN (VAR_JP_IDS)
    AND (SP.ExistsUptoDate >= 'VAR_DATE_NOW' OR SP.ExistsUptoDate IS NULL)
    AND (JPP.ExistsUptoDate >= 'VAR_DATE_NOW' OR JPP.ExistsUptoDate IS NULL)
    ORDER BY JP.Id, PIJP_SequenceNumber;