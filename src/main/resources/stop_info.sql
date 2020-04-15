SELECT
  SP.Gid AS STOP_POINT_GID,
  SP.Name AS STOP_POINT_NAME,
  JPP.Number AS JOURNEY_PATTERN_POINT_NUMBER
  FROM [ptDOI4_Community].[dbo].[StopPoint] AS SP
  JOIN [ptDOI4_Community].[dbo].[JourneyPatternPoint] AS JPP ON JPP.Gid = SP.IsJourneyPatternPointGid
  WHERE (JPP.ExistsUptoDate >= ? OR JPP.ExistsUptoDate IS NULL) AND (SP.ExistsUptoDate >= ? OR SP.ExistsUptoDate IS NULL);