SELECT
  SP.Gid AS SP_Gid,
  SP.Name AS SP_Name,
  JPP.Number AS JPP_Number
  FROM ptDOI4.dbo.doi4_StopPoint AS SP
  JOIN ptDOI4.dbo.doi4_JourneyPatternPoint AS JPP ON JPP.Gid = SP.IsJourneyPatternPointGid
  WHERE (JPP.ExistsUptoDate >= 'VAR_DATE_NOW' OR JPP.ExistsUptoDate IS NULL) AND (SP.ExistsUptoDate >= 'VAR_DATE_NOW' OR SP.ExistsUptoDate IS NULL);