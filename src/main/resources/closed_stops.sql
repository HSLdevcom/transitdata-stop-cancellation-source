SELECT SD.stop_deviations_id, SD.status, SD.valid_from AS SD_VALID_FROM, SD.valid_to AS SD_VALID_TO,
       SC.stop_status, SC.stop_id AS SC_STOP_ID,
       B.valid_from AS B_VALID_FROM, B.valid_to AS B_VALID_TO, B.affected_stop_ids, B.affected_route_ids, B.status, B.type, B.category, B.bulletin_template, B.influence_level, B.urgency_level, B.recipient_route_ids,
       BLM.description AS B_DESCRIPTION, DC.deviation_case_id, DC.status, DC.type
    FROM VAR_OMM_DATABASE_NAME.dbo.stop_deviations AS SD
    LEFT JOIN VAR_OMM_DATABASE_NAME.dbo.stop_controls AS SC ON SD.stop_deviations_id = SC.stop_deviations_id
    LEFT JOIN VAR_OMM_DATABASE_NAME.dbo.bulletins B ON SD.bulletin_id = B.bulletins_id
    LEFT JOIN VAR_OMM_DATABASE_NAME.dbo.bulletin_localized_messages AS BLM ON B.bulletins_id = BLM.bulletins_id
    LEFT JOIN VAR_OMM_DATABASE_NAME.dbo.deviation_cases AS DC ON B.bulletins_id = DC.bulletin_id
    WHERE SC.stop_status = 'CLOSED' AND SD.valid_to >= ?
    ORDER BY B.last_modified desc;