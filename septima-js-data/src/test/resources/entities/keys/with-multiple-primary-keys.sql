/**
 * 
 * @name multiple_primary_keys
 */
SELECT
    T_MTD_MDCHNGLOG_1.*,
    T_MTD_ENTITIES.MDENT_ID
FROM
    PUBLIC.MTD_MDCHNGLOG T_MTD_MDCHNGLOG_1,
    MTD_ENTITIES T_MTD_ENTITIES
