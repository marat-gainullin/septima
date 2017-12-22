/**
 * 
 * @author mg
 * @name partial_asterisk_schema
 */
SELECT
    TABLE1.*,
    TABLE2.FiELdB
FROM
    TABLE1,
    TABLE2
WHERE
    TABLE2.FIELDA < TABLE1.F1
    AND
    :P2 = TABLE1.F3
