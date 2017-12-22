/**
 * 
 * @author mg
 */
SELECT T0.ORDER_NO, 'Some text' txt, TABLE1.ID, TABLE1.F1, TABLE1.F3, T0.AMOUNT
FROM TABLE1, TABLE2, #entities/sub-entity T0
WHERE
TABLE2.FIELDA < TABLE1.F1
AND
:P2 = TABLE1.F3
AND
:P3 = T0.AMOUNT
