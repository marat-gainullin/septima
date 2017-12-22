/**
 * Columns here are specified in different manner:
 * - with direct table reference and
 * - with using of table's alias
 */
SELECT GOODORDER.ORDER_ID id, T1.AMOUNT amt, CUSTOMER.CUSTOMER_NAME customer
FROM
    GOODORDER T1 INNER JOIN CUSTOMER ON (GOODORDER.CUSTOMER = CUSTOMER.CUSTOMER_ID)
WHERE
    AMOUNT = :p1 AND (GOODORDER.AMOUNT > CUSTOMER.CUSTOMER_NAME)
