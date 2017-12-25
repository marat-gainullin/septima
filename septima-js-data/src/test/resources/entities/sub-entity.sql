select goodorder.order_id as order_no, goodorder.amount, customers.customer_name as customer
from goodorder goodorder
 inner join customer customers on (goodorder.customer = customers.customer_id)
 and (goodorder.amount > customers.customer_name)
 where :param1 = goodorder.good