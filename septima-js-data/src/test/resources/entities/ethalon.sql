Select
    o.order_id id, o.amount amt, o.good goodik, o.customer orderer, o.field1 other
From
    goodOrder o Inner join customOrders co on(o.id = co.order_id)
