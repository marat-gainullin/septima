Select
    ord.id as id,
    ord.customer_id as customer_id,
    ord.seller_id as seller_id,
    ord.good_id as good_id,
    ord.comment as comment,
    ord.moment as moment,
    ord.paid as paid,
    ord.summ as summ,
    ord.destination as destination,
    ord.bad_data as bad_data
from GoodOrder ord