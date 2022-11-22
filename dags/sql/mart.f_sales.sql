-- alter tables add status
-- alter table staging.user_order_log add column status varchar(20);
-- alter table staging.user_order_log add constraint unique_id unique (uniq_id);
-- alter table mart.f_sales add column status varchar(20);

-- avoid duplicated data
delete from mart.f_sales where date_id in
(select dc.date_id from staging.user_order_log uol
left join mart.d_calendar as dc on uol.date_time::Date = dc.date_actual
where uol.date_time::Date = '{{ds}}');

-- insert increment
insert into mart.f_sales (date_id, item_id, customer_id, city_id, quantity, payment_amount, status)
select
dc.date_id,
item_id,
customer_id,
city_id,
case when status='refunded' then -1*quantity else quantity end as quantity,
case when status='refunded' then -1*payment_amount else payment_amount end as payment_amount,
case when status is null then 'shipped' else status end as status
from staging.user_order_log uol
left join mart.d_calendar as dc on uol.date_time::Date = dc.date_actual
where uol.date_time::Date = '{{ds}}';