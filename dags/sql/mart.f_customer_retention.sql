-- create table 
-- create table mart.f_customer_retention(
-- new_customers_count int,
-- returning_customers_count int,
-- refunded_customer_count int,
-- period_name varchar(10),
-- period_id varchar(100),
-- item_id int,
-- new_customers_revenue numeric(14,2),
-- returning_customers_revenue numeric(14,2),
-- customers_refunded int,
-- constraint unique_increment unique (item_id, period_id)
-- );

-- insert increment
-- added on conflict update
insert into mart.f_customer_retention (
new_customers_count,
returning_customers_count,
refunded_customer_count,
period_name,
period_id,
item_id,
new_customers_revenue,
returning_customers_revenue,
customers_refunded
)
with new_increment as (
select
customer_id,
item_id,
status,
count(*) as cnt,
sum(payment_amount) as revenue
from staging.user_order_log
where date_time between ('{{ds}}'::date-7) and ('{{ds}}'::date-1)
group by customer_id, item_id, status
),
ids as (
select distinct item_id
from new_increment
),
new_customers as (
select
item_id,
count(customer_id) as new_customers_count,
sum(revenue) as new_customers_revenue
from new_increment
where status='shipped' and cnt=1
group by item_id
),
refunded_customers as (
select
item_id,
count(customer_id) as refunded_customer_count,
sum(cnt) as customers_refunded
from new_increment
where status='refunded'
group by item_id
),
returning_customers as (
select
item_id,
count(customer_id) as returning_customers_count,
sum(revenue) as returning_customers_revenue
from new_increment
where status='shipped' and cnt>1
group by item_id
)
select
nc.new_customers_count,
rc.returning_customers_count,
rf.refunded_customer_count,
'weekly' as period_name,
(('{{ds}}'::date-7)::varchar(20) || ' - ' || ('{{ds}}'::date-1)::varchar(20)) as period_id,
ids.item_id,
nc.new_customers_revenue,
rc.returning_customers_revenue,
rf.customers_refunded
from ids
left join new_customers nc using(item_id)
left join refunded_customers rf using(item_id)
left join returning_customers rc using(item_id)
on conflict(item_id, period_id) do update set
(
new_customers_count,
returning_customers_count,
refunded_customer_count,
period_name,
new_customers_revenue,
returning_customers_revenue,
customers_refunded
)=(
excluded.new_customers_count,
excluded.returning_customers_count,
excluded.refunded_customer_count,
excluded.period_name,
excluded.new_customers_revenue,
excluded.returning_customers_revenue,
excluded.customers_refunded
);