-- creating a table named 'blinkit_delivery'

drop table if exists blinkit_delivery;

create table if not exists blinkit_delivery (
	order_id bigint primary key not null,
	delivery_partner_id bigint not null,
	promised_time timestamp,
	actual_time timestamp,
	delivery_time_minutes float,
	distance_km float,
	delivery_status text,
	reasons_if_delayed text,
	delivery_year int,
	delivery_month int,
	delivery_day int,
	delivery_week int,
	-- delivery_week_period int, -- this column is deleted.
	delivery_distance_flag text
);

-- defining relations with other tables keys.
/* as the delivery dataset holds the records of all 5000 orders, we are using order_id as the primary key and creating 
	relations between blinkit_orders and blinkit_order_items tables.

	if the blinkit_delivery dataset only hold the details about delivery agents and their LV analysis, we would move forward
	with delivery_id or any surrogate keys to make that key as a primary key.
*/

alter table blinkit_delivery
add constraint fk_to_blinkit_orders_order_id
foreign key (order_id)
references blinkit_orders (order_id);

/*
	Removing delivery_week_period column as our analysis will only be based on actual delivery timing analysis and actual cause
	of delay.
*/
alter table blinkit_delivery drop column delivery_week_period;


-- select * from blinkit_delivery;

-- ****** Data injection is completed ******

-- Further data quality check.
select
	*
from blinkit_delivery
where distance_km < 0 or distance_km > 5; -- no anomalies.

select
	*
from blinkit_delivery
where delivery_time_minutes < -5; -- no anomalies.


alter table blinkit_delivery
add column distance_flag_dupl text;

update blinkit_delivery
set distance_flag_dupl =
case	when distance_km <= 1 then 'below 1 km'
	when distance_km <= 2 then 'below 2 km'
	when distance_km <= 3 then 'below 3 km'
	when distance_km <= 4 then 'below 4 km'
	when distance_km <= 5 then 'below 5 km'
else 'above 5 km'
end;

alter table blinkit_delivery
rename column distance_flag_dupl TO delivery_distance_category;


select
	delivery_year,
	delivery_distance_flag,
	count(*) as order_cnt,
	round(avg(delivery_time_minutes)::decimal, 2) as avg_delivery_timing
from blinkit_delivery
group by delivery_year, delivery_distance_flag
order by delivery_year;


-- checking year-wise monthly average delivery time.
select
	delivery_year,
	delivery_month,
	round(avg(delivery_time_minutes)::decimal, 2) as avg_delivery_time_in_minutes
from blinkit_delivery
group by delivery_year, delivery_month
order by delivery_year, delivery_month;

-- Getting orders that received the most bad reviews for delivery timing from different customers segments.
-- This way we will get to know which customer group needs more attention to prevent churning and AOV to drop.

select
	cd.customer_segment,
	count(*) as count_of_orders,
	count(case when cf.feedback_category = 'delivery' then 1 end) as delivery_related_reviews,
	count(case when cf.feedback_category = 'delivery' 
	and cf.sentiment = 'negative' then 1 end) as neg_delivery_review,
	
	round((count(case when cf.feedback_category = 'delivery' 
	and cf.sentiment = 'negative' then 1 end) * 100 / count(*))::decimal, 2) as bad_delivery_review_pct
from blinkit_customer_details as cd
join blinkit_customer_feedback as cf on cf.customer_id = cd.customer_id
group by 1;

