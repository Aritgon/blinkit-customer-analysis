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


select
	delivery_year,
	delivery_distance_flag,
	count(*) as order_cnt,
	round(avg(delivery_time_minutes)::decimal, 2) as avg_delivery_timing
from blinkit_delivery
group by delivery_year, delivery_distance_flag
order by delivery_year;


