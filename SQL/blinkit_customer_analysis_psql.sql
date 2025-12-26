-- SELECT *
-- FROM information_schema.tables
-- WHERE table_type = 'BASE TABLE'
-- AND table_schema NOT IN ('pg_catalog', 'information_schema');

-- drop table if exists blinkit_cust_details;
-- drop table if exists blinkit_customer_feedback;
-- drop table if exists blinkit_orders;

-- creating blinkit_orders table.
create table if not exists blinkit_orders (
	order_id bigint not null,
	customer_id bigint not null,
	order_date timestamp not null,
	promised_delivery_time timestamp not null,
	actual_delivery_time timestamp not null,
	order_total float,
	payment_method text,
	delivery_partner_id int,
	store_id int,
	delivery_gap_in_minutes float,
	gap_between_promised_actual_time float,
	delivery_gap_flag text,
	order_year int,
	order_month int,
	order_week int,
	order_day text,
	order_weekly_period int,
	order_hour int
);

-- creating blinkit_customer_details table.
create table if not exists blinkit_customer_details (
	customer_id bigint not null,
	customer_name text not null,
	email text,
	phone int,
	address text,
	area text,
	pincode int,
	registration_date timestamp,
	customer_segment text,
	total_orders int,
	avg_order_value float,
	registration_year int,
	registration_month int
);

-- creating blinkit_customer_feedback table.
create table if not exists blinkit_customer_feedback (
	feedback_id bigint,
	order_id bigint,
	customer_id bigint,
	rating int check (rating >= 0 and rating <= 5),
	feedback_text text,
	feedback_category text,
	sentiment text,
	feedback_date timestamp,
	feedback_year int,
	feedback_month int,
	feedback_day_name text,
	feedback_week int,
	feedback_period text,
	feedback_day int
);

-- checking tables.
select * from blinkit_customer_details;
select * from blinkit_orders;
select * from blinkit_customer_feedback;


-- fixing data columns which is in date_period scenario.
alter table blinkit_orders
alter column order_weekly_period type text;

alter table blinkit_customer_feedback
alter column feedback_period type text;

-- fixing phone number (as phone numbers are 12 digits include country_code 91)
alter table blinkit_customer_details
alter column phone type bigint;


-- ************** All dataset's are injected into the database ****************
-- checking data quality
select count(*) from blinkit_customer_details;
select count(*) from blinkit_customer_feedback;
select count(*) from blinkit_orders;


-- set up table relations, PKs and FKs.