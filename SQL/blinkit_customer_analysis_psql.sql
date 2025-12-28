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
	registration_date timestamp not null,
	customer_segment text,
	total_orders int,
	avg_order_value float,
	registration_year int,
	registration_month int
);

-- creating blinkit_customer_feedback table.
create table if not exists blinkit_customer_feedback (
	feedback_id bigint not null,
	order_id bigint not null,
	customer_id bigint not null,
	rating int check (rating >= 0 and rating <= 5),
	feedback_text text,
	feedback_category text,
	sentiment text,
	feedback_date timestamp not null,
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


-- date_period (which are weekly period of 7 days) are set as text type. 
alter table blinkit_orders
alter column order_weekly_period type text;

alter table blinkit_customer_feedback
alter column feedback_period type text;

-- fixing phone number (as phone numbers are 12 digits include country_code 91)
alter table blinkit_customer_details
alter column phone type bigint;


-- ************** All dataset's are injected into the database ****************
-- checking data quantity
select count(*) from blinkit_customer_details;
select count(*) from blinkit_customer_feedback;
select count(*) from blinkit_orders;


-- set up table relations, PKs and FKs.

-- Pks.
alter table blinkit_orders
add constraint pk_order_id
primary key (order_id);

alter table blinkit_customer_feedback
add constraint pk_feedback_id
primary key (feedback_id);

alter table blinkit_customer_details
add constraint pk_customer_id
primary key (customer_id);


-- Fks.
alter table blinkit_orders
add constraint fk_orders_customer
foreign key (customer_id)
references blinkit_customer_details (customer_id);

alter table blinkit_customer_feedback
add constraint fk_feedback_customer
foreign key (customer_id)
references blinkit_customer_details (customer_id);

alter table blinkit_customer_feedback
add constraint fk_feedback_order
foreign key (order_id)
references blinkit_orders (order_id);

-- alter table blinkit_customer_feedback
-- add constraint fk_feedback_customers_via_orders
-- foreign key (customer_id)
-- references blinkit_orders (customer_id);

select
	table_name,
	constraint_name,
	column_name
from information_schema.key_column_usage
where table_name in ('blinkit_orders', 'blinkit_customer_details', 'blinkit_customer_feedback')
and constraint_name LIKE '%fk%';

-- checking data quantity, joining anomalies and duplicates.

select count(*) from blinkit_orders;
select count(*) from blinkit_customer_feedback;
select count(*) from blinkit_customer_details;

select
	*
from blinkit_orders as a
left join blinkit_customer_details as b on b.customer_id = a.customer_id
left join blinkit_customer_feedback as c on c.order_id = a.order_id
where b.customer_id is null and c.order_id is null;
-- no null data. This was expected as the datasets had no null values!
-- this also indicates that the the primary and foreign keys are absolutely working as expected and the data is still in good shape.


select
	*
from (select
	order_id,
	row_number() over (partition by order_id order by order_date) as duplicate_rank
from blinkit_orders) as dupl_checker_order
where duplicate_rank >1;
-- no duplicates.


select
	*
from (select
	customer_id,
	row_number() over (partition by customer_id order by registration_date) as duplicate_rank
from blinkit_customer_details) as dupl_checker_cust_details
where duplicate_rank >1;
-- no duplicates.

select
	*
from (select
	customer_id,
	row_number() over (partition by customer_id order by feedback_date) as duplicate_rank
from blinkit_customer_feedback) as dupl_checker_cust_feedback
where duplicate_rank >1; 
-- duplicates are found!

select
	count(*) as rows_count,
	count(distinct customer_id) as original_cust_count
from blinkit_customer_feedback; -- rows -> 5000, distinct_cust_count -> 2172.


-- checking each customer's feedback_id and feedback_date to see if customers have distinctive feedbacks.
select
	*
from (select
	customer_id,
	feedback_id,
	feedback_date,
	feedback_text,
	dense_rank() over (partition by customer_id, feedback_id order by feedback_date) as dupl_cnt
from blinkit_customer_feedback) as dupl_feedback_chk
where dupl_cnt >1;
-- as expected there is no duplicate here. 
-- This indicates that customer have more orders where they have given feedbacks for each orders (3 feedback for 3 distinctive orders).


-- checking customer + order details.
select
	*
from (select
	b.customer_id,
	a.order_id,
	dense_rank() over (partition by b.customer_id, a.order_id order by a.order_date) as ord_dupl_check
from blinkit_orders as a
join blinkit_customer_details as b on b.customer_id = a.customer_id) as s
where ord_dupl_check > 1; -- no duplicates found!


-- ******* Data injection and data validation phase ends here ******* --


-- ******* Data analysis using SQL ******* --
-- In this phase, We will find out the following - 
-- 1. RFM (recency , frequency and monetary of each customer)
	-- subset task: RFM by area wise.
	

-- Basically we will be doing business level analysis.

-- 1. RFM analysis (recency, frequency and monetary analysis of each customer).
-- 2. Cohort user growth.
-- 3. Cohort user retention rate.
-- 3. Delivery time analysis.

-- ******************************************************************************************************* --
-- 1. RFM analysis (recency, frequency and monetary analysis of each customer).
-- getting dataset's last transaction day (order_date from order_table) as an identifier of the last buying day. 

-- now we will get each customer's latest transaction_day, count of their orders and total money they have spent in the app.

with rfm as (
select
	a.customer_id,
	b.customer_name,
	(select max(order_date)::date from blinkit_orders) - max(a.order_date)::date as recency,
	count(a.order_id) as frequency,
	ROUND(sum(a.order_total)::decimal, 2) as monetary
from blinkit_orders as a
join blinkit_customer_details as b on b.customer_id = a.customer_id
group by a.customer_id, b.customer_name
),

-- using CASE WHEN ... ELSE .. END + NTILE() to divide RFM groups into 5 numerical categories.
user_bin as (select
	*,
	case
		WHEN recency <= 7 THEN 5
        WHEN recency <= 30 THEN 4
        WHEN recency <= 90 THEN 3
        WHEN recency <= 180 THEN 2
        ELSE 1
	end as recency_score,
	ntile(5) over (order by frequency desc) as frequency_score,
	ntile(5) over (order by monetary desc) as monetary_score
from rfm)

-- distributing customers.
-- champions (5,5,5 or 5,4,3) , loyal customers (4, 4, 3 or 4,3,3), potential_loyalist (4,3,3 or 3,3,2) ,
-- at risk (3,2,2 or 2,2,2) and churned (1,1,1 or 1, 1, 2)
 
select
	CASE
            when recency_score >= 4 and frequency_score >= 4 and monetary_score >= 3 then 'Champions'
			when recency_score >= 4 and frequency_score >= 3 and monetary_score >= 3 then 'Loyal customer'
			when recency_score >= 3 and frequency_score >= 3 and monetary_score >= 2 then 'Potential loyalist'
			when recency_score >= 2 and frequency_score >= 2 and monetary_score >= 2 then 'At risk'
			when recency_score >= 1 and frequency_score >= 1 and monetary_score >= 1 then 'Churned'
        	ELSE 'Others'
    END AS rfm_segment,
	
	count(customer_id) as customer_count,
	
	-- numerical.
	round(avg(frequency), 2) as avg_buying_frequency,
	round(avg(monetary), 2) as avg_money_spend,

	round(count(customer_id) * 100.0 / (select count(distinct customer_id) from blinkit_customer_details), 2) as contribution_pct
from user_bin
group by rfm_segment;

-- 2. Cohort analysis. 
-- We are using first_transaction_date for every user as the "birth" date in blinkit for the users.
with first_txn_date as (
select
	customer_id,
	min(date_trunc('month', order_date)) as cohort_month
from blinkit_orders
group by 1
),

-- joining with the blinkit_orders with customer_id to get the next transaction date.
cohort_analysis as (
	select
		a.customer_id,
		(extract(year from b.order_date) - extract(year from a.cohort_month)) * 12 
		+ (extract(month from b.order_date) - extract(month from a.cohort_month)) as month_number,
		a.cohort_month
	from first_txn_date as a
	join blinkit_orders as b on b.customer_id = a.customer_id
),

prev_month_cust_cnt as 
(select 
	cohort_month,
	month_number,
	count(distinct customer_id) as active_customers,
	lag(count(distinct customer_id)) over (partition by cohort_month order by month_number) as prev_active_customer_count
from cohort_analysis
group by 1,2)

select
	cohort_month,
	month_number,
	active_customers,
	prev_active_customer_count,

	case
		when prev_active_customer_count is null or prev_active_customer_count = 0 then 0
		else round(((active_customers - prev_active_customer_count) * 100.0 / prev_active_customer_count), 2)
	end as monthly_change_pct
from prev_month_cust_cnt;

-- Cohort User retention rate (Counting from month 0).
-- for a Q-commerce app like blinkit, its a valuable insight to see how many user logged in at first then came back in next months.

with first_txn_date as (
select
	customer_id,
	min(date_trunc('month', order_date)) as cohort_month
from blinkit_orders
group by 1
),

month_number_cte as (
select
	b.cohort_month,
	b.customer_id,
	(extract(year from a.order_date) - extract(year from b.cohort_month)) * 12 +
	(extract(month from a.order_date) - extract(month from b.cohort_month)) as month_number
from blinkit_orders as a
join first_txn_date as b on b.customer_id = a.customer_id 
),

cohort_analysis as (
select
	cohort_month,
	month_number,
	count(distinct customer_id) as active_customer_count
from month_number_cte
group by 1,2
),

-- using first_value() window function to lock in first month's (where month is zero for each cohort month) total user count.
first_month_value as (
select
	cohort_month,
	month_number,
	active_customer_count,
	-- using order by month_number to actually get each cohort month's first month value.
	first_value(active_customer_count) over (partition by cohort_month order by month_number) as cohort_size_locked
from cohort_analysis
)

-- retention rate of each month from cohort month.
select
	cohort_month,
	month_number,
	active_customer_count,
	round((active_customer_count * 100.0 / cohort_size_locked), 2) as user_retention_rate
from first_month_value;


-- 3. Delivery time analysis.
select
	order_hour,
	COUNT(CUSTOMER_ID) AS CUSTOMER_VISIT_COUNT,
	-- getting average delay by every order hour.
	ROUND(avg(gap_between_promised_actual_time)::decimal, 2) as avg_promised_delivery_gap_timing,
	count(case when gap_between_promised_actual_time > 0 then 1 end) as on_time_delivery_count,
	count(case when gap_between_promised_actual_time < 0 then 1 end) as late_delivery_count
from blinkit_orders
group by 1
ORDER BY 1;