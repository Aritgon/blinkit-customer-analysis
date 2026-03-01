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

no null values!
-- this also indicates that the thselect
	*
from blinkit_orders as a
left join blinkit_customer_details as b on b.customer_id = a.customer_id
left join blinkit_customer_feedback as c on c.order_id = a.order_id
where b.customer_id is null and c.order_id is null;
-- no null data. This was expected as the datasets had e primary and foreign keys are absolutely working as expected and the data is still in good shape.


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


-- altering values to full lowercase in blinkit_customer_feedback table.
update blinkit_customer_feedback
set feedback_category = lower(feedback_category);

update blinkit_customer_feedback
set sentiment = lower(sentiment);


-- ******* Data injection and data validation phase ends here ******* --


-- ******* Data analysis using SQL ******* --
-- In this phase, We will find out the following -

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
 
select
	CASE
         when recency_score >= 4 and frequency_score >= 1 and monetary_score >= 1 then 'Champions'
		 when recency_score >= 4 and frequency_score >= 2 and monetary_score >= 2 then 'Loyal customer'
		 when recency_score >= 3 and frequency_score >= 3 and monetary_score >= 3 then 'Potential loyalist'
		 when recency_score >= 2 and frequency_score >= 1 and monetary_score >= 2 then 'At risk'
		 when recency_score >= 1 and frequency_score >= 1 and monetary_score >= 1 then 'Churned'
         ELSE 'Others'
    END AS rfm_segment,
	
	count(customer_id) as customer_count,
	
	-- numerical.
	round(avg(frequency), 2) as avg_buying_frequency,
	round(avg(monetary), 2) as avg_money_spend,

	round(count(customer_id) * 100.0 / (select count(distinct customer_id) from blinkit_customer_details), 2) as cust_contribution_pct
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
	end as monthly_change_pct,

	case
		when prev_active_customer_count < active_customers then 'retention improved'
	end as retention_improve_cycle
	
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
	cohort_size_locked,
	round((active_customer_count * 100.0 / cohort_size_locked), 2) as user_retention_rate
from first_month_value;

-- MAU analysis.
with cte as
(select
	date_trunc('month', a.order_date) as order_month,
	count(a.order_id) as order_cnt,

	-- stats.
	round(avg(a.order_total)::decimal, 2) as aov,
	count(case when b.sentiment = 'Positive' then 1 end) as positive_fdbk_cnt,
	count(b.feedback_id) filter (where b.sentiment != 'Positive') as neutral_and_neg_fdbk_cnt
	
from blinkit_orders as a
join blinkit_customer_feedback as b on b.order_id = a.order_id
group by 1
)

select
	order_month,
	order_cnt,
	lag(order_cnt) over (order by order_month) prev_order_cnt,
	round((order_cnt - lag(order_cnt) over (order by order_month)) * 100.0 / lag(order_cnt) over (order by order_month), 2) as mom_order_cnt_growth,
	
	aov,
	lag(aov) over (order by order_month) prev_aov_cnt,
	round((aov - lag(aov) over (order by order_month)) * 100.0 / lag(aov) over (order by order_month), 2) as mom_avg_aov_growth
from cte;


-- 3. Delivery time analysis per hour in a day.
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

-- penalty analysis. (do late deliveries affect customer's ordering behavior?)
with customer_order as
(select
	customer_id,
	order_id,
	order_date,
	case
		when gap_between_promised_actual_time < 0 then 'late delivery'
		else 'on time delivery'
	end as delivery_pattern
from blinkit_orders
order by 1),

next_customer_order as (select
	customer_id,
	order_date,
	delivery_pattern,
	lead(order_date) over (partition by customer_id order by order_date) as next_order_date
from customer_order)

select
	delivery_pattern,
	count(*) as sample_size,
	round(avg(next_order_date::date - order_date::date), 2) as avg_delivery_delay,
	percentile_cont(0.5) within group (order by (next_order_date::date - order_date::date)) as median_days_to_next_order
from next_customer_order
where next_order_date IS NOT NULL
group by delivery_pattern;

-- customer churn analysis (pct of customers who churn per delivery category.)
select
	delivery_pattern,
	count(*) as total_order_count,
	round(count(case when next_order_date is null then 1 end) * 100.0 / count(*), 2) as churn_rate_pct
from next_customer_order
group by delivery_pattern;

-- binning late delivery time and counting how much revenue has generated by each late delivery type?
-- this would help us to determine where we have to be more time efficient in our delivery.
-- (powerBI dashboard)

select
	delivery_gap_flag,
	count(*) as total_order_cnt,
	round(sum(order_total)::decimal, 2) avg_total_value,
	round(avg(delivery_gap_in_minutes)::decimal, 2) as avg_delivery_gap
from blinkit_orders
where delivery_gap_in_minutes != 0
group by 1 order by 3 desc;

-- correlation between customer late delivery and customer ordering behaviors.
With cust_order_cte as 
(select
	customer_id,
	round(count(case when gap_between_promised_actual_time < 0 then 1 end) * 100.0 / count(*)::decimal, 2) as customer_late_rate,
	round(avg(order_total)::decimal, 2) as avg_spent_per_order,
	round(sum(order_total)::decimal, 2) as total_order_value,
	count(*) as order_cnt
from blinkit_orders
group by 1
having count(*) > 1) -- filtering single orders as they may skew the 100% late delivery rate.

select
	case
		when customer_late_rate < 25.0 then '25% late deliveries'
		when customer_late_rate between 25.0 and 50.0 then '50% late deliveries'
		when customer_late_rate between 50.0 and 75.0 then '75% late deliveries'
		else 'all late deliveries'
	end as delivery_pattern,
	count(customer_id) as customer_count,
	round(avg(avg_spent_per_order), 2) as avg_order_value,
	round(sum(total_order_value), 2) as total_order_value
from cust_order_cte
group by delivery_pattern;


-- revenue leakage per customer's delivery delay order. 
with order_comparison as (
select
	customer_id,
	order_total,
	lead(order_total) over (partition by customer_id order by order_date) as next_order_total
from blinkit_orders
),

avg_rev_leak as 
(select
	*,
	-- calculating revenue leakage.
	round(avg(next_order_total::decimal - order_total::decimal), 2) as avg_revenue_leakage
from order_comparison
where next_order_total is not null
group by 1,2,3)

-- next step try counting customer pct for each month to see how many customers usually order lesser amount in the next order.

select
	case
		when avg_revenue_leakage < 0 then 'degraded order value'
		when avg_revenue_leakage > 0 then 'increased order value'
		else 'others'
	end as penalty_order_analysis,
	count(customer_id) as cust_count,
	round(count(distinct customer_id) * 100.0 / (select count(distinct customer_id) from blinkit_orders), 2) as customer_share_pct
from avg_rev_leak
group by 1;


-- does delivery delay effects users to purchase lower volume or products?
WITH cte as 
(select
	order_id,
	order_date,
	customer_id,
	order_total as current_order_total,
	lead(order_total) over (partition by customer_id order by order_date) as next_avg_order_total,
	gap_between_promised_actual_time as current_delivery_gap,
	lead(gap_between_promised_actual_time) over (partition by customer_id order by order_date) as next_delivery_gap
from blinkit_orders
group by 1, 2, 4, 6
)

select

	case
		when next_avg_order_total < current_order_total and next_delivery_gap > current_delivery_gap 
		then 'degraded order value but delivery timing improved'
		when next_avg_order_total < current_order_total and next_delivery_gap < current_delivery_gap 
		then 'degraded order value, delivery timing is still bad'
		when next_avg_order_total > current_order_total and next_delivery_gap > current_delivery_gap 
		then 'increased order value even after a delivery gap'
		when next_avg_order_total > current_order_total and next_delivery_gap < current_delivery_gap 
		then 'increased order value improved delivery timing'
		when next_avg_order_total < current_order_total and next_delivery_gap = current_delivery_gap 
		then 'decreased order value but delivery timing stayed same'
		when next_avg_order_total > current_order_total and next_delivery_gap = current_delivery_gap 
		then 'increased order value but delivery timing stayed same'
	else 'unknown timing and order value'
	end as delivery_corr_order_volume,
	
	count(*) as order_count,
	round(avg(current_order_total)::decimal, 2) as avg_total_order_value,
	round(count(customer_id) * 100.0 / (select count(order_id) from cte), 2) as order_count_pct
from cte 
where next_avg_order_total is not null and next_delivery_gap is not null
group by 1;


-- MoM revenue analysis.
with monthly_rev as 
(select
	date_trunc('month', order_date) as order_month,
	count(*) as order_count,
	lag(count(*)) over (order by date_trunc('month', order_date)) as prev_order_count,
	round(sum(order_total)::decimal, 2) as total_order_value,
	round(lag(sum(order_total)::decimal) over (order by date_trunc('month', order_date)), 2) as prev_month_total_order_value
from blinkit_orders
group by 1)

select
	order_month,
	
	order_count,
	case when prev_order_count is null or prev_order_count = 0 then 0
	else prev_order_count
	end as prev_order_count,

	case
		when prev_order_count is null or prev_order_count = 0 then 0
		else round((order_count- prev_order_count) * 100.0 / prev_order_count, 2) 
	end as MoM_order_growth,
	
	total_order_value,
	case when prev_month_total_order_value is null or prev_month_total_order_value = 0 then 0
	else prev_month_total_order_value
	end as prev_month_total_order_value,
	
	case
		when prev_month_total_order_value is null or prev_month_total_order_value = 0 then 0
		else round((total_order_value - prev_month_total_order_value) * 100.0 / prev_month_total_order_value, 2) 
	end as MoM_growth
	
from monthly_rev;

-- ************************************************************************* --
-- analysing monthly feedback ratio.

with cte as (
select
	date_trunc('month', a.order_date) as order_month,
	count(a.order_id) as total_order,
	count(b.feedback_id) as total_feedback,
	count(case when b.sentiment = 'Negative' then 1 end) as bad_review,
	count(case when b.sentiment = 'Neutral' then 1 end) as neutral_review,
	count(case when b.sentiment not in ('Neutral', 'Negative') then 1 end) as good_review
from blinkit_orders as a
join blinkit_customer_feedback as b on b.order_id = a.order_id
group by 1)

select
	order_month,
	
	total_order,
	round((total_order - lag(total_order) over (order by order_month)) * 100.0 / 
	lag(total_order) over (order by order_month), 2) as order_cnt_growth,
	
	round(bad_review * 100.0 / total_feedback, 2) as bad_feedback_rate,
	round(neutral_review * 100.0 / total_feedback, 2) as neutral_feedback_rate,
	round(good_review * 100.0 / total_feedback, 2) as good_feedback_rate

from cte order by 1;

-- analysing customers who actually left ordering after a bad review.

With feedback_cte as 
(select
	date_trunc('month', a.order_date) as order_month,
	a.customer_id,
	-- getting customer's last order and its review.
	a.order_date,
	-- indicator, if lead becomes null, then there was no order on the next date.
	lead(a.order_date) over (partition by a.customer_id order by a.order_date) as next_order_date,
	b.rating,
	b.sentiment
from blinkit_orders as a
join blinkit_customer_feedback as b on b.order_id = a.order_id
)


select
	order_month,
	count(distinct customer_id) as total_customer_cnt,
	count(case when next_order_date is NULL and sentiment = 'Negative' then 1 end) as customer_churn_cnt,
	round(count(case when next_order_date is NULL and sentiment = 'Negative' then 1 end) * 100.0 / 
	count(distinct customer_id), 2) as churn_pct
from feedback_cte
group by 1
order by 1;

-- analysing the possible reason for customer's bad review?
-- considering multiple factor, as - 
	-- 1. average delivery gap per order for the same customer.
	-- 2. order_value of that particular order.
	-- 3. rating given of that particular order.
	-- 4. sentiment and feedback text of the order.
	-- 5. if the customer has churned after that order or not (considering negative feedback and smaller order count)

-- I am using windows functions such as lag() or lead() to get customer previous order quantity, order_total, rating, sentiment.

with cte as
(select
	a.customer_id,
	a.order_id,
	a.order_total,
	a.delivery_gap_in_minutes,
	a.gap_between_promised_actual_time,
	b.feedback_category,
	b.sentiment,
	b.rating
from blinkit_orders as a
join blinkit_customer_feedback as b on b.order_id = a.order_id
)

select
	customer_id,
	feedback_category,

	count(order_id) as total_order_cnt,
	avg(case when sentiment = 'Negative' then order_total end) as aov_negative_feedback,
	avg(case when sentiment = 'Negative' then delivery_gap_in_minutes end) as delivery_gap_negative_feedback,
	
	avg(case when sentiment = 'Neutral' then order_total end) as aov_neutral_feedback,
	avg(case when sentiment = 'Neutral' then delivery_gap_in_minutes end) as delivery_gap_Neutral_feedback,
	
	avg(case when sentiment = 'Positive' then order_total end) as aov_Positive_feedback,
	avg(case when sentiment = 'Positive' then delivery_gap_in_minutes end) as delivery_gap_Positive_feedback

from cte
group by 1,2;


-- area wise delivery performance?
select
	b.area,
	-- area wise stats.
	count(distinct b.customer_id) as customer_count_per_area, 
	count(*) as order_cnt,
	round(avg(a.order_total)::decimal, 2) as AOV,

	-- delivery wise experience for each customer.
	c.feedback_category,
	round(avg(c.rating), 2) as avg_rating_per_category,

	-- delivery delay.
	round(avg(delivery_gap_in_minutes)::decimal, 2) as avg_delivery_delay,
	round(avg(gap_between_promised_actual_time)::decimal, 2) as avg_promised_delivery_delay

from blinkit_orders as a
join blinkit_customer_details as b on b.customer_id = a.customer_id
join blinkit_customer_feedback as c on c.order_id = a.order_id
group by b.area, c.feedback_category;

-- deep analysing : percentage of bad feedbacks that each month got ?
with cte as
(select
	date_trunc('month', a.order_date) as order_month,
	c.feedback_category,
	round(avg(c.rating), 2) as avg_rating,
	count(*) as total_fdbk_received,
	count(case when c.sentiment in ('Negative') then 1 end) as neg_fdbk_cnt,
	round(count(case when c.sentiment in ('Negative') then 1 end) * 100.0 / count(*), 2) neg_and_neutral_fdck_pct
from blinkit_orders as a
join blinkit_customer_details as b on b.customer_id = a.customer_id
join blinkit_customer_feedback as c on c.order_id = a.order_id
group by 1,2
),

-- analysing which category got the highest number of negative feedbacks in monthly basis.
cte2 as 
(select
	order_month,
	feedback_category,
	avg_rating,
	total_fdbk_received,
	neg_fdbk_cnt,
	dense_rank() over (partition by order_month order by neg_fdbk_cnt desc) as neg_fdbk_rnk
from cte
)

select
	*
from cte2
where neg_fdbk_rnk <= 1
order by 1;

-- analysing which area has the most negative review on which feedback category basis?

select
	b.area,
	c.feedback_category,
	c.sentiment,
	count(*) as feedbk_size
from blinkit_orders as a
join blinkit_customer_details as b on b.customer_id = a.customer_id
join blinkit_customer_feedback as c on c.order_id = a.order_id
where c.sentiment = 'Negative'
group by 1, 2, 3
order by 1, 4 desc;

-- analysing blinkit's revenue, rating and delivery performance by quarterly.
with cte as 
(select
	date_trunc('month', a.order_date) as order_month,
	extract(quarter from a.order_date) as quarter,

	-- business stats.
	count(a.order_id) as order_cnt,
	round(avg(a.order_total)::decimal, 2) as aov,
	round(avg(b.rating)::decimal, 2) as avg_rating,

	-- delivery performance.
	round(avg(extract(epoch from (a.actual_delivery_time::timestamp - a.order_date::timestamp)) / 60), 2) as avg_delivery_delay,
	round(avg(extract(epoch from (a.promised_delivery_time::timestamp - a.actual_delivery_time::timestamp)) / 60), 2) avg_promised_delivery_delay
from blinkit_orders as a
join blinkit_customer_feedback as b on b.customer_id = a.customer_id
group by 1, 2
)

select
	*,

	lag(order_cnt) over (order by order_month) as prev_ord_cnt,
	lag(aov) over (order by order_month) as prev_aov,
	lag(avg_delivery_delay) over (order by order_month) as prev_avg_delivery_delay,
	lag(avg_promised_delivery_delay) over (order by order_month) as prev_avg_promised_delivery_delay
from cte;


-- more day wise analysis, In this part we will do when blinkit faced more delivery delays by order hour,
-- order week and order month.
-- first we will be doing, which order hour faced more delivery delays 
-- and how many customer has rated bad reviews regarding delivery delays?

with cte as
(select
	extract(hour from a.order_date) as order_hour,

	round(avg(a.order_total)::decimal, 2) as avg_order_total,
	
	-- counting orders.
	count(a.order_id) as order_cnt,

	-- counting counts of orders that faced late deliveries.
	count(case when a.promised_delivery_time < a.actual_delivery_time then 1 end) as late_delivery_cnt,

	-- counting hours that got bad feedbacks regarding late delivery experience.
	count(case when c.feedback_category = 'delivery' and c.sentiment = 'negative' then 1 end) as bad_delivery_related_fdbk_cnt,

	round(count(case when c.feedback_category = 'delivery' and c.sentiment = 'negative' then 1 end) * 100.0 / count(case when a.promised_delivery_time < a.actual_delivery_time then 1 end), 2) as bad_delivery_related_fdbk_pct
	
from blinkit_orders as a
join blinkit_customer_details as b on b.customer_id = a.customer_id
join blinkit_customer_feedback as c on c.order_id = a.order_id
group by 1
order by 1
)

select
	*,
	dense_rank() over (order by order_cnt desc) order_cnt_rnk,
	dense_rank() over (order by bad_delivery_related_fdbk_pct desc) as bad_delivery_fdbk_pct_rnk
from cte;

-- pct of customer who experienced faster delivery after having a delayed delivery timing.
-- We might try more monthly basis.

with cte as
(select
	order_id,
	customer_id,
	gap_between_promised_actual_time,
	
	-- check for the next order delivery timing.
	-- Find customers who had a bad delivery timing (actual_delivery_time > promised_delivery_time) and for next order,
	-- he got a (actual_delivery_time < promised_delivery_time) good delivery timings.

	-- "gap_between_promised_actual_time" column in positive value means the delivery delivered before on time.
	
	lead(gap_between_promised_actual_time) over (partition by customer_id order by order_date) as next_order_timing
	
from blinkit_orders
)

select
	round(count(*) * 100.0 / (select count(*) from blinkit_orders), 2) as next_order_good_delivery_pct,
	round(count(case when next_order_timing >= 0 then 1 end) * 100.0 / (select count(*) from blinkit_orders), 2) as nxt_delivery_ordered_before_promised_timing_pct
from cte
where next_order_timing > gap_between_promised_actual_time; -- filtering out null and also which delivery timing that was poor than the previous delivery timing.
-- out of all orders, only ~26% orders faced a delivery timing improved, while ~16% of orders got their delivery before their promised timing.

-- checking customers who made more than one order.
with repeated_cust as
(select
	customer_id,
	count(*) as order_frequency
from blinkit_orders
group by customer_id
-- filtering out one time orders per customer.
having count(order_id) > 1)

select
	round((count(customer_id) * 100 / 
	(select count (distinct customer_id) from blinkit_orders))::decimal, 4) as repeated_customer_pct 
from repeated_cust;