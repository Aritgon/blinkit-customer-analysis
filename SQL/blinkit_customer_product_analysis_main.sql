-- In this file, we are commencing our main part blinkit analysis which will be put in github to create a portfolio.

-- 1. Customer cohort analysis --
/* In this analysis, we will analyse each customer's first month, 
	it will be termed as 'birth month' and how many customer's came at later month. 

	This will help us identify customer's buying pattern and monthly retention rate of blinkit.
*/

-- getting each customer's first month of purchase as 'birth month of each customer'
with birth_month_cte as 
(select
	customer_id,
	min(date_trunc('month', order_date)) as birth_month
from blinkit_orders
group by customer_id),

-- joining with the main table to get next purchase date.
next_month_cte as (
select
	b.birth_month,
	b.customer_id,
	(extract(year from a.order_date) - extract(year from b.birth_month)) * 12 +
	(extract(month from a.order_date) - extract(month from b.birth_month)) as month_number
from blinkit_orders as a
join birth_month_cte as b on b.customer_id = a.customer_id),

-- now counting each month's user first purchase and it's subsequent month's user comeback.
cohort_month_cte as (
select
	birth_month,
	month_number,
	count(distinct customer_id) as customer_cnt
from next_month_cte
group by birth_month, month_number),

/* using first_value() windows functions to get each birth_month's total user count.
	that first_value() will fetch the each month's total user count who ordered for first time in the app. 
	the subsequent month will show users who ordered in first month and cameback next month too.
*/

cohort_analysis_main as (
select
	birth_month,
	month_number,
	customer_cnt,
	first_value(customer_cnt) over (partition by birth_month) as first_month_customer_cnt_locked,
	round(customer_cnt * 100.0 / first_value(customer_cnt) over (partition by birth_month), 2) as retention_rate
from cohort_month_cte)

-- analysing what is the average retention rate for each month.

select
	month_number,
	round(avg(retention_rate)::decimal, 3) as avg_retention_rate
from cohort_analysis_main
group by month_number
order by month_number;

/*
	The latest query shows that right after birth month, user retention rate dropped to ~9% at 
	the subsequent month.
	This shows that blinkit's most customers are one time buyers according to the dataset and 
	only a small unit of people ordered second time.

	Blinkit need to improve it's user retention. 
	May be by giving super deals, coupons to selected customers and better product value.
*/


-- 1. RFM (recency, frequency and monetary) analysis --

/*
	This analysis will help us know blinkit's customer base in depth.
	what is RFM analysis ?
	
	R -> Recency - meaning how current the customer has ordered from blinkit from today 
	(in this case, last transactional day of the dataset).
	F -> Frequency - how many times each customers have ordered from blinkit.
	M -> Monetary - How much total value each customer has spent in the platform.

	After getting this three stats of each customer, we will use NTILE() to bucketize customers or create bins
	that will help us know which category of customer is contributing to blinkit.
*/

with cust_details_cte as 
(select
	a.customer_id,
	b.customer_name,

	(select max(order_date)::date from blinkit_orders) - max(order_date)::date as recency,
	count(a.order_id) as frequency,
	round(sum(a.order_total)::decimal, 2) as monetary
from blinkit_orders as a
join blinkit_customer_details as b on b.customer_id = a.customer_id
group by a.customer_id, b.customer_name),

/*
	Now, we will categorize each customer to give them a certain category.
	we will use case function to categorize recency score based on a numerical rank.
	
	min recency -> 0 & max recency -> 599
	since the gap is huge and probably contains a lot of churn customers. we are gonna rank recency score from backwards.
	we will follow the following formula, 
		if recency between 0 and 20, then rank 1,
		if recency between 21 and 35, then rank 2,
		if recency between 36 and 50, then rank 3,
		if recency between 51 and 80, then rank 4
		and rank 5 to customer's who have a recency score more than 80.
	
	min frequency -> 1 & max frequency -> 9.
	for setting up freq_score we will follow the following formula,
		if frequency between 9 and 6 then rank 1,
		if frequency between 6 and 4 then rank 2,
		if frequency betweek 3 and 2 then rank 3,
		if frequency = 2 then rank 4,
		else 1.

	min monetary -> 22.14 & max monetary -> 21686.80
	for setting up monetary_score, we will follow the following formula,
		when monetary > 5000 then 1
		when monetary between 3001 and 5000 then 2
		when monetary between 1501 and 3000 then 3
		when monetary between 801 and 1500 then 4
		else 5
*/

score_cte as 
(select
	customer_id,
	customer_name,

	recency,
	frequency,
	monetary,
	
	case
		when recency between 0 and 20 then 1
		when recency between 21 and 35 then 2
		when recency between 36 and 50 then 3
		when recency between 51 and 80 then 4
		else 5
	end as r_score,
	case
		when frequency >= 7 and frequency <= 9 then 1
		when frequency >= 4 and frequency <= 6 then 2
		when frequency = 3 then 3
		when frequency = 2 then 4
		else 5
	end as f_score,
	case
		when monetary > 5000 then 1
		when monetary between 3001 and 5000 then 2
		when monetary between 1501 and 3000 then 3
		when monetary between 801 and 1500 then 4
		else 5
	end as m_score
	
from cust_details_cte
)

/*
	In the next CTE we are going to create bins or categories 
	with the merged scores to get more compact result from the score cte.		
*/

select
	-- merging all the scores first.
	r_score,
	f_score,
	m_score,
	
	(r_score * 100 + f_score * 10 + m_score) as rfm_bins,

	round(avg(recency)::decimal, 2) as avg_recency,
	round(avg(monetary)::decimal, 2) as avg_monetary,
	
	-- counting customers.
	count(distinct customer_id) as cust_cnt,
	
	-- using whole customer_base to get pct of customers from each segments.
	round(count(distinct customer_id) * 100.0 / (select count(distinct customer_id) from blinkit_orders), 2) as cust_size_pct
from score_cte
group by r_score, f_score, m_score, rfm_bins
order by cust_size_pct desc;

/*
	Through this RFM analysis, we acknowledged that segment "531" has the highest customer share in blinkit's data with 10.22%
	of users. But for blinkit this group of customer is at risk because they have an avg recency approx. ~218 days but spends
	the 
	
*/


-- Analysis: Next-Product Propensity (Sequencing)
/*
	since we can't do product busket analysis in this dataset as the data is heavily normalized
	(one row = one order id = one product).
	we are doing next product after a order for every customer,
	this will help us what is the next product that customers bought after a order. 
*/

with product_cte as
(select

	a.customer_id,
	a.order_date as curr_order_date,
	d.product_name as curr_product,
	a.order_total as curr_order_total,
	
	lead(a.order_date) over (partition by a.customer_id order by a.order_date) as next_order_date,
	lead(d.product_name) over (partition by a.customer_id order by a.order_date) as next_product,
	lead(a.order_total) over (partition by a.customer_id order by a.order_date) as next_order_total,
	
	count(*) as sample_size
from blinkit_orders as a
join blinkit_order_items as b on b.order_id = a.order_id
join blinkit_customer_details as c on c.customer_id = a.customer_id
join blinkit_products as d on d.product_id = b.product_id
group by a.customer_id, a.order_date, d.product_name, a.order_total),

-- after that concatenate or count product name wise order size.
sequential_cte as
(select
	curr_product,
	next_product,
	count(*) as product_pair_size,
	round(avg(curr_order_total + next_order_total)::decimal, 2) as avg_paired_order_total
from product_cte
where next_product is not null
group by curr_product, next_product
having count(*) > 1 -- filtering out one random order pair.
order by product_pair_size desc)

/*
	From this sequencing analysis, we acknowledged that baby wipes and vitamins was the first most bought pair.
	After buying 'baby wipes' customers generally bought 'vitamins' the most. This pair has a size of 9. Meaning, it was bought
	9 times, averaging rupees 5716.29 of revenue.

	Followed by 'dish soap' + 'potatoes' which was also bought 9 times averaging 4314.02 rupees of revenue.

	After that, 'dish soap' + 'cough syrup' which was bough 8 times in pair averaging 4262.11 rupees of revenue
	in the third position.
*/

-- From here Moving onto which product order made more revenue and were ordered the most. 
select
	s1.curr_product as item_x,
	s1.next_product as item_y,

	-- stats.
	s1.product_pair_size as x_to_y_count,
	s1.avg_paired_order_total as x_to_y_avg_revenue,

	s2.curr_product as item_x_reverse,
	s2.next_product as item_y_reverse,
	
	-- stats for the next two products bought together.
	s2.product_pair_size as y_to_x_count,
	s2.avg_paired_order_total as y_to_x_avg_revenue,

	-- avg gap between first pairing and second pairing.
	(s1.avg_paired_order_total - s2.avg_paired_order_total) as avg_revenue_gap
from sequential_cte as s1
join sequential_cte as s2 on s1.curr_product = s2.next_product -- matching on the items that is first on first pair and second in second pair.
	and s1.next_product = s2.curr_product -- matching on items that is second in first pair and first in second pair.
where s1.curr_product > s1.next_product -- filtering out product pairs that 
order by avg_revenue_gap desc;

/*
	this analysis is showing order pairs that made how much revenue than the same product pairs
	but in reverse orders made how much revenue.
*/

-- Analysis: Next-category Propensity (Sequencing)
with category_cte as
(select
	a.customer_id,

	d.category as curr_category,
	a.order_total as curr_order_total,
	
	a.order_date as curr_order_date,
	lead(d.category) over (partition by a.customer_id order by a.order_date) as next_category,
	lead(a.order_total) over (partition by a.customer_id order by a.order_date) as next_order_total,
	lead(a.order_date) over (partition by a.customer_id order by a.order_date) as next_order_date
	
from blinkit_orders as a
join blinkit_order_items as b on b.order_id = a.order_id
join blinkit_customer_details as c on c.customer_id = a.customer_id
join blinkit_products as d on d.product_id = b.product_id)

select
	curr_category,
	next_category,

	count(*) as pair_ordering_size,
	round(avg(curr_order_total + next_order_total)::decimal, 2) as avg_revenue_per_pair
from category_cte
where next_category is not null
group by 1, 2
having count(*) > 1
order by 3 desc;

-- Next up, we are going to do correlation analysis between delivery timing and customer feedback rating per customer.

/*
	
*/

with cust_cte as
(select

	b.order_id,
	a.customer_id,
	b.gap_between_promised_actual_time,
	a.feedback_category,
	a.sentiment,
	a.rating
	
from blinkit_customer_feedback as a
left join blinkit_orders as b on b.order_id = a.order_id) 
/*
	using left join to get every order each customers made from the left table.
	
	Joining on the order_id for both the tables because only some customers leave a feedback after their order.
	For example, there might be 10 orders for a customer where they only submitted a feedback for only one order.
	THe count() on the next query will every row for every order. Now to count order numbers , this is okay.
	but when we need to use count with case function this will wrongly output the result because every row will be counted
	based on the case function condition.
*/

select
	case
		when gap_between_promised_actual_time between -5 and -1 then 'slightly late delivery'
		when gap_between_promised_actual_time between -10 and -6 then 'moderate late delivery'
		when gap_between_promised_actual_time between -15 and -11 then 'late delivery'
		when gap_between_promised_actual_time between -20 and -16 then 'very late delivery'
		when gap_between_promised_actual_time between -25 and -21 then 'concerningly late delivery'
		when gap_between_promised_actual_time between -30 and -26 then 'customer might churn if delivery becomes this late'

		when gap_between_promised_actual_time between 0 and 5 then 'slightly fast delivery'
		when gap_between_promised_actual_time between 6 and 10 then 'moderate late delivery'
		when gap_between_promised_actual_time between 11 and 15 then 'late delivery'
		when gap_between_promised_actual_time between 16 and 20 then 'very late delivery'
		when gap_between_promised_actual_time between 21 and 25 then 'concerningly late delivery'
	else 'other timing'
	end as delivery_timing_range,

	-- stats.
	round(avg(gap_between_promised_actual_time)::decimal, 2) as avg_gap_between_promised_and_actual_time,
	count(order_id) as order_cnt,
	
	-- getting negative sentiments feedback count and pct from all feedbacks recorded for each delivery type.
	count(case when sentiment = 'negative' then 1 end) as negative_fdbk_cnt,

	count(case when sentiment = 'negative' and feedback_category = 'delivery' then 1 end) as negative_fdbk_regarding_delivery_cnt,
	-- round(count(case when sentiment = 'negative' and feedback_category = 'delivery' then 1 end) * 100.0 / 
	-- count(order_id), 2) as negative_fdbk_pct

	round(count(case when sentiment = 'negative' and feedback_category = 'delivery' then 1 end) * 100.0 / 
	count(case when sentiment = 'negative' then 1 end), 2) as negative_fdbk_for_delivery_pct,
	
	-- count(case when sentiment = 'positive' then 1 end) as positive_fdbk_cnt,
	-- count(case when sentiment = 'neutral' then 1 end) as neutral_fdbk_cnt

	round(count(case when sentiment = 'negative' and feedback_category = 'delivery' then 1 end) * 100.0 
	/ sum(count(*)) over (), 2) as delivery_negative_reviews_pct,

	-- getting order contribution pct of each segments from total orders.
	round(count(order_id) * 100.0 / sum(count(order_id)) over (), 2) as order_contribution_pct
	
from cust_cte
group by delivery_timing_range
order by delivery_negative_reviews_pct desc;

/*
	according to analysis, surprisingly, the positive category which is 'slightly fast delivery' got the most negative reviews 
	for delivery being late than promised delivery time among all orders. 
	
	This category had an average time (promised_delivery_time - actual_delivery_time) 2.47 in minutes.
	it signifies that customers aren't satisfied with blinkit delivery timing even 
	if the average between promised and actual timing is 2.47 minutes.

	After that, we have 'slightly late delivery' at the second position where the most negative reviews 
	for delivery being late than promised delivery time among all orders.
	This category had an average timing (promised_delivery_time - actual_delivery_time) as -3.02 in minutes.

	both of this segments have the most combined negative review pct among all orders (~6%) which maintained
	a close delivery timing while sometimes being 2.47 minutes fast (avg) and 3.02 minutes late than promised delivery time.

*/


-- analysing which company is making the most revenue/loss monthly wise.

select
	p.brand,
	p.category,
	p.product_name,
	
	-- product stats.
	round(sum(p.price)::decimal, 2) as total_cost_price,
	round(sum(o.order_total)::decimal, 2) as total_order_value,

	round(sum(o.order_total)::decimal - sum(p.price)::decimal, 2) as revenue_gap
from blinkit_orders as o
join blinkit_order_items as oi on oi.order_id = o.order_id
join blinkit_products as p on p.product_id = oi.product_id
group by p.brand, p.category, p.product_name
order by p.brand;








