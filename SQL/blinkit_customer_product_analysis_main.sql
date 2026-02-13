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
having count(*) > 1) -- filtering out one random order pair.

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
where s1.curr_product > s1.next_product -- filtering out product pairs that are same.
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
	round(avg(curr_order_total + next_order_total)::decimal, 2) as avg_revenue_per_pair,
	
	-- we are analysis products that was bought after a certain product and how many much revenue they made.
	dense_rank() over (partition by curr_category order by avg(curr_order_total + next_order_total) desc) as revenue_rank
from category_cte
where next_category is not null
group by 1, 2
having count(*) > 1; -- filtering category sequence that was only ordered one time.

-- Next up, we are going to do correlation analysis between delivery timing and customer feedback rating per customer.

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
	THe count() on the next query will count every row for every order. Now to count order numbers , this is okay.
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

	count(case when sentiment = 'negative' and feedback_category = 'delivery' then 1 end) as neg_fdbk_for_delivery_cnt,
	-- round(count(case when sentiment = 'negative' and feedback_category = 'delivery' then 1 end) * 100.0 / 
	-- count(order_id), 2) as negative_fdbk_pct

	round(count(case when sentiment = 'negative' and feedback_category = 'delivery' then 1 end) * 100.0 / 
	count(case when sentiment = 'negative' then 1 end), 2) as neg_fdbk_for_delivery_among_all_neg_reviews_pct,
	
	-- count(case when sentiment = 'positive' then 1 end) as positive_fdbk_cnt,
	-- count(case when sentiment = 'neutral' then 1 end) as neutral_fdbk_cnt

	round(count(case when sentiment = 'negative' and feedback_category = 'delivery' then 1 end) * 100.0 
	/ sum(count(*)) over (), 2) as neg_review_for_delivery_among_all_orders_pct,

	-- getting order contribution pct of each segments from total orders.
	round(count(order_id) * 100.0 / sum(count(order_id)) over (), 2) as order_contribution_pct
	
from cust_cte
group by delivery_timing_range
order by delivery_negative_reviews_pct desc;

/*
	according to analysis, surprisingly, the positive category which is 'slightly fast delivery' got the most negative reviews 
	for delivery being late than promised delivery time among all orders. 
	
	This category had an average time (promised_delivery_time - actual_delivery_time) of  2.47 in minutes.
	it signifies that customers aren't satisfied with blinkit delivery timing even 
	if the average between promised and actual timing is 2.47 minutes.
	now in this analysis, 2.47 minutes means delivery agents still reached at the location `2 minutes ago` of the
	promised delivery timing. now this category is the only positive category interms of delivery timing, all other
	category is in negative which surprises this finding even more.

	
	After that, we have 'slightly late delivery' at the second position where the most negative reviews 
	for delivery being late than promised delivery time among all orders.
	This category had an average timing (promised_delivery_time - actual_delivery_time) as -3.02 in minutes.

	both of this segments have the most combined negative review pct among all orders (~6%) which maintained
	a close delivery timing while sometimes being 2.47 minutes fast (avg) and 3.02 minutes late than promised delivery time.
*/


/*
	This is a more personalized analysis for products, where we can find area wise category/product analysis or monthly most
	ordered categories/products. But this personalized analysis can be done in PowerBI, so we will only be doing customer's 
	most bought and amount they have spend on which category.
	
	next up customer's busket analysis.

	in this analysis, we will analyze on which category customer's spend the highest amount.

	After that, we will analyze monthly customer's busket. 
*/

select
	c.customer_id,
	c.customer_name,

	p.category,
	
	count(*) as order_size,

	round(sum(o.order_total)::decimal, 2) as order_value

from blinkit_orders as o
join blinkit_customer_details as c on c.customer_id = o.customer_id
join blinkit_order_items as oi on oi.order_id = o.order_id
join blinkit_products as p on p.product_id = oi.product_id
group by c.customer_id, c.customer_name, p.category
having count(*) > 1 -- filtering customer's one time purchase.
order by c.customer_id, order_size desc, order_value desc;

/* As there is less insights about customer's most ordered products, we are moving to monthly most ordered and most avg revenue 
generated category for blinkit */

with mthly_categorical_order_cte as
(select
	date_trunc('month', o.order_date) as order_month,
	
	-- product category.
	p.category,
	count(*) as order_cnt,

	round(avg(o.order_total)::decimal, 2) as aov,
	round(sum(o.order_total)::decimal, 2) as order_total_value

from blinkit_orders as o
join blinkit_order_items as oi on oi.order_id = o.order_id
join blinkit_products as p on p.product_id = oi.product_id
group by 1,2),

/*
	Next cte will be about per month's total order count, avg order value and total order value.
*/

mthly_order_cte as (
select
	date_trunc('month', order_date) as order_month,
	count(*) as order_count_mth, -- each month's total order count.
	round(avg(order_total)::decimal, 2) as monthly_aov,
	round(sum(order_total)::decimal, 2) as mth_total_order_value
from blinkit_orders
group by 1),

main_cte as
(select
	a.order_month,
	a.category,
	a.order_cnt,
	b.order_count_mth,
	
	a.aov,
	b.monthly_aov,

	a.order_total_value,
	b.mth_total_order_value,
	
	-- analysing order contribution ratio for that month's total order count.
	round((a.order_cnt * 100.0 / b.order_count_mth)::decimal, 2) as mthly_total_order_count_contribution_pct,
	round((a.order_total_value * 100.0 / b.mth_total_order_value)::decimal, 2) as monthly_order_value_contribution_pct
	
from mthly_categorical_order_cte as a
left join mthly_order_cte as b on b.order_month = a.order_month)

/*
	Using dense_rank() on order_count_pct and order_value_pct to rank categories that made the highest revenue
	for a month and also order count ranking.
*/

select
	*,

	-- dense_rank() on revenue first.
	dense_rank() over (partition by order_month order by monthly_order_value_contribution_pct desc) as monthly_order_value_cont_rank,

	-- dense_rank() on order count.
	dense_rank() over (partition by order_month order by mthly_total_order_count_contribution_pct desc) as monthly_order_count_cont_rank
from main_cte;

/*
	Through this analysis, we have acknowledged that despite not being placed at top for total monthly order count rank, 
	'grocery & staples', 'cold drinks & juices', 'baby care' & 'dairy & breakfast' ranked 3 times as top revenue generating
	category, followed by 'fruits & vegetables' , 'pharmacy' & 'personal care' which ranked 2 times.
	
	In terms of revenue making analysis, the category 'dairy & breakfast' achieved the most highest total order value of ~2.2 lakhs,
	followed by 'grocery & staples' which made around ~1.6 lakhs of total order value.
*/


with product_cte as 
(select
	o.customer_id,
	p.product_id,
	p.category as curr_category,
	
	-- next product_category for the same customer.
	lead(p.category) over (partition by o.customer_id order by o.order_date) as next_category,
	lead(o.order_total) over (partition by o.customer_id order by o.order_date) as next_order_total

from blinkit_orders as o
left join blinkit_order_items as oi on oi.order_id = o.order_id
left join blinkit_products as p on p.product_id = oi.product_id)

select
	next_category,
	count(*) as next_order_cnt,
	round(sum(next_order_total)::decimal, 2) as tov_for_next_category,
	round(avg(next_order_total)::decimal, 2) as aov_for_next_category
from product_cte
where next_category is not null -- filtering out null values as categories.
group by next_category
order by next_order_cnt desc; -- 158 milliseconds, after 133 milliseconds.

/*
	According to our 'immediate next product' or 'second product after first order' analysis,
	'dairy & breakfast' has ranked the first with 320 total order count which has aov of rupees
	2141.93, followed by 'pet care', which has ranked second with 294 total order count and an aov of 2287.18
	and 'household care' category at the third position with 284 total order count and a aov of 2248.06 rupees.
*/

/*
	Next up, we have 'monthly bad review pct of all reviews', 
	this will help us identify lags that users are experiencing while interacting with blinkit's app, 
	delivery and product experience.
	
*/

with review_cte as
(select
	date_trunc('month', feedback_date) as feedback_month,
	feedback_category,
	count(*) as feedback_cnt,
	count(case when sentiment = 'negative' then order_id end) as negative_rev_cnt,
	sum(count(case when sentiment = 'negative' then order_id end)) over(partition by date_trunc('month', feedback_date)) as total_bad_review_cnt,
	sum(count(*)) over (partition by date_trunc('month', feedback_date)) as total_order_cnt
from blinkit_customer_feedback
group by 1, 2),

fdbk_cte as
(select
	feedback_month,
	feedback_category,
	feedback_cnt,
	negative_rev_cnt,
	total_order_cnt,
	round((negative_rev_cnt * 100.0 / feedback_cnt)::decimal, 2) as neg_to_category_fdbk_ratio,
	round((negative_rev_cnt * 100.0 / total_order_cnt)::decimal, 2) as neg_to_monthly_total_fdbk_ratio
from review_cte
order by feedback_month),

/*
	from here, we can analyse or find out which category of feedback got the most numbers of negative fdbk
	and how many times it came in first rank.
*/

rank_cte as
(select
	feedback_month,
	feedback_category,
	feedback_cnt,
	negative_rev_cnt,
	total_order_cnt,

	-- ranking for each month by category wise.
	dense_rank() over (partition by feedback_month order by negative_rev_cnt desc) as neg_rev_rank
from fdbk_cte)

select * from rank_cte;

select
	feedback_category,
	neg_rev_rank,
	count(*) as num_of_first_ranks
from rank_cte
group by feedback_category, neg_rev_rank
order by feedback_category, neg_rev_rank;

/*
	From our analysis, we get to see that 'customer service' and 'product quality' has got the 1st rank
	for 8 times in months interms of negative reviews. For a q-commerce like blinkit having the most negative 
	reviews for categories like 'customer service' is a red flag because either customer's aren't happy with
	app's service, delivery and timing or they aren't getting the right product or amount as per their need.

	'delivery' category came in the second spot with a ranking score of 5. Though, for a q-commerce, negative 
	reviews are generally expected for late delivery or other delivery related issues but blinkit surprisingly 
	got this category at the second position.

	'app experience' came in third position with the ranking score of 2. This signifies that blinkit still need 
	to upgrade their app experience via more UX tweaking for easier navigation and shopping experience.

	*** Note to keep : this analysis could still be done by each category's avg rating and sentiment count but
	this monthly trend analysis gives more complex insights and findings.
*/


/*
	Next up, we have margin difference vs total and avg generated revenue per product and product category.
	This analysis will help us know does higher margin_difference_pct generates 
	more average order value or not?
*/


select
	margin_percentage,

	count(distinct p.category) as category_cnt,
	count(distinct p.product_id) as prod_cnt, -- counting actual products that fall into the margin difference category.
	count(*) as order_cnt, -- counting order counts.
	
	-- each margin difference's avg mrp.
	round(avg(p.mrp)::decimal, 2) as avg_mrp,
	
	-- each margin difference's avg_generated order_total.
	round(avg(o.order_total)::decimal, 2) as aov,

	-- ratio of avg mrp and avg order value of the margin tally.
	round((avg(p.mrp) * 100.0 / avg(o.order_total))::decimal, 2) as avg_mrp_to_order_value_ratio
	
from blinkit_orders as o
left join blinkit_order_items as oi on oi.order_id = o.order_id
left join blinkit_products as p on p.product_id = oi.product_id
group by margin_percentage
order by avg_mrp_to_order_value_ratio desc; -- restructuring the output on descending order per margin difference tally.


/*
	From our analysis, margin category of 30 got the most avg mrp to order value ratio of 33.53, 
	surpassing margin category of both 40 and 35 (pcts). 
	
	Interestingly, the margin category of 35 pct got the highest order count 1438 
	while 30 pct got 709 count of total orders.
	
	this signifies that product categories that falls between 30 and 35 of margin difference (mrp to order total)
	makes more revenue for blinkit than the other categories.

*/

/*
	By relating to the prev analysis and insights, we will analyse categories and products that fall into different 
	margin difference pct and how they are making revenues for blinkit.
*/

select
	-- firstly, we are getting the margin difference percentage.
	p.margin_percentage,
	
	-- getting the product categories.
	p.category,
	p.product_name,

	count(*) as order_cnt,
	
	-- avg mrp per category.
	round(avg(p.price)::decimal, 2) as avg_price,
	round(avg(p.mrp)::decimal, 2) as avg_mrp,
	
	-- avg order value.
	round(avg(o.order_total)::decimal, 2) as aov
	
from blinkit_orders as o
left join blinkit_order_items as oi on oi.order_id = o.order_id
left join blinkit_products as p on p.product_id = oi.product_id
group by p.margin_percentage, p.category, p.product_name
order by aov desc;


/*
	from our previous analysis, we saw that margin difference category 30 is making the most revenues per order
	than other margin tallies. 

	But when we combine both category and product, the insights are telling a different story.
	we are seeing 'sugar' which falls under ''grocery & staples'' category making the highest average order value
	(2501.59 Rs per order) while having margin difference (mrp to price) is only 15%!
	
	
	Now, Sugar is measured by weight but not by quantity, so this insight still stays safe from data anomaly.
*/


/*
	Next up we are analysing, QoQ growth of blinkit and revenue performance.
*/

with year_cte as
(select
	o.order_year,
	extract(quarter from o.order_date) as order_quarter,

	-- getting cost prices to calculate average company spend per product and total company spend.
	round(avg(p.price)::decimal, 2) as avg_cost_price,
	round(sum(p.price)::decimal, 2) as total_cost_price,
	
	-- normal stats.
	count(*) as order_cnt,
	round(avg(o.order_total)::decimal, 2) as aov,
	round(sum(o.order_total)::decimal, 2) as total_order_value

from blinkit_orders as o
join blinkit_order_items as oi on oi.order_id = o.order_id
join blinkit_products as p on p.product_id = oi.product_id
group by 1,	2),

qoq_cte as
(select
	 order_year,
	 order_quarter,

	 total_cost_price,
	 -- last year's total cost price.
	 lag(total_cost_price) over (order by order_year, order_quarter) as prev_cost_price,
	 
	 total_order_value,
	 --	last year's total order value.
	 lag(total_order_value) over (order by order_year, order_quarter) as prev_total_order_value
from year_cte)

select
	order_year,
	order_quarter,

	total_cost_price,
	prev_cost_price,
	-- prev year total cost price.
	round(((total_cost_price - prev_cost_price) * 100.0 / prev_cost_price)::decimal, 2) as qoq_cost_price_growth,

	total_order_value,
	prev_total_order_value,
	-- prev year total order value.
	round(((total_order_value - prev_total_order_value) * 100.0 / prev_total_order_value)::decimal, 2) as qoq_order_value_growth
	
from qoq_cte;
-- where prev_cost_price is not null and prev_total_order_value is not null;
-- filtering out quarter 4 of 2024 because quarter 4 had lesser quantity of orders due to not sufficient data.


/*
	Now, we are analysing hourly order rate. 
	This analysis will help us know which hour of the day gets the most orders from customers and which hour
	derives the most revenue.
*/

select
	*,

	-- ranking based on aov for each hour in each timing division tally.
	dense_rank() over (partition by timing_division_per_day order by aov DESC) as aov_rank

from (select
	case
		when order_hour >= 0 and order_hour <= 6 then 'night to early morning'
		when order_hour >= 7 and order_hour <= 12 then 'morning to day'
		when order_hour >= 13 and order_hour <= 18 then 'noon to evening'
		when order_hour >= 19 and order_hour <= 24 then 'night time'
		else 'other timing'
	end as timing_division_per_day, -- creating buckets for timings.
	order_hour, -- using order hour to actually signify the order hour.
	count(*) as order_cnt,
	round(avg(order_total)::decimal, 2) as aov -- aov per order.
from blinkit_orders
group by order_hour) as main
order by timing_division_per_day;


/*
	analysing monthly and weekly sales for blinkit.
*/

select
	*,

	-- ranking each week in a month based on aov.
	dense_rank() over (partition by order_month order by aov DESC) as aov_rank

from (select
	TO_CHAR(order_date, 'month') as order_month,
	'Week ' || ceil(extract (day from order_date) / 7.0) as week,
	count(*) as order_cnt,
	round(avg(order_total)::decimal, 2) as aov
from blinkit_orders
group by 1,2) as main
order by order_month;

/*
	We are now analysing monthly bad review count for delivery service of blinkit.
*/

select
	o.order_month,
	count(*) as order_cnt,
	count(cf.feedback_id) as feedback_cnt,

	-- counting negative reviews.
	count(case when cf.sentiment = 'negative' then o.order_id end) as neg_rev_cnt,

	-- counting negative reviews for delivery.
	count(case when cf.feedback_category = 'delivery' and 
	cf.sentiment = 'negative' then o.order_id end) as neg_rev_for_delivery_cnt,

	-- neg review for delivery percentage.
	round((count(case when cf.feedback_category = 'delivery' and 
	cf.sentiment = 'negative' then o.order_id end) * 100.0 / 
	count(case when cf.sentiment = 'negative' then o.order_id end))::decimal, 2) as neg_delivery_review_pct,

	round((count(case when cf.feedback_category = 'delivery' and 
	cf.sentiment = 'negative' then o.order_id end) * 100.0 / count(*))::decimal, 2) as neg_delivery_fdbk_to_all_order_pct
	
	
	
from blinkit_orders as o
join blinkit_customer_feedback as cf on cf.order_id = o.order_id
group by o.order_month
order by o.order_month;


/*
	As per our analysis requirement and business questions, the analysis with SQL is pretty much done
	and we are moving forward to dashboard building with the data (powerBI).

	Before jumping onto the powerBI, I am creating some views for some important queries that gave us
	valuable insights which will help me build the dashboards in a more controlled, clean and faster way.

	Why views are important for our case? 
		- Views are doing heavy lifting (calculation for each query) inside SQL, not powerBI model.
		- Views are not stored physically (virtual table). So it is not bulking on memory and storage.
		- Views are pre-calculated once it is initiated and we can query a view for infinite times.
		- Views are easier to explain to the stakeholders. 
		- Views requires a manual refresh if data changes which can be easily inside powerBI.
		As views are pre-calculated, powerBI dashboards are much lighter and our dashboard will stay fresh and fast.

*/

-- creating a VIEW for the RFM analysis.

create view vw_rfm_customer_segment as
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
group by birth_month, month_number)

/* using first_value() windows functions to get each birth_month's total user count.
	that first_value() will fetch the each month's total user count who ordered for first time in the app. 
	the subsequent month will show users who ordered in first month and cameback next month too.
*/

select
	birth_month,
	month_number,
	customer_cnt,
	first_value(customer_cnt) over (partition by birth_month) as first_month_customer_cnt_locked,
	round(customer_cnt * 100.0 / first_value(customer_cnt) over (partition by birth_month), 2) as retention_rate
from cohort_month_cte;

-- analysing what is the average retention rate for each month.

-- select
-- 	month_number,
-- 	round(avg(retention_rate)::decimal, 3) as avg_retention_rate
-- from cohort_analysis_main
-- group by month_number
-- order by month_number;

-- select * from vw_rfm_customer_segment;

select
	month_number,
	round(avg(retention_rate)::decimal, 2) as avg_retention_rate
from vw_rfm_customer_segment
group by month_number
order by month_number;

drop view if exists vw_rfm_customer_segment;


-- analysing year-on-year and monthly revenue per order (order_total - unit_price * mrp)

with cte as (
select 
	o.order_date,
	o.order_id,
	oi.product_id,
	oi.quantity,
	oi.unit_price,
	o.order_total,

	-- value check.
	round((oi.unit_price * oi.quantity)::decimal , 2)as original_price
from blinkit_order_items as oi
join blinkit_orders as o on o.order_id = oi.order_id)

select
	extract(year from order_date) as order_year,
	extract(month from order_date) as order_month,

	round(sum(order_total)::decimal, 2) as total_order_value,
	round(sum(original_price)::decimal, 2) as total_original_price,
	round((sum(order_total) - sum(original_price))::decimal, 2) as total_revenue,

	(sum(original_price) / sum(order_total)) as total_revenue_ratio
from cte
group by 1,2
order by 1,2;
