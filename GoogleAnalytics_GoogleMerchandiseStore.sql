#Exploring the public dataset from google analytics for google merchandise store
#Dataset Url: https://www.blog.google/products/marketingplatform/analytics/introducing-google-analytics-sample/
#Google Analytics' BigQuery Export Schema: https://support.google.com/analytics/answer/3437719?hl=en

#This self-training partly follows the Big Query cookbook: https://support.google.com/analytics/answer/4419694?hl=en&ref_topic=3416089
#Disclaimer 1: Some queries can differ fundamentally from those in the cookbook for the same questions.
#Disclaimer 2: My understanding of hits.product.productQuantity can be incorrect.

#----------
#The number of sessions for each channel on 2017-08-01
#----------
SELECT channelGrouping, count(distinct concat(cast(visitId as STRING), "-", fullVisitorId)) session_count
FROM [bigquery-public-data:google_analytics_sample.ga_sessions_20170801]
group by channelGrouping
order by session_count;

#----------
#The number of sessions from different browsers on 2017-08-01
#----------
SELECT device.browser , count(distinct concat(cast(visitId as STRING), "-", fullVisitorId)) session_count
FROM [bigquery-public-data:google_analytics_sample.ga_sessions_20170801]
group by device.browser 
order by session_count;

#----------
#Total number of transactions generated per device in July 2017?
#----------
SELECT
device.browser,
SUM ( totals.transactions ) AS total_transactions
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`
WHERE
_TABLE_SUFFIX BETWEEN '20170701' AND '20170731'
GROUP BY
device.browser
ORDER BY
total_transactions DESC

#----------
#Average bounce rate per channel in July 2017?
#Note: some other dimensions: trafficSource.source, device.browser
#----------
SELECT
channel,
total_visits,
total_bounces,
( ( total_bounces / total_visits ) * 100 ) AS bounce_rate
FROM (
SELECT
channelGrouping as channel,
COUNT (*) AS total_visits,
SUM ( totals.bounces ) AS total_bounces
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`
WHERE
_TABLE_SUFFIX BETWEEN '20170701' AND '20170731'
GROUP BY
channel )
ORDER BY
total_visits DESC

#----------
#View the sessions that have completed transaction on 1st August 2017
#order by transaction revenue
#----------
with sessionList as 
(
  select distinct concat(cast(fullVisitorId as string), cast(visitId as string)) as unique_session
  from `bigquery-public-data.google_analytics_sample.ga_sessions_20170801`
  cross join unnest(hits) as hits
  where
    hits.eCommerceAction.action_type = '6'
)
select *
from `bigquery-public-data.google_analytics_sample.ga_sessions_20170801` as a
where concat(cast(a.fullVisitorId as string), cast(a.visitId as string)) in (select unique_session from sessionList)
order by a.totals.totalTransactionRevenue desc

#----------
#Average number of product page views for purchaser vs non-purchaser in July 2017
#----------
(select
count(distinct fullVisitorId) as number_of_users,
sum(totals.pageviews) as total_pageviews,
(sum(totals.pageviews)/count(distinct fullVisitorId)) as avg_pageviews,
'purchaser' as type
from `bigquery-public-data.google_analytics_sample.ga_sessions_*`
where _table_suffix between '20170701' and '20170731'
and totals.transactions >= 1)

union all

(select
count(distinct fullVisitorId) as number_of_users,
sum(totals.pageviews) as total_pageviews,
(sum(totals.pageviews)/count(distinct fullVisitorId)) as avg_pageviews,
'non-purchaser' as type
from `bigquery-public-data.google_analytics_sample.ga_sessions_*`
where _table_suffix between '20170701' and '20170731'
and totals.transactions is null)



#-------------
#Show sequence of viewed pages for each visitor on 31st July 2017
#-------------
select 
fullVIsitorId,
visitNumber as session_number,
hits.hitNumber as hitNumber,
hits.page.pagePath  as viewed_url
from `bigquery-public-data.google_analytics_sample.ga_sessions_20170731`, unnest(hits) as hits
where 
(hits.type = 'PAGE')
order by fullVisitorId, session_number, hitNumber

#-------
#View unique products purchased on 31st July 2017
#-------
SELECT
distinct product.v2ProductName 
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_20170731`,
	unnest (hits) as hits,
	unnest (hits.product) as product
WHERE 
	product.productQuantity is not null
	AND 
	hits.eCommerceAction.action_type = '6'

#-------
#View products that are also bought by purchasers on 20170731 who
#bought products with "Men" in the product names during the previous 90 days (exclusive of 20170731)
#-------
with visitorList as 
(
  SELECT
  distinct fullVisitorId as id
  FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`,
        unnest (hits) as hits,
        unnest (hits.product) as product
  WHERE 
    (
      _table_suffix 
        between 
        format_date("%Y%m%d", date_sub('2017-07-31', interval 90 day)) 
        and 
        format_date("%Y%m%d", date_sub('2017-07-31', interval 1 day))
     )
    and
    (hits.eCommerceAction.action_type = '6')
    and
    (product.v2ProductName like "% Men%")
 )
select 
fullVisitorId as repeating_visitor,
product.v2ProductName as products,
sum(product.productQuantity) as quantity
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_20170731` a,
    unnest (hits) as hits,
    unnest (hits.product) as product
where
  a.fullVisitorId in (select id from visitorList)
  and
  (hits.eCommerceAction.action_type = '6')
  and
  (product.productQuantity is not null)
group by repeating_visitor, products
order by repeating_visitor, quantity

#-------
#The list of unique purchasing fullVisitorId(s) and the total revenue from them 
#from the last 3 months (relative to 2017-07-31)
#-------

select
fullVisitorId,
sum(totals.totalTransactionRevenue)/1000000 as total_revenue
from `bigquery-public-data.google_analytics_sample.ga_sessions_*`
where
  (
      _table_suffix 
        between 
        format_date("%Y%m%d", date_sub('2017-07-31', interval 3 month)) 
        and 
        format_date("%Y%m%d", '2017-07-31')
  )
  AND
  fullVisitorId in 
                (select distinct fullVisitorId
                from `bigquery-public-data.google_analytics_sample.ga_sessions_*`, 
                  unnest(hits) as hits
                where
                  (
                      _table_suffix 
                        between 
                        format_date("%Y%m%d", date_sub('2017-07-31', interval 3 month)) 
                        and 
                        format_date("%Y%m%d", '2017-07-31')
                  )
                  AND   
                  hits.eCommerceAction.action_type = '6'
                 )
group by fullVisitorId
order by total_revenue desc








