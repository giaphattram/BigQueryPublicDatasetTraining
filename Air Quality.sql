#Exploring Big Query Public Dataset Global Air Quality
#Dataset URL: https://console.cloud.google.com/marketplace/details/openaq/real-time-air-quality?filter=solution-type:dataset&filter=category:science-research

#------
#In which years is the data availalbe?
#------
select distinct extract(year from timestamp) as timestamp_year
from `bigquery-public-data.openaq.global_air_quality`
order by timestamp_year;

#------
#In many countries is the data available?
#------
select count(distinct country)
from `bigquery-public-data.openaq.global_air_quality`;

#------
#What are the difference air pollutants in the dataset?
#------
select distinct pollutant
from `bigquery-public-data.openaq.global_air_quality`;

#------
#Which air pollutants are most and least prevalent in February 2019?
#Prevalence is measured by the number of city-country in which the pollutant is recorded
#------
(select pollutant, count(distinct concat(city,"-",country)) as city_count, 'least' as label
from  
  (select pollutant, city, country, timestamp from `bigquery-public-data.openaq.global_air_quality` 
   where 
     (extract(year from timestamp)=2019) and 
     (extract(month from timestamp)=2)
  ) as a
group by pollutant 
order by city_count asc limit 1)

union distinct 

(select pollutant, count(distinct concat(city,"-",country)) as city_count, 'most' as label
from  
  (select pollutant, city, country, timestamp from `bigquery-public-data.openaq.global_air_quality` 
   where 
     (extract(year from timestamp)=2019) and 
     (extract(month from timestamp)=2)
  ) as a
group by pollutant 
order by city_count desc limit 1)

#------
#For each air pollutant, which are the cities that have the highest level of that pollutant in February 2019?
#------
with Feb2019data as(
  select pollutant, city, country, value, source_name
  from `bigquery-public-data.openaq.global_air_quality`
  where 
  (extract(year from timestamp) = 2019) and
  (extract(month  from timestamp) = 2)
),
pollutant_max as
(
  select pollutant, max(value) as max_value
  from Feb2019data
  group by pollutant
)
select a.pollutant, a.city, a.country, a.value, a.source_name
from Feb2019data as a, pollutant_max as b
where (a.pollutant = b.pollutant) and (a.value = b.max_value);


#------
#Find the cities with the highest and lowest level of each pollutant in India, Australia, Germany and US in February 2019.
#From this, I find out that Baden-Württemberg, the state where I study my Master's degree, has the highest level of CO and O3; 
#while Nordrhein-Westfalen, the state where I am living and doing my internship, has the lowest level of O3.          
#The reason is perhaps because BASF is headquartered in Ludwigshafen in Baden-Württemberg.
#------
with Feb2019 as
(
  select country, pollutant, city, value
  from `bigquery-public-data.openaq.global_air_quality`
  where
    (extract(year from timestamp) = 2019) and
    (extract(month from timestamp) = 2)
),
pollutant_max as
(
  select a.country, a.pollutant, max(a.value) as extremum, 'max' as label
  from Feb2019 as a
  group by a.country, a.pollutant
  having a.country  in  ('IN', 'AU', 'DE', 'US')
  order by a.country, a.pollutant
),
pollutant_min as 
(
  select a.country, a.pollutant, min(a.value) as extremum, 'min' as label
  from Feb2019 as a
  group by a.country, a.pollutant
  having a.country  in  ('IN', 'AU', 'DE', 'US')
  order by a.country, a.pollutant
),
unioned as
(
select * from pollutant_max
union all
select * from pollutant_min
)
select a.country, a.pollutant, a.city, a.value, b.label as extremum_label
from 
  Feb2019 as a,
  unioned as b
where
  (a.country = b.country and a.pollutant = b.pollutant and a.value  = b.extremum) 
order by a.country, a.pollutant;

