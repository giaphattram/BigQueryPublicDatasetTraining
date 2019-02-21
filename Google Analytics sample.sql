
#The number of sessions for each channel on 2017-08-01

SELECT channelGrouping, count(distinct concat(cast(visitId as STRING), "-", fullVisitorId)) session_count
FROM [bigquery-public-data:google_analytics_sample.ga_sessions_20170801]
group by channelGrouping
order by session_count;

#The number of sessions from different browsers on 2017-08-01
SELECT device.browser , count(distinct concat(cast(visitId as STRING), "-", fullVisitorId)) session_count
FROM [bigquery-public-data:google_analytics_sample.ga_sessions_20170801]
group by device.browser 
order by session_count;
