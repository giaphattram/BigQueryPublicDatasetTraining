
#The number of sessions for each channel on 2017-08-01

SELECT channelGrouping, count(distinct concat(cast(a.visitId as STRING), "-", a.fullVisitorId)) session_count
FROM [bigquery-public-data:google_analytics_sample.ga_sessions_20170801] as a
group by channelGrouping
order by session_count;
