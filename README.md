# SQL_Spotify_data_analysis
## Objective
Spotify wants to enhance user engagement by optimizing shuffle mode and improving track completion rates. To achieve this, they need to understand how shuffle mode affects listening behavior, identify patterns in track interruptions, and explore platform specific performance trends.

## Task:
My responsibility as an analyst is to examine the dataset and respond to the following particular business inquiries:\
      1.The Spotify history table's data was extracted, and the csv file was loaded into MySQL.\
      2.Duplicate data was eliminated, and null and empty spaces were replaced.\
```
WITH cte AS (
    SELECT 
        *, 
        ROW_NUMBER() OVER (
            PARTITION BY spotify_track_uri, ts, platform, ms_played, 
                         track_name, artist_name, album_name, 
                         reason_start, reason_end, skipped, shuffle 
            ORDER BY ts
        ) AS rn
    FROM spotify_history_staging
)
DELETE FROM spotify_history_staging 
WHERE spotify_track_uri IN (SELECT spotify_track_uri FROM cte WHERE rn > 1);
```
    

### 1.Business Questions
Impact of shuffle mode on listening behavior:\
1.1Do users play a more diverse range of tracks when shuffle mode is enabled?\
```
select shuffle, count(distinct track_name) as distinct_tracks
from spotify_history_staging
group by  shuffle;
```
1.2What percentage of tracks played in shuffle mode are interrupted (reason_end)?\
```
with cte1 as(
select count(*) as total_tracks,
sum(case when reason_end!='trackdone' then 1 else 0 end) as interrupted_count
from spotify_history_staging
where shuffle='TRUE')
select total_tracks,interrupted_count,round(interrupted_count*100/total_tracks,2) as interruption_percentage 
from cte1;

```
1.3Which platforms have the highest shuffle mode usage?
```
select platform, count(*) as total_tracks
from spotify_history_staging
where shuffle='TRUE'
group by platform
order by 2 desc; 
```

### 2.Track completion rates:
2.1 What percentage of tracks are stopped early versus completed?\
```
with cte1 as
(select count(*) as total_track, 
sum(case when reason_end='trackdone' then 1 else 0 end) as completed_track,
sum(case when reason_end!='trackdone' then 1 else 0 end) as stopped_track
from spotify_history_staging)
select total_track, completed_track,stopped_track,
round(completed_track*100/total_track,2) as percentage_completion,
round(stopped_track*100/total_track,2) as stopped_percentage
from cte1;

```
2.2 Are there specific tracks or artists with consistently high interruption rates?\
```
WITH track_stats AS (
    SELECT 
        track_name, 
        artist_name, 
        COUNT(*) AS total_tracks,
        SUM(CASE WHEN reason_end != 'trackdone' THEN 1 ELSE 0 END) AS interrupted_count
    FROM spotify_history_staging
    GROUP BY track_name, artist_name
)
SELECT 
    track_name, 
    artist_name, 
    total_tracks, 
    interrupted_count,
    ROUND(interrupted_count * 100 / NULLIF(total_tracks, 0), 2) AS interruption_percentage
FROM track_stats
ORDER BY interrupted_count DESC
LIMIT 10;

```
2.3 Does the platform or shuffle mode influence track completion rates?
```
with cte1 as (
select platform,shuffle, 
count(*) as total_track, 
sum(case when reason_end='trackdone' then 1 else 0 end) as completed_track,
sum(case when reason_end!='trackdone' then 1 else 0 end) as stopped_track
from spotify_history_staging
group by platform,shuffle )
select *,round(completed_track*100/total_track,2) as completion_rate 
from cte1
order by completion_rate desc;
```

### 3.Platform usage trends:
3.1 Which platforms have the longest average playback duration?\
```
select platform, round(avg(ms_played/60000),2) as avg_play_time
from spotify_history_staging
group by platform
order by 2 desc;
```
3.2 Are there specific hours or days where platform usage peaks?
```
select hour(ts) as hrs,count(*) as total 
from spotify_history_staging group by 
hrs order by total desc;

```

### 4.Timestamp based insights:
4.1 What are the most popular hours for streaming across different platforms?\
```
SELECT platform,hour(ts) as hour,count(*) as total
FROM spotify_history_staging
group by 1,2
order by total desc;
```
4.2 Which tracks are most frequently played during peak hours?
```
select track_name,hour(ts) as hr,count(*) as total
FROM spotify_history_staging
where hour(ts)=(select hour(ts) as hour 
FROM spotify_history_staging 
group by hour(ts)
order by count(*) desc limit 1 )
group by 1,2 
```

## Deliverables
### Key Metrics:
•	When shuffle function is turned off, users play a wider variety of songs.\
•	 The longest average playback duration (3:57 minutes) is found among Mac users.\
•	The peak streaming activity occurs at midnight (0 hours) (10,446 plays on Android).\
•	The songs that are played the most during peak hours: The First Youth, The Boxer.

### Key Learnings
Queries showcasing the use of JOINs to combine platform, timestamp, and track data
CTEs to simplify calculations, such as track completion rates and shuffle mode diversity
