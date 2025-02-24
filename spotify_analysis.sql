SHOW VARIABLES LIKE 'local_infile';
SET GLOBAL local_infile = 1;

CREATE TABLE spotify_history_staging AS SELECT * FROM spotify_history;
select count(*) from spotify_history_staging;

-- Identifying duplicates 

WITH cte AS (
    SELECT 
        spotify_track_uri, 
        ROW_NUMBER() OVER (
            PARTITION BY spotify_track_uri, ts, platform, ms_played, 
                         track_name, artist_name, album_name, 
                         reason_start, reason_end, skipped, shuffle 
            ORDER BY ts  
        ) AS rn
    FROM spotify_history_staging
)
SELECT spotify_track_uri 
FROM cte 
WHERE rn > 1;

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

-- identifying start timing and converting millisecond played to minutes played.

select * from spotify_history_staging limit 10;

SELECT DATE_FORMAT(ts, '%H:%i:%s') AS extracted_time FROM spotify_history;


SELECT ts, DATE_SUB(ts, INTERVAL ms_played/60000 Minute) AS start_time, round(ms_played/1000,2) as seconds_played,round(ms_played/60000,2) as minute_played
FROM spotify_history_staging
order by ts;

select spotify_track_uri,
ts as stop_time,
round(ms_played/60000,2) as minute_played,
ms_played,
DATE_SUB(ts, INTERVAL ms_played/60000 Minute) AS start_time,
track_name,
Platform,
artist_name , 
album_name, 
reason_start, 
reason_end, 
shuffle 
from 
spotify_history_staging
Limit 5;

-- identifying null and empty values

select spotify_track_uri,ts,
reason_start,reason_end ,shuffle ,skipped  from 
spotify_history_staging where spotify_track_uri IS NULL OR 
ts IS NULL OR platform IS NULL OR 
track_name IS NULL or track_name='' or
artist_name IS NULL OR artist_name='' or
album_name IS NULL OR album_name='' or
reason_start IS NULL OR reason_start='' or
reason_end IS NULL OR reason_end='' or
shuffle IS NULL OR shuffle='' or
skipped IS NULL Or skipped='';

-- Updating null or empty values 
update spotify_history_staging
SET reason_start=
case when reason_start is null or reason_start='' then 'unknown' else reason_start end,
reason_end =case when reason_end is null or reason_end='' then 'unknown' else reason_end end;

SET SQL_SAFE_UPDATES = 0;

-- do users play a amore diverse range of tracks when shuffle mode is enabled 

select shuffle, count(distinct track_name) as distinct_tracks
from spotify_history_staging
group by  shuffle;

-- what percentage of tracks played in shuffle mode are interrupted (reason_end)

with cte1 as(
select count(*) as total_tracks,
sum(case when reason_end!='trackdone' then 1 else 0 end) as interrupted_count
from spotify_history_staging
where shuffle='TRUE')
select total_tracks,interrupted_count,round(interrupted_count*100/total_tracks,2) as interruption_percentage 
from cte1;


-- Which platforms have the highest shuffle mode usage?
select distinct shuffle from spotify_history_staging;

select platform, count(*) as total_tracks
from spotify_history_staging
where shuffle='TRUE'
group by platform
order by 2 desc; 

-- Track completion rates:
-- What percentage of tracks are stopped early versus completed?
select distinct reason_start from spotify_history_staging;
Select distinct reason_end from spotify_history_staging;

with cte1 as
(select count(*) as total_track, 
sum(case when reason_end='trackdone' then 1 else 0 end) as completed_track,
sum(case when reason_end!='trackdone' then 1 else 0 end) as stopped_track
from spotify_history_staging)
select total_track, completed_track,stopped_track,
round(completed_track*100/total_track,2) as percentage_completion,
round(stopped_track*100/total_track,2) as stopped_percentage
from cte1;

-- Are there specific tracks or artists with consistently high interruption rates?
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

-- Does the platform or shuffle mode influence track completion rates?
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

-- Platform usage trends:
-- Which platforms have the longest average playback duration?
select platform, round(avg(ms_played/60000),2) as avg_play_time
from spotify_history_staging
group by platform
order by 2 desc;

-- Are there specific hours or days where platform usage peaks?
-- DATE_FORMAT(ts, '%H')this also gives the same result

select hour(ts) as hrs,count(*) as total 
from spotify_history_staging group by 
hrs order by total desc;

-- Timestamp based insights:
-- What are the most popular hours for streaming across different platforms?
SELECT platform,hour(ts) as hour,count(*) as total
FROM spotify_history_staging
group by 1,2
order by total desc;

-- Which tracks are most frequently played during peak hours?
select track_name,hour(ts) as hr,count(*) as total
FROM spotify_history_staging
where hour(ts)=(select hour(ts) as hour 
FROM spotify_history_staging 
group by hour(ts)
order by count(*) desc limit 1 )
group by 1,2 
order by total desc;
