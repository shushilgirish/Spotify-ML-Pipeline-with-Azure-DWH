SELECT t.trend, COUNT(*) AS track_count
FROM dbo.fact_streams f
JOIN dbo.dim_trend t ON f.trend_id = t.trend_id
GROUP BY t.trend
ORDER BY track_count DESC;

--chart analysis
SELECT c.chart, SUM(f.streams) AS total_streams
FROM dbo.fact_streams f
JOIN dbo.dim_chart c ON f.chart_id = c.chart_id
GROUP BY c.chart
ORDER BY total_streams DESC;

--trend analysis
SELECT r.region, t.trend, COUNT(*) AS track_count
FROM dbo.fact_streams f
JOIN dbo.dim_trend t ON f.trend_id = t.trend_id
JOIN dbo.dim_region r ON f.region_id = r.region_id
GROUP BY r.region, t.trend
ORDER BY r.region, track_count DESC;

SELECT d.year, d.month, SUM(f.streams) AS total_streams
FROM dbo.fact_streams f
JOIN dbo.dim_artist a ON f.artist_id = a.artist_id
JOIN dbo.dim_date d ON f.date_id = d.date_id
WHERE a.artist = 'Ed Sheeran'
GROUP BY d.year, d.month
ORDER BY d.year, d.month;
--popularity trend
SELECT top 10 t.title, SUM(f.streams) AS total_streams
FROM dbo.fact_streams f
JOIN dbo.dim_track t ON f.track_id = t.track_id
JOIN dbo.dim_region r ON f.region_id = r.region_id
WHERE r.region = 'United States'
GROUP BY t.title
ORDER BY total_streams DESC
;
--Track Performance
SELECT top 10 t.title, SUM(f.streams) AS total_streams
FROM dbo.fact_streams f
JOIN dbo.dim_track t ON f.track_id = t.track_id
GROUP BY t.title
ORDER BY total_streams DESC
;
--Seasonal and Temporal Analysis
SELECT d.month, SUM(f.streams) AS total_streams
FROM dbo.fact_streams f
JOIN dbo.dim_date d ON f.date_id = d.date_id
GROUP BY d.month
ORDER BY total_streams DESC;

SELECT 
    CASE 
        WHEN DATEPART(WEEKDAY, d.date) IN (1, 7) THEN 'Weekend'
        ELSE 'Weekday'
    END AS day_type,
    SUM(f.streams) AS total_streams
FROM dbo.fact_streams f
JOIN dbo.dim_date d ON f.date_id = d.date_id
GROUP BY 
    CASE 
        WHEN DATEPART(WEEKDAY, d.date) IN (1, 7) THEN 'Weekend'
        ELSE 'Weekday'
    END
ORDER BY total_streams DESC;
--Long-Tail Analysis
WITH TotalStreams AS (
    SELECT t.title, SUM(f.streams) AS total_streams
    FROM dbo.fact_streams f
    JOIN dbo.dim_track t ON f.track_id = t.track_id
    GROUP BY t.title
),
RunningTotal AS (
    SELECT title, total_streams,
           SUM(total_streams) OVER (ORDER BY total_streams DESC) AS cumulative_streams,
           SUM(total_streams) OVER () AS grand_total
    FROM TotalStreams
)
SELECT title, total_streams, cumulative_streams, grand_total
FROM RunningTotal
WHERE cumulative_streams <= 0.8 * grand_total;


