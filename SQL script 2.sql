-- Insert into dim_artist
INSERT INTO dbo.dim_artist (artist)
SELECT DISTINCT artist
FROM dbo.spotify_stagging;

-- Insert into dim_track
INSERT INTO dbo.dim_track (track_id, title, url)
SELECT DISTINCT track_id, title, url
FROM dbo.spotify_stagging;

-- Insert into dim_date
INSERT INTO dbo.dim_date (date, year, month, day)
SELECT DISTINCT 
    date,
    YEAR(date) AS year,
    MONTH(date) AS month,
    DAY(date) AS day
FROM dbo.spotify_stagging;

-- Insert into dim_region
INSERT INTO dbo.dim_region (region)
SELECT DISTINCT region
FROM dbo.spotify_stagging;

-- Populate dim_trend
INSERT INTO dbo.dim_trend (trend)
SELECT DISTINCT trend
FROM dbo.spotify_stagging;
-- Populate dim_chart
INSERT INTO dbo.dim_chart (chart)
SELECT DISTINCT chart
FROM dbo.spotify_stagging;

-- Add trend and chart relationships during population
INSERT INTO dbo.fact_streams (track_id, artist_id, region_id, date_id, trend_id, chart_id, streams, rank)
SELECT 
    st.track_id,
    da.artist_id,
    dr.region_id,
    dd.date_id,
    dt.trend_id,
    dc.chart_id,
    st.streams,
    st.rank
FROM dbo.spotify_stagging st
JOIN dbo.dim_artist da ON st.artist = da.artist
JOIN dbo.dim_region dr ON st.region = dr.region
JOIN dbo.dim_date dd ON st.date = dd.date
JOIN dbo.dim_trend dt ON st.trend = dt.trend
JOIN dbo.dim_chart dc ON st.chart = dc.chart;