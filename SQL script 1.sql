DROP TABLE dbo.spotify_stagging;

CREATE TABLE dbo.spotify_stagging (
    title NVARCHAR(255),
    rank INT,
    date DATE,
    artist NVARCHAR(255),
    url NVARCHAR(500),
    region NVARCHAR(50),
    chart NVARCHAR(100),
    trend NVARCHAR(50),
    streams BIGINT,
    track_id VARCHAR(255) -- Changed to VARCHAR for compatibility
)
WITH (
    DISTRIBUTION = ROUND_ROBIN,
    HEAP
);
--artists
CREATE TABLE dbo.dim_artist (
    artist_id INT IDENTITY(1, 1),
    artist NVARCHAR(255)
)
WITH (
    DISTRIBUTION = REPLICATE,
    HEAP
);

CREATE CLUSTERED INDEX CI_dim_artist ON dbo.dim_artist (artist_id);
--dim track
CREATE TABLE dbo.dim_track (
    track_id VARCHAR(255),
    title NVARCHAR(255),
    url NVARCHAR(500)
)
WITH (
    DISTRIBUTION = REPLICATE,
    HEAP
);

CREATE CLUSTERED INDEX CI_dim_track ON dbo.dim_track (track_id);

--dim date
CREATE TABLE dbo.dim_date (
    date_id INT IDENTITY(1, 1),
    date DATE,
    year INT,
    month INT,
    day INT
)
WITH (
    DISTRIBUTION = REPLICATE,
    HEAP
);

CREATE CLUSTERED INDEX CI_dim_date ON dbo.dim_date (date_id);

--dim region
CREATE TABLE dbo.dim_region (
    region_id INT IDENTITY(1, 1),
    region NVARCHAR(50)
)
WITH (
    DISTRIBUTION = REPLICATE,
    HEAP
);

CREATE CLUSTERED INDEX CI_dim_region ON dbo.dim_region (region_id);

CREATE TABLE dbo.dim_trend (
    trend_id INT IDENTITY(1, 1),
    trend NVARCHAR(50)
)
WITH (
    DISTRIBUTION = REPLICATE,
    HEAP
);
CREATE CLUSTERED INDEX CI_dim_trend ON dbo.dim_trend (trend_id);

CREATE TABLE dbo.dim_chart (
    chart_id INT IDENTITY(1, 1),
    chart NVARCHAR(50)
)
WITH (
    DISTRIBUTION = REPLICATE,
    HEAP
);

CREATE CLUSTERED INDEX CI_dim_chart ON dbo.dim_chart (chart_id);

--fact table

CREATE TABLE dbo.fact_streams (
    fact_id INT IDENTITY(1, 1),
    track_id VARCHAR(255),  -- FK reference to dim_track
    artist_id INT,          -- FK reference to dim_artist
    region_id INT,          -- FK reference to dim_region
    date_id INT,            -- FK reference to dim_date
    trend_id INT, -- FK reference to dim_trend
    chart_id INT, -- FK reference to dim_chart
    streams BIGINT,
    rank INT
)WITH (
    DISTRIBUTION = HASH(track_id),
    HEAP
);

CREATE CLUSTERED COLUMNSTORE INDEX CCI_fact_streams ON dbo.fact_streams;



