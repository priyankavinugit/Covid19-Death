create database covid;--Creating Database
use covid;

--Table Creation

CREATE TABLE covid_data ( 
    data_as_of DATE,
    Jurisdiction_Residence VARCHAR(255),
    [Group] VARCHAR(100), 
    data_period_start DATE,
    data_period_end DATE,
    COVID_deaths FLOAT,
    COVID_pct_of_total FLOAT,
    pct_change_wk FLOAT,
    pct_diff_wk FLOAT,
    crude_COVID_rate FLOAT,
    aa_COVID_rate FLOAT,
    footnote TEXT
);

--Load the dataset

BULK INSERT covid_data
FROM 'C:\Users\VinoSekar-VP\Desktop\priya data analyst\SQL Project Placement\DA_Data.csv'
WITH (
    FIRSTROW = 2,  -- Skip the header row
    FIELDTERMINATOR = ',', 
    ROWTERMINATOR = '\n', 
    TABLOCK
);

--Checking the entire dataset

select * from covid_data;

--Handling missing values

UPDATE covid_data 
SET COVID_deaths = COALESCE(COVID_deaths, 0),
    COVID_pct_of_total = COALESCE(COVID_pct_of_total, 0),
    pct_change_wk = COALESCE(pct_change_wk, 0),
    pct_diff_wk = COALESCE(pct_diff_wk, 0),
    crude_COVID_rate = COALESCE(crude_COVID_rate, 0),
    aa_COVID_rate = COALESCE(aa_COVID_rate, 0);

--Drop the unnecessary columns

ALTER TABLE covid_data DROP COLUMN footnote;

--Trim Spaces and Standardize Text Case

UPDATE covid_data 
SET Jurisdiction_Residence = TRIM(Jurisdiction_Residence),
    [Group] = TRIM([Group]);

UPDATE covid_data
SET Jurisdiction_Residence = UPPER(LEFT(Jurisdiction_Residence, 1)) + LOWER(SUBSTRING(Jurisdiction_Residence, 2, LEN(Jurisdiction_Residence))),
    [Group] = UPPER(LEFT([Group], 1)) + LOWER(SUBSTRING([Group], 2, LEN([Group])));


SELECT DISTINCT Jurisdiction_Residence, [Group] FROM covid_data;

--Checking Duplicates

SELECT Jurisdiction_Residence, [Group], data_period_start, data_period_end, COUNT(*) 
FROM covid_data 
GROUP BY Jurisdiction_Residence, [Group], data_period_start, data_period_end
HAVING COUNT(*) > 1;

select * from covid_data;

select count(*) from covid_data;

--Check memory allocation
EXEC sp_configure 'show advanced options', 1;
RECONFIGURE;
EXEC sp_configure 'max server memory (MB)';

-- Set memory to 10 GB
EXEC sp_configure 'max server memory (MB)', 10240;
RECONFIGURE;

--Verify memory settings
EXEC sp_configure 'max server memory (MB)';


--Retrieve the jurisdiction residence with the highest number of COVID deaths for the latest  data period end date.

WITH LatestData AS (
    SELECT  
        Jurisdiction_Residence, 
        Covid_Deaths, 
        data_period_end,
        RANK() OVER (ORDER BY data_period_end DESC) AS DateRank
    FROM covid_data
)
SELECT TOP 10 Jurisdiction_Residence, Covid_Deaths, data_period_end
FROM LatestData
WHERE DateRank = 1
ORDER BY Covid_Deaths DESC;

--Retrieve the top 5 jurisdictions with the highest percentage difference in aa_COVID_rate  compared to the overall crude COVID rate for the latest data period end date.

WITH LatestData AS (
    SELECT 
        Jurisdiction_residence, 
        aa_covid_rate, 
        crude_covid_rate, 
        pct_diff_wk, 
        data_period_end,
        RANK() OVER (ORDER BY data_period_end DESC) AS DateRank
    FROM covid_data
)
SELECT TOP 5 
    Jurisdiction_residence, 
    aa_covid_rate, 
    crude_covid_rate, 
    pct_diff_wk, 
    data_period_end
FROM LatestData
WHERE DateRank = 1
ORDER BY pct_diff_wk DESC;

--Calculate the average COVID deaths per week for each jurisdiction residence and group, for  the latest 4 data period end dates.

WITH LatestPeriods AS (
    -- Get the latest 4 data period end dates
    SELECT DISTINCT TOP 4 data_period_end
    FROM covid_data
    ORDER BY data_period_end DESC
),
FilteredData AS (
    -- Select data for only the latest 4 periods
    SELECT 
        Jurisdiction_residence, 
        [group], 
        covid_deaths, 
        data_period_end
    FROM covid_data
    WHERE data_period_end IN (SELECT data_period_end FROM LatestPeriods)
)
SELECT 
    Jurisdiction_residence, 
    [group], 
    AVG(covid_deaths) AS avg_weekly_covid_deaths
FROM FilteredData
GROUP BY Jurisdiction_residence, [group];

--Retrieve the data for the latest data period end date, but exclude any jurisdictions that had  zero COVID deaths and have missing values in any other column.

WITH LatestData AS (
    -- Get the latest data_period_end
    SELECT TOP 1 data_period_end
    FROM covid_data
    ORDER BY data_period_end DESC
)
SELECT *
FROM covid_data
WHERE data_period_end = (SELECT data_period_end FROM LatestData)
AND covid_deaths > 0 -- Exclude zero COVID deaths
AND Jurisdiction_residence IS NOT NULL 
AND [group] IS NOT NULL
AND covid_pct_of_total IS NOT NULL
AND pct_change_wk IS NOT NULL
AND pct_diff_wk IS NOT NULL
AND crude_covid_rate IS NOT NULL
AND aa_covid_rate IS NOT NULL;

--Calculate the week-over-week percentage change in COVID_pct_of_total for all jurisdictions  and groups, but only for the data period start dates after March 1, 2020.

WITH RankedData AS (
    SELECT 
        Jurisdiction_residence, 
        [group], 
        data_period_start, 
        data_period_end, 
        covid_pct_of_total,
        LAG(covid_pct_of_total) OVER (
            PARTITION BY Jurisdiction_residence, [group] 
            ORDER BY data_period_start ASC
        ) AS PrevWeek_pct_of_total
    FROM covid_data
    WHERE data_period_start > '2020-01-01'
)
SELECT 
    Jurisdiction_residence, 
    [group], 
    data_period_start, 
    data_period_end, 
    covid_pct_of_total, 
    PrevWeek_pct_of_total,
    CASE 
        WHEN PrevWeek_pct_of_total IS NOT NULL AND PrevWeek_pct_of_total != 0 
        THEN ((covid_pct_of_total - PrevWeek_pct_of_total) / PrevWeek_pct_of_total) * 100
        ELSE NULL
    END AS pct_change_wk
FROM RankedData;


--Group the data by jurisdiction residence and calculate the cumulative COVID deaths for each  jurisdiction, but only up to the latest data period end date.

WITH LatestDate AS (
    -- Get the latest data_period_end
    SELECT TOP 1 data_period_end
    FROM covid_data
    ORDER BY data_period_end DESC
),
CumulativeDeaths AS (
    SELECT 
        Jurisdiction_residence, 
        data_period_end, 
        SUM(covid_deaths) OVER (
            PARTITION BY Jurisdiction_residence 
            ORDER BY data_period_end ASC 
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS cumulative_covid_deaths
    FROM covid_data
    WHERE data_period_end <= (SELECT data_period_end FROM LatestDate)
)
SELECT Jurisdiction_residence, MAX(cumulative_covid_deaths) AS cumulative_covid_deaths
FROM CumulativeDeaths
GROUP BY Jurisdiction_residence;


--Implementation of Function & Procedure-"Create a stored procedure that takes in a date  range and calculates the average weekly percentage change in COVID deaths for each  jurisdiction. The procedure should return the average weekly percentage change along with  the jurisdiction and date range as output. Additionally, create a user-defined function that  takes in a jurisdiction as input and returns the average crude COVID rate for that jurisdiction  over the entire dataset. Use both the stored procedure and the user-defined function to  compare the average weekly percentage change in COVID deaths for each jurisdiction to the  average crude COVID rate for that jurisdiction.

--User Defined Function

CREATE FUNCTION dbo.GetAvgCrudeCovidRate (@Jurisdiction NVARCHAR(255))
RETURNS FLOAT
AS
BEGIN
    DECLARE @AvgCrudeRate FLOAT;

    SELECT @AvgCrudeRate = AVG(crude_covid_rate)
    FROM covid_data
    WHERE Jurisdiction_residence = @Jurisdiction;

    RETURN @AvgCrudeRate;
END;

--Stored Procedure

CREATE PROCEDURE dbo.GetAvgWeeklyPctChangeInCovidDeaths
    @StartDate DATE,
    @EndDate DATE
AS
BEGIN
    SET NOCOUNT ON;

    WITH RankedData AS (
        SELECT 
            Jurisdiction_residence, 
            data_period_start, 
            covid_deaths,
            LAG(covid_deaths) OVER (
                PARTITION BY Jurisdiction_residence 
                ORDER BY data_period_start ASC
            ) AS PrevWeekDeaths
        FROM covid_data
        WHERE data_period_start BETWEEN @StartDate AND @EndDate
    )
    SELECT 
        Jurisdiction_residence, 
        @StartDate AS StartDate,
        @EndDate AS EndDate,
        AVG(CASE 
                WHEN PrevWeekDeaths IS NOT NULL AND PrevWeekDeaths > 0 
                THEN ((covid_deaths - PrevWeekDeaths) / PrevWeekDeaths) * 100
                ELSE NULL 
            END) AS AvgWeeklyPctChange
    FROM RankedData
    GROUP BY Jurisdiction_residence;
END;

--Compare Procedure & Function

DECLARE @StartDate DATE = '2020-01-01';  
DECLARE @EndDate DATE = '2023-11-03';

-- Store procedure results in a temporary table
DECLARE @Results TABLE (
    Jurisdiction_residence NVARCHAR(255),
    StartDate DATE,
    EndDate DATE,
    AvgWeeklyPctChange FLOAT
);

INSERT INTO @Results
EXEC dbo.GetAvgWeeklyPctChangeInCovidDeaths @StartDate, @EndDate;

-- Compare with function results
SELECT 
    r.Jurisdiction_residence, 
    r.StartDate, 
    r.EndDate, 
    r.AvgWeeklyPctChange, 
    dbo.GetAvgCrudeCovidRate(r.Jurisdiction_residence) AS AvgCrudeCovidRate,
    CASE 
        WHEN r.AvgWeeklyPctChange > dbo.GetAvgCrudeCovidRate(r.Jurisdiction_residence) 
        THEN 'Higher'
        WHEN r.AvgWeeklyPctChange < dbo.GetAvgCrudeCovidRate(r.Jurisdiction_residence) 
        THEN 'Lower'
        ELSE 'Equal'
    END AS ComparisonResult
FROM @Results r;
