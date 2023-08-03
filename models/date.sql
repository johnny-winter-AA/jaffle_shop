
/* query to specify date range */
WITH DateRange AS (
    SELECT
        CAST(CONCAT(YEAR(MIN(order_date)), '-01-01') AS DATE) AS StartDate, /* should always start on 1st January */
        CAST(CONCAT(YEAR(MAX(order_date)), '-12-31') AS DATE) AS EndDate /* should always end on 31st December*/
    FROM 
        {{ ref('orders') }}
),

/* query to generate dates between given start date and end date */
Calendar AS (
SELECT
    DATEADD(DAY, r.generated_number, dr.StartDate ) AS Date
FROM
     {{ ref('stg_numberseries10k') }} r
    CROSS JOIN DateRange dr
WHERE
    DATEADD(DAY, r.generated_number, dr.StartDate ) <= dr.EndDate
)

/* date table query */
SELECT
    c.Date,
    YEAR(c.Date) AS Year,
    CAST(CONCAT('Qtr ',DATEPART(QUARTER, c.Date)) AS VARCHAR(5)) AS Quarter,
    CAST(DATENAME(MONTH, c.Date) AS VARCHAR(10))AS Month,
    DAY(c.Date) AS Day,
    MONTH(c.Date) AS MonthNumber,
    DATEPART(WEEKDAY, c.Date) AS DayOfWeek,
    CAST(DATENAME(WEEKDAY, c.Date) AS VARCHAR(10)) AS DayOfWeekName,
    ROW_NUMBER() OVER (PARTITION BY YEAR(c.Date) ORDER BY c.Date) AS DayOfYear,
    DATEPART(WEEK, c.Date) AS Week,
    DATEPART(ISO_WEEK, c.Date) AS ISOWeek
FROM
    Calendar c
