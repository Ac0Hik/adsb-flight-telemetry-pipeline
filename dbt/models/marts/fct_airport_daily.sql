{{ config(tags=['adsb']) }}

with source as (
    select * from {{ source('gold', 'daily_airport_stats') }}
),

final as (
    select
        *,
        AVG(total_movements) OVER (
            PARTITION BY airport_icao
            ORDER BY flight_date
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) as rolling_avg,
        
        total_movements - LAG(total_movements) OVER (
            PARTITION BY airport_icao
            ORDER BY flight_date
        ) as day_over_day_change

    from source
)

select * from final


-- Questions this model answers:

-- Is this airport getting busier or quieter over time?
-- Was today an anomaly compared to recent trends? (e.g. sudden spike or drop in movements)
-- Which airports are consistently high traffic vs seasonal?
-- Did a specific event  impact airport activity?