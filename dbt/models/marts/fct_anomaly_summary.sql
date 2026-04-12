{{ config(tags=['adsb']) }}

with source as (
    select * from {{ source('silver', 'silver_anomalies') }}
),

 aggregated as (
    select
        detection_date,
        anomaly_type,
        anomaly_severity,
        count(*) as occurrence_count
    from source
    group by 1, 2, 3
),

final as (
    select
        *,
        max(case when anomaly_severity = 'CRITICAL' then true else false end) over (
            partition by detection_date
        ) as high_alert_day
    from aggregated
)

select * from final;