{{ config(
    materialized='incremental',
    unique_key='puzzle_id',
    partition_by={
        "field": "local_date",
        "data_type": "date"
    },
    cluster_by=["puzzle_id"]
) }}


select 
    puzzle_id,
    date_utc,
    local_date,
    local_time,
    day_of_week,
    is_passed,
    move_count,
    my_rating,
    my_time,
    rating_change,
    score,
    ingested_at
    
from {{ ref('int_chess_puzzles') }}

{% if is_incremental() %}
WHERE ingested_at > (SELECT MAX(ingested_at) FROM {{ this }})
{% endif %}