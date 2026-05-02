select 
    puzzle_id,
    {{ time_to_seconds('JSON_VALUE(payload, "$.average_time")') }} as average_time,
    JSON_VALUE(payload, '$.code') as code,
    CAST(JSON_VALUE(payload, '$.correct_move_count') AS INT64) as correct_move_count,
    TIMESTAMP(JSON_VALUE(payload, '$.date')) as date_utc,
    JSON_VALUE(payload, '$.fen') as fen,
    CAST(JSON_VALUE(payload, '$.flip_board') AS BOOLEAN) as flip_board,
    CAST(JSON_VALUE(payload, '$.is_passed') AS BOOLEAN) as is_passed,
    CAST(JSON_VALUE(payload, '$.is_provisional') AS BOOLEAN) as is_provisional,
    JSON_VALUE(payload, '$.labeled_tags') as labeled_tags,
    CAST(JSON_VALUE(payload, '$.move_count') AS INT64) as move_count,
    CAST(JSON_VALUE(payload, '$.my_rating') AS INT64) as my_rating,
    CAST(JSON_VALUE(payload, '$.my_time') AS INT64) as my_time,
    JSON_VALUE(payload, '$.outcome') as outcome,
    CAST(JSON_VALUE(payload, '$.rating') AS INT64) as rating,
    CAST(JSON_VALUE(payload, '$.rating_change') AS INT64) as rating_change,
    CAST(JSON_VALUE(payload, '$.score') AS FLOAT64) as score,
    CAST(JSON_VALUE(payload, '$.target_time') AS INT64) as target_time,
    ingested_at

from {{ ref('stg_chess_puzzles__payload') }}