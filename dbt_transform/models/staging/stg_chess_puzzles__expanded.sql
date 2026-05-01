select 
    puzzle_id,
    {{ time_to_seconds('JSON_VALUE(payload, "$.average_time")') }} as average_time,
    payload.code,
    payload.correct_move_count,
    TIMESTAMP(JSON_VALUE(payload, '$.date')) as date_utc,
    payload.fen,
    payload.flip_board,
    payload.is_passed,
    payload.is_provisional,
    payload.labeled_tags,
    payload.move_count, 
    payload.my_rating,
    payload.my_time,
    payload.outcome,
    payload.rating,
    payload.rating_change,
    payload.score,
    payload.target_time,
    ingested_at

from {{ ref('stg_chess_puzzles__payload') }}