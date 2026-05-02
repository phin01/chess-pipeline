select
    puzzle_id,

    -- puzzle specific info
    average_time,
    correct_move_count,
    fen,
    labeled_tags,
    rating as puzzle_rating,
    target_time,
    CASE
        WHEN date_utc <= '2025-10-08' THEN false
        ELSE true
    END AS new_rating_system,

    -- puzzle attempt specific info
    date_utc,
    {{ local_date('date_utc') }} AS local_date,
    {{ local_time('date_utc') }} AS local_time,
    {{ local_day_of_week('date_utc') }} AS day_of_week,
    is_passed,
    move_count,
    my_rating,
    my_time,
    rating_change,
    score,
    ingested_at

from {{ ref('stg_chess_puzzles__expanded') }}