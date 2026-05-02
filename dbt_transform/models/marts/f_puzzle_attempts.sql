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