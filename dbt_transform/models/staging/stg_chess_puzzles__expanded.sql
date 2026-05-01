select 
    puzzle_id,
    payload.average_time,
    payload.code,
    payload.correct_move_count,
    payload.date,
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

where 1=1
and puzzle_id in ('987224', '1552226')