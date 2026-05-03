{{ config(cluster_by=["puzzle_id"]) }}

select 
    puzzle_id,
    average_time,
    correct_move_count,
    fen,
    puzzle_rating,
    target_time,
    new_rating_system,
    CONCAT(FLOOR(puzzle_rating/100) * 100, '-', FLOOR(puzzle_rating/100) * 100 + 100) as puzzle_range
from {{ ref('int_chess_puzzles') }}