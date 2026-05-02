select 
    puzzle_id,
    average_time,
    correct_move_count,
    fen,
    puzzle_rating,
    target_time,
    new_rating_system
    
from {{ ref('int_chess_puzzles') }}