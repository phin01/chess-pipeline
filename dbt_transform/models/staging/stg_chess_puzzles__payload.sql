with deduplicated_puzzles as (
    select 
    puzzle_id,
    payload,
    ingested_at,
    ROW_NUMBER() OVER (PARTITION BY puzzle_id) as row_num
    
    from {{ source('chess_puzzles', 'raw_chess_puzzles') }}
)

select
    puzzle_id,
    payload,
    ingested_at
    from deduplicated_puzzles
where row_num = 1

