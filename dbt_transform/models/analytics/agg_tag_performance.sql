WITH tag_performance AS (
    SELECT
        t1.tag_name,
        COUNT(DISTINCT t4.puzzle_id) AS num_puzzles,
        SUM(
            CASE 
                WHEN t4.is_passed THEN 1 
                ELSE 0 
            END) AS correct_attempts
        
    FROM {{ ref ('d_tags') }} t1
    JOIN {{ ref ('link_puzzle_tag') }} t2
        ON t1.tag_name = t2.puzzle_tag
    JOIN {{ ref ('d_puzzles') }} t3
        ON t2.puzzle_id = t3.puzzle_id
    JOIN {{ ref ('f_puzzle_attempts') }} t4
        ON t3.puzzle_id = t4.puzzle_id
    GROUP BY t1.tag_name
)

SELECT
    tag_name,
    num_puzzles,
    correct_attempts,
    ROUND(correct_attempts / NULLIF(num_puzzles, 0), 4) AS success_rate
FROM tag_performance
ORDER BY num_puzzles DESC