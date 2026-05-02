WITH dow_performance AS (
    SELECT
        day_of_week,
        COUNT(*) AS num_attempts,
        SUM(
            CASE 
                WHEN is_passed THEN 1 
                ELSE 0 
            END) AS correct_attempts

    FROM {{ ref ('f_puzzle_attempts') }}
    GROUP BY day_of_week
)

SELECT
    day_of_week,
    num_attempts,
    correct_attempts,
    ROUND(correct_attempts / NULLIF(num_attempts, 0), 4) AS success_rate
FROM dow_performance
ORDER BY day_of_week ASC