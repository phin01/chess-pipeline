WITH daily_performance AS (
    SELECT
        local_date,
        COUNT(*) AS num_attempts,
        SUM(
            CASE 
                WHEN is_passed THEN 1 
                ELSE 0 
            END) AS correct_attempts

    FROM {{ ref ('f_puzzle_attempts') }}
    GROUP BY local_date
),

cumulative_performance AS (
    SELECT
        local_date,
        num_attempts,
        correct_attempts,
        SUM(num_attempts) OVER (ORDER BY local_date) AS cumulative_attempts,
        SUM(correct_attempts) OVER (ORDER BY local_date) AS cumulative_correct_attempts
    FROM daily_performance
)

SELECT
    local_date,
    num_attempts,
    correct_attempts,
    cumulative_attempts,
    cumulative_correct_attempts,
    ROUND(cumulative_correct_attempts / NULLIF(cumulative_attempts, 0), 4) AS cumulative_success_rate

FROM cumulative_performance
ORDER BY local_date ASC

