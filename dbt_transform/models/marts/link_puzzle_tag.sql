{{ config(cluster_by=["puzzle_id"]) }}

SELECT DISTINCT
  puzzle_id,
  CASE
    WHEN tag IS NULL OR TRIM(tag) = '' THEN 'No Tag'
    ELSE TRIM(tag)
  END AS puzzle_tag

FROM {{ ref('int_chess_puzzles') }},

UNNEST(SPLIT(labeled_tags, ',')) AS tag