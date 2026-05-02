SELECT DISTINCT
  TRIM(tag) AS tag_name

FROM {{ ref('int_chess_puzzles') }},

UNNEST(SPLIT(labeled_tags, ',')) AS tag
WHERE tag IS NOT NULL AND tag != ''