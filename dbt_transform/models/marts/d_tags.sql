SELECT DISTINCT
  puzzle_tag AS tag_name

FROM {{ ref('link_puzzle_tag') }}