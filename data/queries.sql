SELECT
    CAST(year AS INTEGER) AS Year,
    city   AS City,
    country AS Country,
    season AS Season,
    notes  AS Notes
FROM olympics
WHERE
    year GLOB '[0-9][0-9][0-9][0-9]'           -- only 4 digit years
    AND CAST(year AS INTEGER) <= 2024          -- drop anything after Paris 2024
    AND season IN ('Summer', 'Winter')         -- keep only real Games
ORDER BY
    CAST(year AS INTEGER),
    CASE season WHEN 'Summer' THEN 0 ELSE 1 END,
    city;

