SELECT dist.*, maxx.color FROM (
SELECT SUM(c) as s, ROUND(MAX(c)/SUM(c)*100, 2) as m, creatureType FROM
(
    SELECT CAST(COUNT(*) AS FLOAT) as c, creatureType, color FROM
    (
        SELECT name, flatten(subtypes) as creatureType, flatten(colors) as color
              FROM `mtg`.`root`.`AllCardsCutted.json`
              WHERE REPEATED_CONTAINS(types, 'Creature')
    )
    GROUP BY creatureType, color
) GROUP BY creatureType
) AS dist JOIN (
    SELECT m.creatureType, color FROM (
    SELECT MAX(c) as m, creatureType FROM (
        SELECT CAST(COUNT(*) AS FLOAT) as c, creatureType, color FROM
        (
            SELECT name, flatten(subtypes) as creatureType, flatten(colors) as color
                  FROM `mtg`.`root`.`AllCardsCutted.json`
                  WHERE REPEATED_CONTAINS(types, 'Creature')
        )
        GROUP BY creatureType, color
        ) GROUP BY creatureType
    ) AS m JOIN (
    SELECT COUNT(*) as c, creatureType, color FROM
        (
            SELECT name, flatten(subtypes) as creatureType, flatten(colors) as color
                  FROM `mtg`.`root`.`AllCardsCutted.json`
                  WHERE REPEATED_CONTAINS(types, 'Creature')
        )
        GROUP BY creatureType, color
    ) AS c
    ON c.creatureType = m.creatureType and m.m = c.c
) as maxx
ON maxx.creatureType = dist.creatureType
ORDER BY dist.m, creatureType DESC