-- Comparer les notes moyeennes des business avec ou sans prise en charge de la CB 
SELECT 
    ROUND("average_stars"),
    COUNT(CASE WHEN Service."BusinessAcceptsCreditCards" = 1 THEN 1 END) AS AcceptCreditCards,
    COUNT(CASE WHEN Service."BusinessAcceptsCreditCards" = 0 THEN 1 END) AS NotAcceptCreditCards
FROM 
    TENDENCY 
JOIN 
    SERVICE ON SERVICE."service_id" = TENDENCY."service_id" 
GROUP BY 
    ROUND("average_stars")
ORDER BY 
    ROUND("average_stars")

-- Quels catégories de commerce sont les mieux notés ?
-- /!\ à modifier car inexploitable sur metabase (beaucoup trop de données) | peut-être groupé plutôt par service ? idk

SELECT 
    CATEGORY."category_name",
    ROUND(AVG(TENDENCY."average_stars")) AS "average_rating"
FROM TENDENCY
INNER JOIN 
    BUSINESS ON BUSINESS."business_id" = TENDENCY."business_id" 
INNER JOIN 
    BUSINESS_CATEGORY ON BUSINESS."business_id" = BUSINESS_CATEGORY."business_id"
INNER JOIN 
    CATEGORY ON BUSINESS_CATEGORY."category_id" = CATEGORY."category_id"
GROUP BY 
    CATEGORY."category_name"
ORDER BY 
    "average_rating" DESC

-- Quels catégories de commerce sont les mieux notés ?
-- Idem pas exploitable car trop de données

SELECT 
	distinct ROUND(t."average_stars"),
	distinct t."review_count"
FROM TENDENCY t
INNER JOIN BUSINESS b ON b."business_id" = t."business_id"
WHERE t.category_id = {{category_id}}
ORDER BY ROUND(t."average_stars")

-- CARTE : note moyenne par état
SELECT 
    b."state",
    ROUND(AVG(t."average_stars"),1) AS "avg_rounded_stars"
FROM 
    TENDENCY t
INNER JOIN 
    BUSINESS b ON b."business_id" = t."business_id"
GROUP BY 
    b."state"
ORDER BY 
    "avg_rounded_stars" DESC

-- bonus cliquable sur la map : Nombre de business par état
SELECT COUNT(t."business_id")
from TENDENCY t 
INNER JOIN BUSINESS b ON b."business_id" = t."business_id"
WHERE b."state" = {{state}}

-- Calculez la moyenne des étoiles pour chaque entreprise et analysez comment cette moyenne varie au fil du temps.
SELECT 
    t."business_id",
    t."business_name",
    EXTRACT(YEAR FROM r."date") AS "year",
    EXTRACT(MONTH FROM r."date") AS "month",
    ROUND(AVG(r."stars"), 2) AS "average_stars"
FROM 
    TENDENCY t 
INNER JOIN 
    REVIEW r ON r."business_id" = t."business_id"
WHERE 
    t."business_name" = {{business_name}}
GROUP BY 
    t."business_id", t."business_name", EXTRACT(YEAR FROM r."date"), EXTRACT(MONTH FROM r."date")
ORDER BY 
    t."business_id", EXTRACT(YEAR FROM r."date"), EXTRACT(MONTH FROM r."date");



-- Les endroits accessibles sont mieux notés ?
SELECT 
    ROUND(AVG(CASE WHEN a."WheelchairAccessible"  = 1 THEN b."stars" ELSE NULL END)) AS avg_rating_accessible,
    ROUND(AVG(CASE WHEN a."WheelchairAccessible" =  0 THEN b."stars" ELSE NULL END)) AS avg_rating_non_accessible
FROM 
    BUSINESS b
LEFT JOIN 
    ACCESSIBILITY a ON b."business_id" = a."business_id"