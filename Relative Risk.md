# SQL Queries (on BigQuery)
## 1. Data Processing and Preparation
### 1.1 Standardization and imputations
```sql
SELECT *
  EXCEPT (CRS_ELAPSED_TIME, DEP_DELAY),
  IFNULL (CRS_ELAPSED_TIME, CRS_ARR_TIME_NEW - CRS_DEP_TIME_NEW) AS CRS_ELAPSED_TIME_NEW,
  CASE WHEN DEP_DELAY IS NULL AND DEP_TIME_NEW IS NOT NULL THEN DEP_TIME_NEW - CRS_DEP_TIME_NEW ELSE DEP_DELAY END AS DEP_DELAY_NEW
FROM (
  SELECT
    * EXCEPT (CRS_DEP_TIME, DEP_TIME, WHEELS_OFF, WHEELS_ON, CRS_ARR_TIME, ARR_TIME),
    CAST(SUBSTR(FORMAT("%04d", CRS_DEP_TIME), 1, 2) AS INT64) * 60 +
    CAST(SUBSTR(FORMAT("%04d", CRS_DEP_TIME), 3, 2) AS INT64) AS CRS_DEP_TIME_NEW,
    CAST(SUBSTR(FORMAT("%04d", DEP_TIME), 1, 2) AS INT64) * 60 +
    CAST(SUBSTR(FORMAT("%04d", DEP_TIME), 3, 2) AS INT64) AS DEP_TIME_NEW,
    CAST(SUBSTR(FORMAT("%04d", WHEELS_OFF), 1, 2) AS INT64) * 60 +
    CAST(SUBSTR(FORMAT("%04d", WHEELS_OFF), 3, 2) AS INT64) AS WHEELS_OFF_NEW,
    CAST(SUBSTR(FORMAT("%04d", WHEELS_ON), 1, 2) AS INT64) * 60 +
    CAST(SUBSTR(FORMAT("%04d", WHEELS_ON), 3, 2) AS INT64) AS WHEELS_ON_NEW,
    CAST(SUBSTR(FORMAT("%04d", CRS_ARR_TIME), 1, 2) AS INT64) * 60 +
    CAST(SUBSTR(FORMAT("%04d", CRS_ARR_TIME), 3, 2) AS INT64) AS CRS_ARR_TIME_NEW,
    CAST(SUBSTR(FORMAT("%04d", ARR_TIME), 1, 2) AS INT64) * 60 +
    CAST(SUBSTR(FORMAT("%04d", ARR_TIME), 3, 2) AS INT64) AS ARR_TIME_NEW
  FROM
    `proyecto-no4-datalab.dataset.flights`
  WHERE
   DIVERTED = 0 OR ARR_TIME IS NOT NULL
  ) AS subquery
```
### 1.2 Data extraction
```sql
SELECT
  * EXCEPT (Description), 
REGEXP_EXTRACT(Description, r'([^:]+)') AS DESCRIPTION_NEW
FROM
  `proyecto-no4-datalab.dataset.dot_code_dictionary`
WHERE Description IS NOT NULL
```
### 1.2 Join tables
Codes Table
```sql
SELECT
A.Code AS AIRLINE_CODE,
B.Code AS DOT_CODE,
A.Description
FROM
  `proyecto-no4-datalab.dataset.airline_code_dictionary` AS A
INNER JOIN
  `proyecto-no4-datalab.dataset.dot_code` AS B
ON
 A.Description= B.DESCRIPTION_NEW
ORDER BY A.Description
```
Master Table
```sql
CREATE OR REPLACE TABLE `proyecto-no4-datalab.dataset.master` AS
SELECT
A.FL_DAY,
FORMAT_DATE('%A', DATE(2023, 1, FL_DAY)) AS FLDAY_WEEK,
A.AIRLINE_CODE,
A.DOT_CODE,
B.Description AS DESCRIPTION,
A.ORIGIN, A.ORIGIN_CITY,
SPLIT(ORIGIN_CITY, ', ')[OFFSET(1)] AS ORIGIN_STATE,
A.DEST, A.DEST_CITY,
SPLIT(DEST_CITY, ', ')[OFFSET(1)] AS DEST_STATE,
A.CRS_DEP_TIME_NEW, A.DEP_TIME_NEW,
A.DEP_DELAY_NEW,
A.TAXI_OUT,
A.WHEELS_OFF_NEW,
A.WHEELS_ON_NEW,
A.TAXI_IN,
A.CRS_ARR_TIME_NEW, A.ARR_TIME_NEW,
A.ARR_DELAY,
A.CANCELLED, A.CANCELLATION_CODE,
A.CRS_ELAPSED_TIME_NEW, A.ELAPSED_TIME,
A.AIR_TIME,
A.DELAY_DUE_CARRIER, A.DELAY_DUE_WEATHER, A.DELAY_DUE_NAS, A.DELAY_DUE_SECURITY, A.DELAY_DUE_LATE_AIRCRAFT,
A.DISTANCE
FROM
  `proyecto-no4-datalab.dataset.flights_clean` AS A
LEFT JOIN
  `proyecto-no4-datalab.dataset.codes` AS B
ON
 A.DOT_CODE = B.DOT_CODE
WHERE
  (ARR_DELAY IS NULL OR ARR_DELAY < 1440)
AND
  (DEP_DELAY_NEW IS NULL OR DEP_DELAY_NEW < 1440)
AND DIVERTED != 1
AND FL_DAY != 11
ORDER BY FL_DAY
```
Final Table
```sql
CREATE OR REPLACE TABLE `proyecto-no4-datalab.dataset.final` AS
SELECT *,
  CASE
  WHEN DEP_DELAY_NEW >= 15 AND CANCELLED = 0 THEN 1
  WHEN CANCELLED = 1 THEN NULL
  ELSE 0
  END AS DELAYS,
CASE
  WHEN CRS_DEP_TIME_NEW <= 360 THEN 'Madrugada'
  WHEN CRS_DEP_TIME_NEW <= 720 THEN 'Mañana'
  WHEN CRS_DEP_TIME_NEW <= 1080 THEN 'Tarde'
  ELSE 'Noche'
  END AS CRS_DEP_Category,
CASE
  WHEN CRS_ELAPSED_TIME_NEW <= 108 THEN 'Corto'
  WHEN CRS_ELAPSED_TIME_NEW <= 216 THEN 'Medio'
  WHEN CRS_ELAPSED_TIME_NEW <= 324 THEN 'Largo'
  ELSE 'Extendido'
  END AS CRS_TIME_Category,
CASE
  WHEN DISTANCE <= 681 THEN 'Corto'
  WHEN DISTANCE <= 1362 THEN 'Medio'
  WHEN DISTANCE <= 2043 THEN 'Largo'
  ELSE 'Extendido'
  END AS DISTANCE_Category,
CASE
  WHEN ORIGIN_STATE IN ('ME','NH','VT','MA','RI','CT') THEN 'New England'
  WHEN ORIGIN_STATE IN ('NY','NJ','PA') THEN 'Mid-Atlantic'
  WHEN ORIGIN_STATE IN ('OH','IN','IL','MI','WI') THEN 'East North Central'
  WHEN ORIGIN_STATE IN ('ND','SD','NE','KS','MN','IA','MO') THEN 'West North Central'
  WHEN ORIGIN_STATE IN ('DE','MD','VA','WV','NC','SC','GA','FL') THEN 'South Atlantic'
  WHEN ORIGIN_STATE IN ('KY','TN','AL','MS') THEN 'East South Central'
  WHEN ORIGIN_STATE IN ('AR','LA','OK','TX') THEN 'West South Central'
  WHEN ORIGIN_STATE IN ('MT','ID','WY','CO','NM','AZ','UT','NV') THEN 'Great Basin'
  WHEN ORIGIN_STATE IN ('WA','OR','CA') THEN 'Pacific'
  ELSE 'Capital' 
  END AS ORIGIN_Region,
CASE
  WHEN DEST_STATE IN ('ME','NH','VT','MA','RI','CT') THEN 'New England'
  WHEN DEST_STATE IN ('NY','NJ','PA') THEN 'Mid-Atlantic'
  WHEN DEST_STATE IN ('OH','IN','IL','MI','WI') THEN 'East North Central'
  WHEN DEST_STATE IN ('ND','SD','NE','KS','MN','IA','MO') THEN 'West North Central'
  WHEN DEST_STATE IN ('DE','MD','VA','WV','NC','SC','GA','FL') THEN 'South Atlantic'
  WHEN DEST_STATE IN ('KY','TN','AL','MS') THEN 'East South central'
  WHEN DEST_STATE IN ('AR','LA','OK','TX') THEN 'West South Central'
  WHEN DEST_STATE IN ('MT','ID','WY','CO','NM','AZ','UT','NV') THEN 'Great Basin'
  WHEN DEST_STATE IN ('WA','OR','CA') THEN 'Pacific'
  ELSE 'Capital'
  END AS DEST_Region,
CASE
  WHEN ORIGIN IN ('ATL','DFW','DEN','ORD','LAX','JFK','LAS','MCO','MIA','CLT','SEA','PHX','EWR','SFO','IAH','BOS','FLL','MSP','LGA','DTW','PHL','SLC','DCA','SAN','BWI','TPA','AUS','IAD','BNA','MDW','HNL') THEN 'Large_hub'
  WHEN ORIGIN IN ('DAL','PDX','STL','HOU','SMF','MSY','RDU','SJC','SNA','OAK','RSW','SJU','MCI','SAT','CLE','IND','OGG','PIT','CVG','CMH','PBI','JAX','BUR','BDL','ONT','MKE','CHS','ANC','ABQ','BOI','OMA','MEM','RNO') THEN 'Medium_hub'
  WHEN ORIGIN IN('ORF','RIC','BUF','KOA','ELP','OKC','SRQ','GEG','SDF','LIH','SAV','GRR','MYR','TUS','LGB','PVD','PSP','TUL','DSM','SFB','BHM','ALB','SYR','PIE','TYS','PNS','ROC','BZN','COS','FAT','GSP','VPS','LIT','PWM','IWA','PGD','AVL','HPN','MSN','XNA','GSO','EUG','ICT','ECP','STT','EYW','MHT','ITO','MAF','MDT','ISP','CID','SBA','FSD','BTV','JAN','HSV','LEX','DAY','SGF','ILM','RDM','FAI','HRL','CAE','MFR','LBB','FAR','ACY','MFE','CHA','GUM','MSO') THEN 'Small_hub'
  ELSE 'Nonhub'
  END AS ORIGIN_Size,
CASE
  WHEN DEST IN ('ATL','DFW','DEN','ORD','LAX','JFK','LAS','MCO','MIA','CLT','SEA','PHX','EWR','SFO','IAH','BOS','FLL','MSP','LGA','DTW','PHL','SLC','DCA','SAN','BWI','TPA','AUS','IAD','BNA','MDW','HNL') THEN 'Large_hub'
  WHEN DEST IN ('DAL','PDX','STL','HOU','SMF','MSY','RDU','SJC','SNA','OAK','RSW','SJU','MCI','SAT','CLE','IND','OGG','PIT','CVG','CMH','PBI','JAX','BUR','BDL','ONT','MKE','CHS','ANC','ABQ','BOI','OMA','MEM','RNO') THEN 'Medium_hub'
  WHEN DEST IN('ORF','RIC','BUF','KOA','ELP','OKC','SRQ','GEG','SDF','LIH','SAV','GRR','MYR','TUS','LGB','PVD','PSP','TUL','DSM','SFB','BHM','ALB','SYR','PIE','TYS','PNS','ROC','BZN','COS','FAT','GSP','VPS','LIT','PWM','IWA','PGD','AVL','HPN','MSN','XNA','GSO','EUG','ICT','ECP','STT','EYW','MHT','ITO','MAF','MDT','ISP','CID','SBA','FSD','BTV','JAN','HSV','LEX','DAY','SGF','ILM','RDM','FAI','HRL','CAE','MFR','LBB','FAR','ACY','MFE','CHA','GUM','MSO') THEN 'Small_hub'
  ELSE 'Nonhub'
  END AS DEST_Size,
CASE
  WHEN DESCRIPTION IN ('United Air Lines Inc.','Southwest Airlines Co.','Delta Air Lines Inc.','American Airlines Inc.','Alaska Airlines Inc.') THEN 'Principal'
  ELSE 'Regional'
  END AS DESCRIPTION_Category,
FROM `proyecto-no4-datalab.dataset_p4.master`
WHERE 
ORIGIN_STATE NOT IN ('PR', 'MP', 'VI', 'GU', 'AS', 'TT', 'AK', 'HI')
AND
DEST_STATE NOT IN ('PR', 'MP', 'VI', 'GU', 'AS', 'TT', 'AK', 'HI')
ORDER BY 
FL_DAY, CRS_DEP_TIME_NEW
```
Delays Table
```sql
CREATE OR REPLACE TABLE `proyecto-no4-datalab.dataset.delay` AS
SELECT *
EXCEPT (CANCELLED, CANCELLATION_CODE)
FROM `proyecto-no4-datalab.dataset_p4.final`
WHERE DELAYS IS NOT NULL
ORDER BY 
FL_DAY, CRS_DEP_TIME_NEW
```
### 2. Relative Risk
By each variable:
```sql
 SELECT
    CRS_DEP_Category AS category,
    SUM(CASE WHEN DELAYS = 1 THEN 1 ELSE 0 END) AS delay_category_1,
    SUM(CASE WHEN DELAYS = 0 THEN 1 ELSE 0 END) AS wo_delay_category_0,
  FROM
    `proyecto-no4-datalab.dataset.delay`
GROUP BY category
ORDER BY category
```
```sql
 WITH Totals AS (
  SELECT
    CRS_DEP_Category,
    SUM(CASE WHEN DELAYS = 1 THEN 1 ELSE 0 END) AS total_delay,
    SUM(CASE WHEN DELAYS = 0 THEN 1 ELSE 0 END) AS total_no_delay
  FROM
    `proyecto-no4-datalab.dataset_p4.delay`
  GROUP BY
    CRS_DEP_Category
)
SELECT
  'Madrugada' AS category,
  SUM(total_delay) AS delay_rest_1,
  SUM(total_no_delay) AS wo_delay_rest_0
FROM
  Totals
WHERE
  CRS_DEP_Category IN ('Mañana', 'Tarde', 'Noche')
UNION ALL
SELECT
  'Mañana' AS category,
  SUM(total_delay) AS delay_rest_1,
  SUM(total_no_delay) AS wo_delay_rest_0
FROM
  Totals
WHERE
  CRS_DEP_Category IN ('Madrugada', 'Tarde', 'Noche')
UNION ALL
SELECT
  'Tarde' AS category,
  SUM(total_delay) AS delay_rest_1,
  SUM(total_no_delay) AS wo_delay_rest_0
FROM
  Totals
WHERE
  CRS_DEP_Category IN ('Madrugada', 'Mañana', 'Noche')
UNION ALL
SELECT
  'Noche' AS category,
  SUM(total_delay) AS delay_rest_1,
  SUM(total_no_delay) AS wo_delay_rest_0
FROM
  Totals
WHERE
  CRS_DEP_Category IN ('Madrugada', 'Mañana', 'Tarde')
ORDER BY
  category
```
Incident rates and RR
```sql
CREATE OR REPLACE TABLE `proyecto-no4-datalab.dataset.rr` AS
SELECT
  '1' AS issue,
  'Parte del Día' AS variable,
  A.category,
  A.delay_category_1 / NULLIF(A.delay_category_1 + A.wo_delay_category_0, 0) AS incidence_rate_1,
  B.delay_rest_1 / NULLIF(B.delay_rest_1 + B.wo_delay_rest_0, 0) AS incidence_rate_0,
  (A.delay_category_1 / NULLIF(A.delay_category_1 + A.wo_delay_category_0, 0)) / (B.delay_rest_1 / NULLIF(B.delay_rest_1 + B.wo_delay_rest_0, 0)) AS relative_risk
FROM
  `proyecto-no4-datalab.dataset_p4.1-0_origin-time` AS A
LEFT JOIN
  `proyecto-no4-datalab.dataset_p4.1-1_origin-time` AS B
ON A.category = B.category
UNION ALL
SELECT
  '2' AS issue,
  'Día de la Semana' AS variable,
  A.category,
  A.delay_category_1 / NULLIF(A.delay_category_1 + A.wo_delay_category_0, 0) AS incidence_rate_1,
  B.delay_rest_1 / NULLIF(B.delay_rest_1 + B.wo_delay_rest_0, 0) AS incidence_rate_0,
  (A.delay_category_1 / NULLIF(A.delay_category_1 + A.wo_delay_category_0, 0)) / (B.delay_rest_1 / NULLIF(B.delay_rest_1 + B.wo_delay_rest_0, 0)) AS relative_risk
FROM
  `proyecto-no4-datalab.dataset_p4.2-0_day` AS A
LEFT JOIN
  `proyecto-no4-datalab.dataset_p4.2-1_day` AS B
ON A.category = B.category
UNION ALL
SELECT
  '3' AS issue,
  'Longitud del Vuelo' AS variable,
  A.category,
  A.delay_category_1 / NULLIF(A.delay_category_1 + A.wo_delay_category_0, 0) AS incidence_rate_1,
  B.delay_rest_1 / NULLIF(B.delay_rest_1 + B.wo_delay_rest_0, 0) AS incidence_rate_0,
  (A.delay_category_1 / NULLIF(A.delay_category_1 + A.wo_delay_category_0, 0)) / (B.delay_rest_1 / NULLIF(B.delay_rest_1 + B.wo_delay_rest_0, 0)) AS relative_risk
FROM
  `proyecto-no4-datalab.dataset_p4.3-0_distance` AS A
LEFT JOIN
  `proyecto-no4-datalab.dataset_p4.3-1_distance` AS B
ON A.category = B.category
UNION ALL
SELECT
  '4' AS issue,
  'Aeropuerto de Origen' AS variable,
  A.category,
  A.delay_category_1 / NULLIF(A.delay_category_1 + A.wo_delay_category_0, 0) AS incidence_rate_1,
  B.delay_rest_1 / NULLIF(B.delay_rest_1 + B.wo_delay_rest_0, 0) AS incidence_rate_0,
  (A.delay_category_1 / NULLIF(A.delay_category_1 + A.wo_delay_category_0, 0)) / (B.delay_rest_1 / NULLIF(B.delay_rest_1 + B.wo_delay_rest_0, 0)) AS relative_risk
FROM
  `proyecto-no4-datalab.dataset_p4.4-0_origin-airport` AS A
LEFT JOIN
  `proyecto-no4-datalab.dataset_p4.4-1_origin-airport` AS B
ON A.category = B.category
UNION ALL
SELECT
  '5' AS issue,
  'Aereolínea' AS variable,
  A.category,
  A.delay_category_1 / NULLIF(A.delay_category_1 + A.wo_delay_category_0, 0) AS incidence_rate_1,
  B.delay_rest_1 / NULLIF(B.delay_rest_1 + B.wo_delay_rest_0, 0) AS incidence_rate_0,
  (A.delay_category_1 / NULLIF(A.delay_category_1 + A.wo_delay_category_0, 0)) / (B.delay_rest_1 / NULLIF(B.delay_rest_1 + B.wo_delay_rest_0, 0)) AS relative_risk
FROM
  `proyecto-no4-datalab.dataset_p4.5-0_airline` AS A
LEFT JOIN
  `proyecto-no4-datalab.dataset_p4.5-1_airline` AS B
ON A.category = B.category
UNION ALL
SELECT
  '6' AS issue,
  'Región de Origen' AS variable,
  A.category,
  A.delay_category_1 / NULLIF(A.delay_category_1 + A.wo_delay_category_0, 0) AS incidence_rate_1,
  B.delay_rest_1 / NULLIF(B.delay_rest_1 + B.wo_delay_rest_0, 0) AS incidence_rate_0,
  (A.delay_category_1 / NULLIF(A.delay_category_1 + A.wo_delay_category_0, 0)) / (B.delay_rest_1 / NULLIF(B.delay_rest_1 + B.wo_delay_rest_0, 0)) AS relative_risk
FROM
  `proyecto-no4-datalab.dataset_p4.6-0_origin-region` AS A
LEFT JOIN
  `proyecto-no4-datalab.dataset_p4.6-1_origin-region` AS B
ON A.category = B.category
UNION ALL
SELECT
  '7' AS issue,
  'Duración del Vuelo' AS variable,
  A.category,
  A.delay_category_1 / NULLIF(A.delay_category_1 + A.wo_delay_category_0, 0) AS incidence_rate_1,
  B.delay_rest_1 / NULLIF(B.delay_rest_1 + B.wo_delay_rest_0, 0) AS incidence_rate_0,
  (A.delay_category_1 / NULLIF(A.delay_category_1 + A.wo_delay_category_0, 0)) / (B.delay_rest_1 / NULLIF(B.delay_rest_1 + B.wo_delay_rest_0, 0)) AS relative_risk
FROM
  `proyecto-no4-datalab.dataset_p4.7-0_duration` AS A
LEFT JOIN
  `proyecto-no4-datalab.dataset_p4.7-1_duration` AS B
ON A.category = B.category
ORDER BY
  issue, relative_risk
```
