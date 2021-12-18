
WITH TA1 AS (
  SELECT UNIT_CLASS,
  SUM(COALESCE(EDAY3,0)) AS  EDAY3_CNT,
  SUM(COALESCE(EDAY7,0)) AS EDAY7_CNT,
  SUM(COALESCE(SYS0,0)) AS SYS_CNT,
  SUM(COALESCE(USER0,0)) AS USER_CNT,
  SUM(COALESCE(EDAYS,0) )AS EDAYS_SUM
  FROM (
    SELECT SUBSTR(DCODE,3,1) as UNIT_CLASS, DCODE, RNO,
    CASE WHEN STS = '40' and (EDAYS is null or EDAYS < 4 ) THEN 1 ELSE 0 END AS EDAY3,
    CASE WHEN STS = '40' and EDAYS > 3 THEN 1 ELSE 0 END AS EDAY7,
    CASE WHEN STS = '50' and ENDID = 'SYSTEM'  THEN 1 ELSE 0 END AS SYS0,
    CASE WHEN STS = '50' and ENDID != 'SYSTEM' THEN 1 ELSE 0 END AS USER0,
    CASE WHEN STS = '50' and ENDID != 'SYSTEM' THEN EDAYS ELSE 0 END AS EDAYS
    FROM DBXZ.DTXZA099
    WHERE STS IN ('40','50')
          AND (SER_NO_4 NOT IN (1871, 1873) or SER_NO_4 is null)
           [ AND SUBSTR(DCODE,3,1) = ':DIV1_STR' ]
          AND (ENDTIME is null or (ENDTIME BETWEEN ':RPT_DT_STR' || ' 00:00:00.000000' AND ':RPT_DT_END' || ' 23:59:59.9999'))

     )
  GROUP BY UNIT_CLASS
),
TA2 AS (
  SELECT UNIT_CLASS,
SUM(COALESCE(RDAYS,0)) AS RDAYS_SUM,
SUM(COALESCE(R_CNT,0)) AS R_CNT,
   SUM(COALESCE(DAY0,0)) AS RDAY0_CNT,
SUM(COALESCE(DAY1,0)) AS RDAY1_CNT,
SUM(COALESCE(DAY2,0)) AS RDAY2_CNT,
SUM(COALESCE(DAY3,0)) AS RDAY3_CNT
 FROM
      (
    SELECT A099.RNO, A020.RSNO, SUBSTR(A020.RDCODE,3,1) AS UNIT_CLASS,
A020.RDCODE, A020.RCKID, A020.OKDATE, A020.RDAYS AS RDAYS_O,
    CASE WHEN A020.RCKID IS NULL and (A020.RDAYS = 0 OR A020.RCV_DT  = CURRENT DATE ) THEN 1 ELSE 0 END AS DAY0,
    CASE WHEN A020.RCKID IS NULL and  A020.RDAYS in (1,2) THEN 1 ELSE 0 END AS DAY1,
    CASE WHEN A020.RCKID IS NULL and  A020.RDAYS =  3     THEN 1 ELSE 0 END AS DAY2,
    CASE WHEN A020.RCKID IS NULL and  A020.RDAYS > 3      THEN 1 ELSE 0 END AS DAY3,
    CASE WHEN A020.RCKID IS NOT NULL THEN A020.RDAYS ELSE 0 END AS RDAYS,
    CASE WHEN A020.RCKID IS NOT NULL THEN 1 ELSE 0 END AS R_CNT
    FROM DBXZ.DTXZA099 A099 INNER JOIN DBXZ.DTXZA020 A020 ON A099.RNO = A020.RNO
    WHERE A099.STS BETWEEN '20' AND '50'
          AND (A099.SER_NO_4 NOT IN (1871, 1873) or A099.SER_NO_4 is null)
          [ AND SUBSTR(A020.RDCODE,3,1) = ':DIV1_STR' ]
          AND ((A020.OKDATE BETWEEN ':RPT_DT_STR' AND ':RPT_DT_END') OR A020.RCKID IS NULL )
          AND A099.COMP_ID = ':COMP_ID'

  )
  GROUP BY UNIT_CLASS

),
TA3 AS (
    SELECT UNIT_CLASS  FROM TA1
    UNION
    SELECT UNIT_CLASS  FROM TA2
)

SELECT '1' AS RPT_LVL, TA3.UNIT_CLASS, '00' || TA3.UNIT_CLASS || '0000' AS DIV_NO,
       TA1.EDAY3_CNT, TA1.EDAY7_CNT, TA1.SYS_CNT, TA1.USER_CNT, TA1.EDAYS_SUM,
       TA2.RDAYS_SUM, TA2.R_CNT, TA2.RDAY0_CNT, TA2.RDAY1_CNT, TA2.RDAY2_CNT, TA2.RDAY3_CNT
 FROM TA3
 LEFT JOIN TA1 ON TA3.UNIT_CLASS = TA1.UNIT_CLASS
 LEFT JOIN TA2 ON TA3.UNIT_CLASS = TA2.UNIT_CLASS
 WHERE TA3.UNIT_CLASS != 'C' 
 ORDER BY TA3.UNIT_CLASS
 WITH UR