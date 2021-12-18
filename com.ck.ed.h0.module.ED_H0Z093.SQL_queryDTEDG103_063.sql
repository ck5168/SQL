with ta as(
select  h007.level,h007.sts_code as H_STS_CODE,h007.chg_date as H_chg_date,hr.DIV_SHORT_NAME as DIV_NM,hr2.DIV_SHORT_NAME as APLY_DIV_NM,
EDG103.*, EDG100.IMG_NAME, EDG100.BAL_TYPE, EDB003.BLDG_NAME,row_number() over(PARTITION by h007.box_no order by h007.box_no, h007.chg_date desc ) as CENUM
from DBED.DTEDG103 EDG103
left join DBED.DTEDH007 h007 on edg103.box_no=h007.box_no and h007.sub_cpy_id=EDG103.sub_cpy_id and h007.level='1' and h007.STS_CODE in ('5','9','10') 
left join DBED.DTEDG100 EDG100 on EDG103.IMG_KIND = EDG100.IMG_KIND
left join DBED.DTEDB003 EDB003 on EDG103.BLDG_NO = EDB003.BLDG_NO
left join DBFK.DTFK_UNIT_WORK hr on hr.div_no=EDG103.DIV_NO
left join DBFK.DTFK_UNIT_WORK hr2 on hr2.div_no=EDG103.APLY_DIV_NO
where EDG103.SUB_CPY_ID= ':SUB_CPY_ID' 
[ and EDG103.BOX_MEMO like ':BCH_MEMO' ]
[ and EDG103.BLDG_NO = ':BLDG_NO'] 
[ and EDG103.BOX_NO like ':BOX_NO'] 
[ AND (EDG103.DIV_NO = ':DIV_NO' or EDG103.MNG_DIV_NO=':DIV_NO')]
[ AND EDG103.IMG_KIND= ':IMG_KIND']
[ AND Date(EDG103.SEAL_DATE) between ':SEAL_DATE_S' and ':SEAL_DATE_E']
[AND Date(EDG103.SEAL_DATE) between ':BOX_DATE_S' and ':BOX_DATE_E']
[ AND Date(EDG103.EXPT_CNCL_DATE) between ':EXPT_CNCL_DATE_S' and ':EXPT_CNCL_DATE_E']
[ AND Date(EDG103.SEAL_DATE) between ':BOX_DATE_S' and ':BOX_DATE_E']
[ AND Date(EDG103.CHG_DATE) between ':CHG_DATE_S' and ':CHG_DATE_E']
[ AND EDG103.BOX_NO between ':BOX_NO_S' and ':BOX_NO_E']
[ and EDG103.ORI_BOX_NO like ':ORI_BOX_NO']
[ AND EDG103.EXPT_CNCL_RANGE = ':EXPT_CNCL_RANGE' ]
[ AND EDG103.OUT_BOX_NO = ':OUT_BOX_NO']
[ AND EDG103.BOX_NO between ':BOX_NO_S' AND ':BOX_NO_E' ]
[ AND EDG103.FILE_NAME1 = ':FILE_NAME1'] 
[ AND EDG103.FILE_ID1 = ':FILE_ID1']
[ AND EDG103.OUT_BOX_NO = ':OUT_BOX_NO']
[ AND EDG103.STORE_CODE_TYPE = ':STORE_CODE_TYPE']
[ AND EDG103.STORE_TYPE = ':STORE_TYPE']
[ AND EDG103.STORE_CODE = ':STORE_CODE']
 [ and EDG103.STS_CODE=':STS_CODE' ]
and EDG103.STS_CODE in ('5','9','10')
),tb as (
select H007.box_no,H007.level,H007.STS_CODE as H_STS_CODE, h007.chg_date,row_number() over(PARTITION by box_no order by box_no, chg_date desc ) as CENUM 
   from DBED.DTEDH007 H007
   where sub_cpy_id=':SUB_CPY_ID' and box_no in (select box_no from ta)
),tc as(
select * from ta
where ta.CENUM=1
)
select *
from tc
WITH UR

