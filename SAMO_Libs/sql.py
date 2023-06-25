from .Base_libs_and_options import *
from .constants import *
from .General_func import *
# Layer linh hoạt

def create_RDB(layer='pool', additional_table=[]):
    global engine

    if layer == 'pool':
        # Create the db engine
        engine = create_engine('sqlite:///:memory:')

        hdkd = pd.read_csv(f'{pool_in}/b3_hdkd.csv', low_memory=False, converters={'CHECK SỐ HĐ': str})
        ns = pd.read_csv(f'{pool_in}/b3_ns.csv', low_memory=False, converters={'CMND/CCCD': str})
        ns_hdkd = pd.read_csv(f'{pool_in}/b3_ns_hdkd.csv', low_memory=False, converters={'CHECK SỐ HĐ': str})
        if 'cohort' in additional_table:
            cohort = pd.read_csv(f'{for_reports_in}/cohort_AA_for_mapa.csv', low_memory=False)
            cohort['Month'] = pd.to_datetime(cohort['Month'])
            cohort.to_sql('cohort', engine)

        pnt = pd.read_csv(f'{pool_in}/pnt.csv', low_memory=False, dtype=str)
        dt = pd.read_csv(f'{pool_in}/Data dao tao chi tiet.csv', low_memory=False)
        if 'hlv' in additional_table:
            hlv = pd.read_csv(f'{pool_in}/Data_HLV.csv', low_memory=False, converters={'POLICY CODE': str})
            hlv.to_sql('hlv', engine)
        if 'slv' in additional_table:
            slv = pd.read_csv(f'{pool_in}/Data_SLV.csv', low_memory=False, converters={'APPLICATION_NUMBER': str, 'POLICY_NUMBER': str})
            slv.to_sql('slv', engine)

        hdkd = hdkd.rename(columns={'NGÀY TẠM PHÁT HÀNH': 'NGÀY PH', 'NGÀY NỘP HỒ SƠ': 'NGÀY NỘP'})
        hdkd['NGÀY NỘP'] = pd.to_datetime(hdkd['NGÀY NỘP'])
        hdkd['NGÀY PH'] = pd.to_datetime(hdkd['NGÀY PH'])
        hdkd['NGÀY HỦY/ HOÃN'] = pd.to_datetime(hdkd['NGÀY HỦY/ HOÃN'])
        ns['NGÀY TẠO MÃ GT'] = pd.to_datetime(ns['NGÀY TẠO MÃ GT'])
        ns['NGÀY DEACTIVE'] = pd.to_datetime(ns['NGÀY DEACTIVE'])
        dt['Ngày học'] = pd.to_datetime(dt['Ngày học'])

        lst_month, this_month = create_lst_month()

        # Store the dataframe as a table
        hdkd.to_sql('hdkd', engine)
        ns.to_sql('ns', engine)
        ns_hdkd.to_sql('ns_hdkd', engine)
        pnt.to_sql('pnt', engine)
        dt.to_sql('dt', engine)
        lst_month.to_sql('month', engine)

        return engine

def test():
    engine = create_RDB()

    hdkd_command = """SELECT hdkd.'CHECK SỐ HĐ', hdkd.'FYP', ns_hdkd.'MÃ GT', ns.'DSD', ns.'TÊN DSD'
		                 FROM hdkd
		                 LEFT JOIN ns_hdkd ON ns_hdkd.'CHECK SỐ HĐ' = hdkd.'CHECK SỐ HĐ'
		                 LEFT JOIN ns ON ns.'MÃ GIỚI THIỆU' = ns_hdkd.'MÃ GT'"""

    ns_command = """SELECT ns.'MÃ GIỚI THIỆU', ns.'NGÀY TẠO MÃ GT'
    				,dt1.'Ngày học' as 'NGÀY HỌC ĐẦU TIÊN'
	                 FROM ns
	                 LEFT JOIN dt dt1 on ns.'MÃ GIỚI THIỆU' = dt1.'Mã GT'
	                 LEFT JOIN dt dt2 on dt1.'Mã GT' = dt2.'Mã GT' and dt1.'Ngày học' > dt2.'Ngày học'
	                 WHERE dt2.'Ngày học' is null
	                 GROUP BY ns.'MÃ GIỚI THIỆU'
	             """

    ns_90D_command = """SELECT ns.'MÃ GIỚI THIỆU', ns.'NGÀY TẠO MÃ GT'
    				,dt1.'Ngày học' as 'NGÀY HỌC ĐẦU TIÊN'
    				,v.column1 as 'Ngày báo cáo'
	                 FROM ns
	                 LEFT JOIN dt dt1 on ns.'MÃ GIỚI THIỆU' = dt1.'Mã GT'
	                 LEFT JOIN dt dt2 on dt1.'Mã GT' = dt2.'Mã GT' and dt1.'Ngày học' > dt2.'Ngày học'
	                 CROSS JOIN (VALUES ('2022-06-30'), ('2022-07-31'), ('2022-08-31')) as v
	                 WHERE dt2.'Ngày học' is null
	                 GROUP BY ns.'MÃ GIỚI THIỆU', v.column1
	                 ORDER BY v.column1
	             """

    hdkd_detail_command = """SELECT hdkd.'CHECK SỐ HĐ', hdkd.'FYP'
			                   FROM hdkd
			                   LEFT JOIN ns_hdkd ON ns_hdkd.'CHECK SỐ HĐ' = hdkd.'CHECK SỐ HĐ'
			                   LEFT JOIN ns ON ns.'MÃ GIỚI THIỆU' = ns_hdkd.'MÃ GT'
			                   LEFT JOIN hlv ON hdkd.'CHECK SỐ HĐ' = hlv.'POLICY CODE'
			                   LEFT JOIN slv ON hdkd.'CHECK SỐ HĐ' = slv.'APPLICATION_NUMBER'"""

    join_with_value_command = """SELECT v.column1 as ValueId FROM (VALUES ('2022-06-30'), ('2022-07-31'), ('2022-08-31')) as v"""

    res = pd.read_sql_query(ns_command, engine)
    print(res)