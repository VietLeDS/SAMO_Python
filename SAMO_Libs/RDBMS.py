from .Base_libs_and_options import *
from .constants import *
from .General_func import *
from .Layer1 import *

# Mục đích: rd_in -> break down to rd_out -> group by to pool in
# Task: Chèn thăng chức vào b3_ns trong rd_in_in và save vào rd_out_in

def RBD():
	global engine
	# Create the db engine
	engine = create_engine('sqlite:///:memory:')
	# metadata_obj = MetaData()
	return engine

def break_down(engine):
	hdkd = pd.read_csv(f'{rd_in_in}/b3_hdkd.csv', low_memory=False, converters={'CHECK SỐ HĐ': str})
	ns = pd.read_csv(f'{rd_in_in}/b3_ns.csv', low_memory=False)
	ns_hdkd = pd.read_csv(f'{rd_in_in}/b3_ns_hdkd.csv', low_memory=False, converters={'CHECK SỐ HĐ': str})

	hdkd.to_sql('hdkd', engine)
	ns.to_sql('ns', engine)
	ns_hdkd.to_sql('ns_hdkd', engine)

	ns_ttin_cn = ns[['MÃ GIỚI THIỆU', 'TÊN CHUYÊN VIÊN', 'CMND/CCCD', 'NGÀY SINH',
	   				'GIỚI TÍNH', 'TỈNH/TP', 'QUẬN/ HUYỆN', 'NGÀY TẠO MÃ GT']]
	ns_gioi_thieu = ns[['MÃ GIỚI THIỆU', 'MÃ NGƯỜI GIỚI THIỆU', 'TÊN NGƯỜI GIỚI THIỆU']]
	ns_status = ns[['MÃ GIỚI THIỆU', 'TRẠNG THÁI CHUYÊN VIÊN', 'NGÀY DEACTIVE', 'VỊ TRÍ', 'STT VỊ TRÍ']]
	ns_cau_truc = ns[['MÃ GIỚI THIỆU', 'MÃ NGƯỜI QLTT', 'DFR', 'SA', 'DFA', 'DFT', 'DFM', 'DSD', 'DSSD', 'DRD', 'DSRD', 'DTD',
	   'DSTD', 'DND', 'DSND', 'DCEO', 'CEO', 'TÊN KÊNH', 'TÊN CHI NHÁNH', 'TÊN ĐẠI LÝ']]
	ns_cau_truc_ten = ns[['MÃ GIỚI THIỆU', 'TÊN NGƯỜI QLTT', 'TÊN DFR', 'TÊN SA', 'TÊN DFA', 'TÊN DFT',
	   'TÊN DFM', 'TÊN DSD', 'TÊN DSSD', 'TÊN DRD', 'TÊN DSRD', 'TÊN DTD',
	   'TÊN DSTD', 'TÊN DND', 'TÊN DSND', 'TÊN DCEO', 'TÊN CEO',
	   'TÊN KÊNH', 'TÊN CHI NHÁNH', 'TÊN ĐẠI LÝ']]
	ns_code = ns[['MÃ GIỚI THIỆU', 'CODE MOHA ĐẠI LÝ', 'CODE MOSU', 'CODE VBI', 'CODE PTI', 'CODE MOF',
	   'NGÀY CẤP CODE MOHA ĐẠI LÝ']]

	ns_ttin_cn.to_sql('ns_ttin_cn', engine)
	ns_gioi_thieu.to_sql('ns_gioi_thieu', engine)
	ns_status.to_sql('ns_status', engine)
	ns_cau_truc.to_sql('ns_cau_truc', engine)
	ns_cau_truc_ten.to_sql('ns_cau_truc_ten', engine)
	ns_code.to_sql('ns_code', engine)

	# Break down hdkd - Làm sau

	return engine

def thang_chuc(engine):
	thang_chuc = pd.read_csv(f'{source_ex}/Thang_chuc.csv', low_memory=False)

	command = "SELECT * FROM ns_cau_truc"
	ns_cau_truc = pd.read_sql_query(command, engine)
	command = "SELECT * FROM ns_status"
	ns_status = pd.read_sql_query(command, engine)
	engine.execute("DROP TABLE ns_status")
	engine.execute("DROP TABLE ns_cau_truc")

	# Thay đổi ns_status, rồi chạy lại cau_truc
	ns_status['VỊ TRÍ'] = ns_cau_truc.merge(thang_chuc, how='left', on='MÃ GIỚI THIỆU')[['VỊ TRÍ', 'Vị trí mới']].apply(lambda a: a[1] if pd.notna(a[1]) else a[0], axis=1)
	ns_cau_truc['VỊ TRÍ CỦA QLTT'] = ns_cau_truc.merge(thang_chuc, how='left', left_on='MÃ NGƯỜI QLTT', right_on='MÃ GIỚI THIỆU')[['VỊ TRÍ CỦA QLTT', 'Vị trí mới']].apply(lambda a: a[1] if pd.notna(a[1]) else a[0], axis=1)

	ns_cau_truc.to_sql('ns_cau_truc', engine)

	ns.to_csv(f'{rd_out_in}/b3_ns.csv', index=False, encoding='utf-8-sig')
	return engine

def build_ns_cau_truc(engine):  # Input = QH QLTT, Output = NS tree
	# Parameters
	lst_position = ['CEO','DSND','DND','DSTD','DTD','DSRD','DRD','DSSD','DSD','DFM','DFT','DFA','DFR']

	def input():
		ns['NGÀY TẠO MÃ GT'] = pd.to_datetime(ns['NGÀY TẠO MÃ GT'])
		ns['NGÀY DEACTIVE'] = pd.to_datetime(ns['NGÀY DEACTIVE'])

	ns['NGÀY TẠO MÃ GT'] = pd.to_datetime(ns['NGÀY TẠO MÃ GT'])
	ns['NGÀY DEACTIVE'] = pd.to_datetime(ns['NGÀY DEACTIVE'])
	ns.to_sql('ns', engine)
	
	engine = create_RDB()
	# thang_chuc = pd.read_csv(f'{source_ex}/Thang_chuc.csv', low_memory=False)
	# thang_chuc.to_sql('thang_chuc', engine)
	ns_command = """SELECT ns.'MÃ GIỚI THIỆU', ns.'VỊ TRÍ', ns.'MÃ NGƯỜI QLTT'
					 ,ns2.'VỊ TRÍ' as 'VỊ TRÍ CỦA QLTT'
					 FROM ns
					 LEFT JOIN ns ns2 ON ns.'MÃ NGƯỜI QLTT' = ns2.'MÃ GIỚI THIỆU'
				 """
	res = pd.read_sql_query(ns_command, engine)
	# res['VỊ TRÍ'] = res.merge(thang_chuc, how='left', on='MÃ GIỚI THIỆU')[['VỊ TRÍ', 'Vị trí mới']].apply(lambda a: a[1] if pd.notna(a[1]) else a[0], axis=1)
	# res['VỊ TRÍ CỦA QLTT'] = res.merge(thang_chuc, how='left', left_on='MÃ NGƯỜI QLTT', right_on='MÃ GIỚI THIỆU')[['VỊ TRÍ CỦA QLTT', 'Vị trí mới']].apply(lambda a: a[1] if pd.notna(a[1]) else a[0], axis=1)
	# print(res)
	# print(res.columns)
	return res

	def calculate(res):
		def lst_fit(df, level):
			result = []
			if level == 'DFR':
				if df[1] == 'DFR':
					result.append(df[0])
				if df[3] == 'DFR':
					result.append(df[2])
			else:
				if df[1] == level:
					result.append(df[0])
				if df[3] == level:
					result.append(df[2])
				if df[6] == level:
					result.append(df[5])
			result = list(dict.fromkeys(result))
			if len(result) == 0:
				return '.'
			elif len(result) == 1:
				return result[0]
			elif len(result) > 1:
				return ','.join(result)

		def check_last(df):
			x = df[-1].split(',')[-1]
			if pd.notna(x) and x != '.':
				return x
			else:
				return df[4]

		res['LAST'] = res['MÃ GIỚI THIỆU']
		res['QLTT CỦA LAST'] = res['MÃ NGƯỜI QLTT']
		res['VỊ TRÍ CỦA QLTT CỦA LAST'] = res['VỊ TRÍ CỦA QLTT']
		for _ in lst_position[::-1]:
			res[_] = res.apply(lambda a: lst_fit(a, _), axis=1).fillna('.')
			res['LAST'] = res.apply(lambda a: check_last(a), axis=1)
			res['QLTT CỦA LAST'] = res.merge(res, how='left', left_on='LAST', right_on='MÃ GIỚI THIỆU')['MÃ NGƯỜI QLTT_y']
			res['VỊ TRÍ CỦA QLTT CỦA LAST'] = res.merge(res, how='left', left_on='LAST', right_on='MÃ GIỚI THIỆU')['VỊ TRÍ CỦA QLTT_y']
		res = res.drop(columns=['QLTT CỦA LAST', 'VỊ TRÍ CỦA QLTT CỦA LAST'])
		res = res.rename(columns={'LAST': 'NGƯỜI CAO NHẤT NHÁNH'})
		res.insert(loc=5, column='VỊ TRÍ CỦA NGƯỜI CAO NHẤT NHÁNH', value=res.merge(res, how='left', left_on='NGƯỜI CAO NHẤT NHÁNH', right_on='MÃ GIỚI THIỆU')['VỊ TRÍ_y'])
		return res

	def output(res):
		if test:
			return res
		else:
			res.to_csv(f'{for_reports_in}/NS_tree.csv', index=False, encoding='utf-8-sig')

	return output(calculate(input()))
	

def transform_through_RDBMS():
	engine = RBD()
	engine = break_down(engine)
	engine = thang_chuc(engine)
	# engine = build_ns_cau_truc(engine)

	insp = inspect(engine)
	print(insp.get_table_names())