from .Base_libs_and_options import *
from .constants import *
from .General_func import *
from .Layer1 import *
from .sql import *
from .cut_off import *
from .Short_data import *

def ns_self_check(adv_df, have_code=True):
	# parameters
	lst_unique_code = ['Đối tác',
	   'Mã code đối tác nội bộ', 'Ngày cấp code nội bộ',
	   'Mã code đối tác tổ chức', 'Ngày cấp code tổ chức', 'Trạng thái code',
	   'Ngày cập nhật trạng thái code']
	lst_tree = ['Tên Tổ Chức', 'Kênh Kinh Doanh', 'Đội Ngũ Kinh Doanh',
	   'CEO', 'DCEO', 'DSND', 'DND', 'DSTD', 'DTD', 'DSRD', 'DRD', 'DSSD',
	   'DSD', 'DFM', 'DFT', 'DFA', 'SA', 'DFR', 'Chức vụ', 'Mã đại diện kinh doanh']
	lst_unique_with_an_advisor = ['Mã đại diện kinh doanh'] + [_ for _ in adv_df.columns.tolist() if _ not in lst_tree and _ not in lst_unique_code]

	unique_info = adv_df[lst_unique_with_an_advisor].drop_duplicates()
	tree = adv_df[lst_tree].drop_duplicates()

	tree = tree.sort_values(by=['Mã đại diện kinh doanh', 'Chức vụ'])
	tree = tree.drop_duplicates(subset=['Mã đại diện kinh doanh'])

	# Merge
	tree = tree.merge(unique_info, on='Mã đại diện kinh doanh')
	if have_code:
		return tree.merge(adv_df[['Mã đại diện kinh doanh'] + lst_unique_code], on='Mã đại diện kinh doanh')
	else:
		return tree

def ngay_cap_code_life_dau_tien(adv):
	temp = adv.loc[(adv['Ngày cấp code nội bộ'].notna()) & ((adv['Đối tác'] == 'Hanwha Life') | (adv['Đối tác'] == 'Sun Life VN') | (adv['Đối tác'] == 'Chubb Life')), ['Mã đại diện kinh doanh', 'Ngày cấp code nội bộ']]
	temp['Ngày tạo code Life đầu tiên'] = temp['Mã đại diện kinh doanh'].apply(lambda a: temp.loc[temp['Mã đại diện kinh doanh'] == a, 'Ngày cấp code nội bộ'].min())
	temp = temp[['Mã đại diện kinh doanh', 'Ngày tạo code Life đầu tiên']].drop_duplicates()
	adv = adv.merge(temp, how='left', on='Mã đại diện kinh doanh')
	# print('Ngay cap code', temp.loc[temp['Mã đại diện kinh doanh'] == 'SKGGHHK'])

	temp2 = adv.loc[adv['Ngày cấp code nội bộ'].notna(), ['Mã đại diện kinh doanh', 'Ngày cấp code nội bộ']]
	temp2['Ngày tạo code đầu tiên'] = temp2['Mã đại diện kinh doanh'].apply(lambda a: temp2.loc[temp2['Mã đại diện kinh doanh'] == a, 'Ngày cấp code nội bộ'].min())
	temp2 = temp2[['Mã đại diện kinh doanh', 'Ngày tạo code đầu tiên']].drop_duplicates()
	adv = adv.merge(temp2, how='left', on='Mã đại diện kinh doanh')
	return adv

def crosscheck_ns():
	adv = pd.read_csv(f'{rd_in_in}/AdvisorInfo.csv', low_memory=False, dtype=str)
	adv_plus = pd.read_csv(f'{rd_in_ex}/AdvisorInfo_plus.csv', low_memory=False, dtype=str)
	dfa_convert_t10 = pd.read_excel(f'{source_ex}/DFA_Convert_202210.xlsx', sheet_name='Final', dtype=str)
	dfa_convert_t11 = pd.read_excel(f'{source_ex}/DFA_Convert_202211.xlsx', sheet_name='Final', dtype=str)
	dfa_convert_t12 = pd.read_excel(f'{source_ex}/DFA_Convert_202212.xlsx', sheet_name='Final', dtype=str)
	dfa_convert_202301 = pd.read_excel(f'{source_ex}/DFA Convert 202301_ns.xlsx', sheet_name='Final', dtype=str)
	dfa_convert_202302 = pd.read_excel(f'{source_ex}/DFA Convert 202302_ns.xlsx', sheet_name='Final', dtype=str)
	dfa_convert_202303 = pd.read_excel(f'{source_ex}/DFA Convert 202303_ns.xlsx', sheet_name='Final', dtype=str)
	dfa_convert_202304 = pd.read_excel(f'{source_ex}/DFA Convert 202304_ns.xlsx', sheet_name='Final', dtype=str)
	dfa_convert_202305 = pd.read_excel(f'{source_ex}/DFA Convert 202305_ns.xlsx', sheet_name='Final', dtype=str)

	# lg.info(adv_plus.loc[adv_plus['Mã đại diện kinh doanh'] == 'MTTAKRI', ['Mã đại diện kinh doanh', 'Đối tác', 'Mã code đối tác nội bộ', 'Ngày cấp code nội bộ']])

	lst_unique_with_an_advisor = ['Tên đại diện kinh doanh',
	   'CMND/CCCD', 'Số điện thoại', 'Ngày sinh', 'Giới tính',
	   'Tỉnh/TP', 'Quận/ Huyện', 'Mã đại diện kinh doanh',
	   'Ngày tạo mã đại diện kinh doanh', 'Ngày tạo chuyên viên',
	   'Trạng thái chuyên viên', 'Ngày cập nhật trạng thái chuyên viên', 'Mã người giới thiệu',
	   'Số tài Khoản', 'Tên Ngân hàng']
	lst_unique_code = ['Đối tác',
	   'Mã code đối tác nội bộ', 'Ngày cấp code nội bộ',
	   'Mã code đối tác tổ chức', 'Ngày cấp code tổ chức', 'Trạng thái code',
	   'Ngày cập nhật trạng thái code']
	lst_tree = ['Tên Tổ Chức', 'Kênh Kinh Doanh', 'Đội Ngũ Kinh Doanh',
	   'CEO', 'DCEO', 'DSND', 'DND', 'DSTD', 'DTD', 'DSRD', 'DRD', 'DSSD',
	   'DSD', 'DFM', 'DFT', 'DFA', 'SA', 'DFR', 'Chức vụ', 'Mã đại diện kinh doanh']

	# transform - new Dec 2022
	adv = ngay_cap_code_life_dau_tien(adv)
	adv_plus = ngay_cap_code_life_dau_tien(adv_plus)
	dfa_convert_t10 = ngay_cap_code_life_dau_tien(dfa_convert_t10)
	dfa_convert_t11 = ngay_cap_code_life_dau_tien(dfa_convert_t11)
	dfa_convert_t12 = ngay_cap_code_life_dau_tien(dfa_convert_t12)
	dfa_convert_202301 = ngay_cap_code_life_dau_tien(dfa_convert_202301)
	dfa_convert_202302 = ngay_cap_code_life_dau_tien(dfa_convert_202302)
	dfa_convert_202303 = ngay_cap_code_life_dau_tien(dfa_convert_202303)
	dfa_convert_202304 = ngay_cap_code_life_dau_tien(dfa_convert_202304)
	dfa_convert_202305 = ngay_cap_code_life_dau_tien(dfa_convert_202305)

	# print('check', dfa_convert_202305.loc[dfa_convert_202305['Mã đại diện kinh doanh'] == 'SKGGHHK'])
	# import sys
	# sys.exit()

	# Hàm data
	def remove_adv(adv):
		'''Mục đích là giảm số Register tháng 9 23000 người và tháng 10 21000 người
		Theo data đã lọc'''
		target_t9 = pd.read_excel(f'{source_ex}/xoa_advisor1.xlsx', dtype=str)
		target_t10 = pd.read_excel(f'{source_ex}/xoa_advisor2.xlsx', dtype=str)
		target_t12 = pd.read_excel(f'{source_ex}/xoa_advisor_t12_2022.xlsx', dtype=str)
		target = pd.concat([target_t9, target_t10, target_t12])
		# target = pd.concat([target_t9, target_t10])
		# print(target.loc[target['Mã đại diện kinh doanh'] == 'PUBZKWB'])
		target['key'] = 1
		adv = adv.merge(target, how='left', on='Mã đại diện kinh doanh')
		# print(adv.loc[adv['Mã đại diện kinh doanh'] == 'PUBZKWB'])
		adv = adv.loc[adv['key'] != 1].copy()
		adv = adv.drop(columns=['key'])
		# print(adv.loc[adv['Mã đại diện kinh doanh'] == 'PUBZKWB'])
		return adv

	# Hàm data
	def add_adv(adv):
		'''Mục đích là tăng 770 account không code của tháng 11
		Theo data đã lọc'''
		target = pd.read_excel(f'{source_ex}/tao_advisor dang ky_t11.xlsx', sheet_name='Final', dtype=str)
		return pd.concat([adv, target])

	# Have code
	final_ns_in = ns_self_check(adv)
	final_ns_in.to_csv(f'{rd_out_in}/AdvisorInfo.csv', index=False, encoding='utf-8-sig')
	final_ns_in.to_csv(f'{pool_in}/AdvisorInfo.csv', index=False, encoding='utf-8-sig')

	# Bổ sung ngày 25112022 - Concat thêm vá
	# print(adv.loc[adv['Mã đại diện kinh doanh'] == 'NYOEGQF'])
	final_ns_in = remove_adv(adv)
	# print(final_ns_in.loc[final_ns_in['Mã đại diện kinh doanh'] == 'NYOEGQF'])
	final_ns_in = add_adv(final_ns_in)
	final_ns_in = ns_self_check(final_ns_in)
	final_ns_ex = ns_self_check(adv_plus)
	final_ns = pd.concat([final_ns_in, final_ns_ex, dfa_convert_t10, dfa_convert_t11, dfa_convert_t12, dfa_convert_202301, dfa_convert_202302, dfa_convert_202303, dfa_convert_202304, dfa_convert_202305])
	final_ns = final_ns.sort_values(by=['Mã đại diện kinh doanh', 'Chức vụ']).drop_duplicates(subset=['Mã đại diện kinh doanh'])
	final_ns.to_csv(f'{rd_out_ex}/AdvisorInfo.csv', index=False, encoding='utf-8-sig')
	final_ns.to_csv(f'{pool_ex}/AdvisorInfo.csv', index=False, encoding='utf-8-sig')

	# Not have code
	final_ns_in = ns_self_check(adv, have_code=False)
	cau_truc = ['DFR', 'SA', 'DFA', 'DFT', 'DFM', 'DSD', 'DSSD', 'DRD', 'DSRD', 'DTD', 'DSTD', 'DND', 'DSND', 'DCEO', 'CEO']
	final_ns_in['MÃ NGƯỜI QLTT'] = final_ns_in[cau_truc].apply(lambda a: '' if len(','.join([_ for _ in a if pd.notna(_)])) in [0,7] else ','.join([_ for _ in a if pd.notna(_)])[:15][8:], axis=1)
	final_ns_in.to_csv(f'{rd_out_in}/AdvisorInfo_no_code.csv', index=False, encoding='utf-8-sig')
	final_ns_in.to_csv(f'{pool_in}/AdvisorInfo_no_code.csv', index=False, encoding='utf-8-sig')

	# Bổ sung ngày 25112022 - Concat thêm vá
	final_ns_in = remove_adv(adv)
	# print(final_ns_in.loc[final_ns_in['Mã đại diện kinh doanh'] == 'NYOEGQF'])
	final_ns_in = add_adv(final_ns_in)
	# print(final_ns_in.loc[final_ns_in['Mã đại diện kinh doanh'] == 'NYOEGQF'])
	final_ns_in = ns_self_check(final_ns_in, have_code=False)
	# print(final_ns_in.loc[final_ns_in['Mã đại diện kinh doanh'] == 'NYOEGQF'])
	final_ns_ex = ns_self_check(adv_plus, have_code=False)
	# print(final_ns_ex.loc[final_ns_ex['Mã đại diện kinh doanh'] == 'NYOEGQF'])
	dfa_convert_t10 = ns_self_check(dfa_convert_t10, have_code=False)
	dfa_convert_t11 = ns_self_check(dfa_convert_t11, have_code=False)
	dfa_convert_t12 = ns_self_check(dfa_convert_t12, have_code=False)
	dfa_convert_202301 = ns_self_check(dfa_convert_202301, have_code=False)
	dfa_convert_202302 = ns_self_check(dfa_convert_202302, have_code=False)
	dfa_convert_202303 = ns_self_check(dfa_convert_202303, have_code=False)
	dfa_convert_202304 = ns_self_check(dfa_convert_202304, have_code=False)
	dfa_convert_202305 = ns_self_check(dfa_convert_202305, have_code=False)
	final_ns = pd.concat([final_ns_in, final_ns_ex, dfa_convert_t10, dfa_convert_t11, dfa_convert_t12, dfa_convert_202301, dfa_convert_202302, dfa_convert_202303, dfa_convert_202304, dfa_convert_202305])
	# print(final_ns.loc[final_ns['Mã đại diện kinh doanh'] == 'NYOEGQF'])
	final_ns = final_ns.sort_values(by=['Mã đại diện kinh doanh', 'Chức vụ']).drop_duplicates(subset=['Mã đại diện kinh doanh'])
	# print(final_ns.loc[final_ns['Mã đại diện kinh doanh'] == 'NYOEGQF'])
	final_ns.to_csv(f'{rd_out_ex}/AdvisorInfo_no_code.csv', index=False, encoding='utf-8-sig')
	final_ns.to_csv(f'{pool_ex}/AdvisorInfo_no_code.csv', index=False, encoding='utf-8-sig')

def crosscheck_ns_io():
	adv_io = pd.read_excel(f'{source_io}/AdvisorInfo-io.xlsx', dtype=str)
	adv_io = ngay_cap_code_life_dau_tien(adv_io)
	final_ns = ns_self_check(adv_io, have_code=False)
	cau_truc = ['DFR', 'SA', 'DFA', 'DFT', 'DFM', 'DSD', 'DSSD', 'DRD', 'DSRD', 'DTD', 'DSTD', 'DND', 'DSND', 'DCEO', 'CEO']
	final_ns['MÃ NGƯỜI QLTT'] = final_ns[cau_truc].apply(lambda a: '' if len(','.join([_ for _ in a if pd.notna(_)])) in [0,7] else ','.join([_ for _ in a if pd.notna(_)])[:15][8:], axis=1)

	# Fix lỗi cụ thể
	lst_code = ['AWBUYCC','BAITIXN','BLIPKEH','CFRSJMW','CKNFHPS','CTATMUH']
	final_ns['Tên Tổ Chức'] = final_ns[['Tên Tổ Chức', 'Mã đại diện kinh doanh']].apply(lambda a: 'Thebank Partnership' if a[1] in lst_code else a[0], axis=1)
	final_ns['Kênh Kinh Doanh'] = final_ns[['Kênh Kinh Doanh', 'Mã đại diện kinh doanh']].apply(lambda a: 'TBPT 1' if a[1] in lst_code else a[0], axis=1)
	final_ns['Đội Ngũ Kinh Doanh'] = final_ns[['Đội Ngũ Kinh Doanh', 'Mã đại diện kinh doanh']].apply(lambda a: 'TBPT 1' if a[1] in lst_code else a[0], axis=1)
	final_ns.to_csv(f'{rd_out_ex}/AdvisorInfo_io_no_code.csv', index=False, encoding='utf-8-sig')
	final_ns.to_csv(f'{pool_ex}/AdvisorInfo_io_no_code.csv', index=False, encoding='utf-8-sig')

def crosscheck_ns_io_2():
	adv_io = pd.read_excel(f'{source_io}/AdvisorInfo-io-v2.xlsx', dtype=str)
	cau_truc = ['DFR', 'SA', 'DFA', 'DFT', 'DFM', 'DSD', 'DSSD', 'DRD', 'DSRD', 'DTD', 'DSTD', 'DND', 'DSND', 'DCEO', 'CEO']
	adv_io['MÃ NGƯỜI QLTT'] = adv_io[cau_truc].apply(lambda a: '' if len(','.join([_ for _ in a if pd.notna(_)])) in [0,7] else ','.join([_ for _ in a if pd.notna(_)])[:15][8:], axis=1)
	for _ in adv_io.columns:
		if 'Ngày' in _ and _ != 'Ngày sinh':
			adv_io[_] = pd.to_datetime(adv_io[_])
	adv_io.to_csv(f'{rd_out_ex}/AdvisorInfo_io_no_code.csv', index=False, encoding='utf-8-sig')
	adv_io.to_csv(f'{pool_ex}/AdvisorInfo_io_no_code.csv', index=False, encoding='utf-8-sig')


def get_id_cus(a):
	hoten = remove_accent(''.join([_[0] for _ in a[0].split(' ') if len(_) >= 1])) if pd.notna(a[0]) else 'SAMO'
	cmnd = a[1][3:7] if pd.notna(a[1]) else '0000'
	sdt = a[2][4:7] if pd.notna(a[2]) else '000'
	index = a[3]
	return f'{hoten}-{cmnd}-{sdt}-{index}'

def crosscheck_cus():
	# 1. Tách customer và Người được BH ra
	# 2. Hoàn thiện danh sách Customer bằng các chuyển Customers của HĐ DN thành Người được BH
	cus = pd.read_csv(f'{rd_in_in}/CustomerInfo.csv', low_memory=False, dtype=str)
	province = pd.read_csv(f'{rd_in_in}/province.csv', low_memory=False, dtype=str)
	op_bo_sung = pd.read_csv(f'{source_in}/OP bổ sung sdt cus.csv', low_memory=False, dtype=str)

	# # Check
	# print(f'Số customer ban đầu = {len(cus)}')

	# # Issue: CMND và Số điện thoại của nhiều Khách hàng HĐ doanh nghiệp giống nhau đều giống số mà DN đăng ký - Xử lý được 1 phần còn 1 phần
	# # Plan 1: Random + Xây dựng database về người, unique người - có CMND và Số điện thoại
	# Transform thông tin của Khách hàng Doanh nghiệp, logic là Người được BH trong HĐ doanh nghiệp mới thật sự là KH
	cus['Nhóm Khách hàng'] = cus['Họ và Tên Khách Hàng'].apply(lambda a: 'Doanh nghiệp' if 'công ty' in a.lower() else 'Cá nhân')
	# Bổ sung thông tin sdt từ OP
	cus = cus.merge(op_bo_sung, how='left', on=['Họ và Tên Khách Hàng', 'Họ và tên người được bảo hiểm 1', 'Mã hợp đồng tổ chức'])
	cus = cus.drop_duplicates(subset='Mã hợp đồng nội bộ')
	cus['Số điện thoại'] = cus[['Số điện thoại', 'Điều chỉnh']].apply(lambda a: a[1] if pd.notna(a[1]) else a[0], axis=1)
	cus = cus.drop(columns=['Điều chỉnh'])
	# Transform thông tin của Khách hàng Doanh nghiệp tiếp
	cus['Nhóm Khách hàng'] = cus['Họ và Tên Khách Hàng'].apply(lambda a: 'Doanh nghiệp' if 'công ty' in a.lower() else 'Cá nhân')
	cus['Họ và Tên Khách Hàng'] = cus[['Họ và Tên Khách Hàng', 'Họ và tên người được bảo hiểm 1', 'Nhóm Khách hàng']].apply(lambda a: a[1] if a[2] == 'Doanh nghiệp' else a[0], axis=1)
	cus['Giới tính'] = cus[['Giới tính', 'Giới tính người được bảo hiểm 1', 'Nhóm Khách hàng']].apply(lambda a: a[1] if a[2] == 'Doanh nghiệp' else a[0], axis=1)
	cus['Tuổi'] = cus[['Tuổi', 'Tuổi bảo hiểm người được bảo hiểm 1', 'Nhóm Khách hàng']].apply(lambda a: a[1] if a[2] == 'Doanh nghiệp' else a[0], axis=1)
	cus['Nghề nghiệp'] = cus[['Nghề nghiệp', 'Nghề nghiệp người được bảo hiểm 1', 'Nhóm Khách hàng']].apply(lambda a: a[1] if a[2] == 'Doanh nghiệp' else a[0], axis=1)
	cus['Ngày sinh'] = cus[['Ngày sinh', 'Ngày sinh người được bảo hiểm 1', 'Nhóm Khách hàng']].apply(lambda a: a[1] if a[2] == 'Doanh nghiệp' else a[0], axis=1)
	cus['Ngày sinh'] = pd.to_datetime(cus['Ngày sinh']).apply(lambda a: a if pd.isna(a) or a.year < 2002 else datetime.datetime.strptime(f'2002-{a.month}-{a.day}', '%Y-%m-%d'))
	cus['Tỉnh/TP'] = cus['Tỉnh/TP'].apply(lambda a: a.strip() if pd.notna(a) else pd.NaT)

	# Clean data
	cus['Họ và Tên Khách Hàng'] = cus['Họ và Tên Khách Hàng'].apply(lambda a: ''.join([_ for _ in a if _.isalpha() or _ == ' ']).upper() if pd.notna(a) else a)
	cus['CMND/CCCD'] = cus['CMND/CCCD'].apply(lambda a: a.strip() if pd.notna(a) else a)
	cus['Số điện thoại'] = cus['Số điện thoại'].apply(lambda a: a.strip() if pd.notna(a) else a)

	id_cus = cus[['Họ và Tên Khách Hàng', 'CMND/CCCD', 'Số điện thoại', 'Nhóm Khách hàng']]
	id_cus['Filter'] = id_cus[['Họ và Tên Khách Hàng', 'Nhóm Khách hàng']].apply(lambda a: remove_accent(str(a[0])) if a[1] == 'Doanh nghiệp' and pd.notna(a[1]) else pd.NaT, axis=1)
	id_cus = id_cus.drop_duplicates(subset=['CMND/CCCD', 'Số điện thoại', 'Filter']).reset_index()
	id_cus = id_cus.drop(columns=['Họ và Tên Khách Hàng', 'Nhóm Khách hàng'])

	cus['Filter'] = cus[['Họ và Tên Khách Hàng', 'Nhóm Khách hàng']].apply(lambda a: remove_accent(str(a[0])) if a[1] == 'Doanh nghiệp' else pd.NaT, axis=1)
	cus = cus.merge(id_cus, how='left', on=['Filter', 'CMND/CCCD', 'Số điện thoại'])
	cus['ID CUSTOMER'] = cus[['Họ và Tên Khách Hàng', 'CMND/CCCD', 'Số điện thoại', 'index']].apply(lambda a: get_id_cus(a), axis=1)
	cus = cus.drop(columns=['Filter', 'index'])
	# print(f'Số customer cuối = {len(cus)}')

	# Bỏ thông tin người được bảo hiểm
	cus = cus[[_ for _ in cus.columns if 'người được bảo hiểm' not in _]]

	# Bỏ qua issue trên, làm luôn
	cus.to_csv(f'{rd_out_in}/CustomerInfo.csv', index=False, encoding='utf-8-sig')
	cus.to_csv(f'{pool_in}/CustomerInfo.csv', index=False, encoding='utf-8-sig')

def remove_dup(file=None, data=None):
	if file == 'policy':
		# Transform Policy
		policy = data
		# print('--- Data trước xóa dup ---')
		# print(f"Data Policy: Ngày cập nhật: {policy['Ngày dữ liệu cập nhật'].max()}, SL: {len(policy)}, unique mã HĐ: {len(policy['Mã Hợp đồng'].drop_duplicates())}, unique mã hồ sơ nội bộ: {len(policy['Mã Hồ Sơ Nội Bộ'].drop_duplicates())}")

		# Step 1: Xóa dup theo Mã hồ sơ nội bộ
		policy = policy.drop(columns=['Ngày dữ liệu cập nhật', 'Tên Sản Phẩm', 'Group product', 'FYP Sản phẩm', 'Thời hạn hợp đồng (tháng)', 'AFYP/APE', 'Phí topup']).drop_duplicates()
		# Xóa dup nhưng giữ lại thông tin phí topup
		policy['Tổng phí top up đã đóng (Tổng topup tất cả thời gian)'] = policy['Tổng phí top up đã đóng (Tổng topup tất cả thời gian)'].fillna(0).astype(int)
		topup = policy.groupby(['Mã Hợp đồng', 'Mã Hồ Sơ Nội Bộ'])['Tổng phí top up đã đóng (Tổng topup tất cả thời gian)'].sum().reset_index()
		policy = policy.drop(columns=['Tổng phí top up đã đóng (Tổng topup tất cả thời gian)']).drop_duplicates()
		policy = policy.merge(topup, how='left', on=['Mã Hợp đồng', 'Mã Hồ Sơ Nội Bộ'])
		
		# Step 2: Xóa dup theo Mã HĐ, mục tiêu là xóa những cặp mã hồ sơ hủy rồi lập lại, chung 1 mã HĐ, data ngày 25/10 là 30 cặp như vậy
		count1= policy.groupby('Mã Hợp đồng')['Mã Hồ Sơ Nội Bộ'].count().reset_index()
		policy = policy.merge(count1, how='left', on='Mã Hợp đồng')
		policy = policy.rename(columns={'Mã Hồ Sơ Nội Bộ_x': 'Mã Hồ Sơ Nội Bộ', 'Mã Hồ Sơ Nội Bộ_y': 'Chung mã HĐ'})

		count2 = policy.loc[policy['Ngày hủy'].isna()].groupby('Mã Hợp đồng')['Mã Hồ Sơ Nội Bộ'].count().reset_index()
		policy = policy.merge(count2, how='left', on='Mã Hợp đồng')
		policy = policy.rename(columns={'Mã Hồ Sơ Nội Bộ_x': 'Mã Hồ Sơ Nội Bộ', 'Mã Hồ Sơ Nội Bộ_y': 'Chung mã HĐ ko hủy'})
		policy['Chung mã HĐ ko hủy'] = policy['Chung mã HĐ ko hủy'].fillna(0)

		# Nhận diện
		x = policy.loc[(policy['Chung mã HĐ'] > 1), 'Mã Hồ Sơ Nội Bộ'].drop_duplicates()
		# print('--- Check vấn đề Trùng mã HĐ ---')
		# print('All chung mã =', len(x), 'Max chung mã =', int(policy['Chung mã HĐ'].max()))
		x = policy.loc[(policy['Chung mã HĐ'] > 1) & (policy['Chung mã HĐ ko hủy'] <= 1), 'Mã Hồ Sơ Nội Bộ'].drop_duplicates()
		y = policy.loc[(policy['Chung mã HĐ'] > 1) & (policy['Chung mã HĐ ko hủy'] <= 1), 'Mã Hợp đồng'].drop_duplicates()  # Có trường hợp lần 1 hủy tạo lại gửi đối tác rồi bị hủy tiếp, nên chung mã hđ ko hủy = 0
		# print('Chung mã nhưng hủy hết còn 1 cái =', len(y), 'cặp', len(x), 'hồ sơ')
		x = policy.loc[(policy['Chung mã HĐ'] > 1) & (policy['Chung mã HĐ ko hủy'] > 1), 'Mã Hồ Sơ Nội Bộ'].drop_duplicates()
		# print('Chung mã nhưng ko hủy =', len(x))
		
		# Xóa
		policy1 = policy.loc[((policy['Chung mã HĐ'] > 1) & (policy['Chung mã HĐ ko hủy'] <= 1))]
		policy2 = policy.loc[~((policy['Chung mã HĐ'] > 1) & (policy['Chung mã HĐ ko hủy'] <= 1))]
		policy1 = policy1.sort_values(by=['Mã Hợp đồng', 'Ngày hủy'], ascending=True)
		# Fix lỗi DR - HĐ hủy thì có người bán mà HĐ không hủy thì ko có người bán
		policy1['Cặp HĐ thiếu người bán'] = policy1['Mã Hợp đồng'].apply(lambda a: policy1.loc[policy1['Mã Hợp đồng'] == a, 'Mã Momi'].isnull().values.any())
		policy1['Mã Momi của cặp HĐ'] = policy1[['Mã Hợp đồng', 'Cặp HĐ thiếu người bán']].apply(lambda a: policy1.loc[(policy1['Mã Hợp đồng'] == a[0]) & (policy1['Mã Momi'].notna()), 'Mã Momi'].tolist()[0] if a[1] else pd.NaT, axis=1)
		# print(policy1.loc[policy1['Cặp HĐ thiếu người bán'], ['Mã Hợp đồng', 'Mã Hồ Sơ Nội Bộ', 'Mã Momi', 'Ngày hủy', 'Mã Momi của cặp HĐ']], '\n')

		policy1['Mã Momi'] = policy1[['Mã Momi', 'Cặp HĐ thiếu người bán', 'Mã Momi của cặp HĐ']].apply(lambda a: a[2] if a[1] else a[0], axis=1)
		# print(policy1.shape)
		# print(policy1[['Mã Hợp đồng', 'Mã Hồ Sơ Nội Bộ', 'Mã Momi', 'Ngày hủy']], '\n')
		
		policy1 = policy1.drop_duplicates(subset='Mã Hợp đồng', keep='last')
		# print(policy1[['Mã Hợp đồng', 'Mã Hồ Sơ Nội Bộ', 'Mã Momi', 'Ngày hủy']])
		
		policy = pd.concat([policy2, policy1])
		policy = policy.drop(columns=['Chung mã HĐ', 'Chung mã HĐ ko hủy', 'Cặp HĐ thiếu người bán', 'Mã Momi của cặp HĐ'])
		
		# Result
		# print(f"PolicyInfo sau xóa dup: {len(policy)}\n")
		return policy
	elif file == 'hlv':
		hdkd_hlv = data
		# Transform HLV: 1. Xóa trùng
		# print(f"Ngày cập nhật của HLV là {hdkd_hlv.loc[0, 'NGÀY CẬP NHẬT'][:10]}, SL: {len(hdkd_hlv)}, SL unique: {len(hdkd_hlv['POLICY CODE'].drop_duplicates())}")
		hdkd_hlv = hdkd_hlv.drop(columns=['NGÀY CẬP NHẬT', 'STT'])
		hdkd_hlv['PRODUCT PRIMIUM'] = hdkd_hlv['PRODUCT PRIMIUM'].astype(int)
		# hdkd_hlv['FYP'] = hdkd_hlv['POLICY CODE'].apply(lambda a: hdkd_hlv.loc[hdkd_hlv['POLICY CODE'] == a, 'PRODUCT PRIMIUM'].sum())
		hdkd_hlv['FYP'] = hdkd_hlv['POLICY PREMIUM']
		hdkd_hlv = hdkd_hlv.drop(columns=['PRODUCT NAME', 'REGULAR PREM', 'PRODUCT PRIMIUM', 'PRODUCT STATUS', 'LIFE ASSURED NAME', 'PREM TERM',
										  'PRODUCT TERM', 'SUM ASSURED', 'RISK COMMENCE DATE', 'NEXT DUE DATE']).drop_duplicates()
		# print(f"SL sau xóa dup: {len(hdkd_hlv)}")
		return hdkd_hlv
	elif file == 'slv':
		hdkd_slv = data
		# Transform SLV
		# print(f"Ngày cập nhật của SLV là {hdkd_slv.loc[0, 'NGÀY CẬP NHẬT'][:10]}, SL: {len(hdkd_slv)}, SL unique 1: {len(hdkd_slv['APPLICATION_NUMBER'].drop_duplicates())}, SL unique 2: {len(hdkd_slv['POLICY_NUMBER'].drop_duplicates())}")
		hdkd_slv = hdkd_slv.drop(columns=['NGÀY CẬP NHẬT', 'NO.'])
		hdkd_slv['PRODUCT PREMIUM'] = hdkd_slv['PRODUCT PREMIUM'].astype(int)
		# hdkd_slv['FYP'] = hdkd_slv['POLICY NUMBER'].apply(lambda a: hdkd_slv.loc[hdkd_slv['POLICY NUMBER'] == a, 'PRODUCT PREMIUM'].sum())
		hdkd_slv['FYP'] = hdkd_slv['POLICY PREMIUM (1) + (2) + (3)']
		# print(f"SL Sun trước xóa dup: {len(hdkd_slv)}")
		hdkd_slv = hdkd_slv.drop(columns=['PRODUCT NAME VIET', 'PRODUCT CODE', 'PRODUCT PREMIUM', 'MAIN PRODUCT PREMIUM', 'SUM ASSURED', 'COVERAGE NUMBER',
										  'PREM TERM', 'PRODUCT TERM',
										  'PRODUCT STATUS', 'COVERAGE STATUS CHANGE DATE']).drop_duplicates()
		# print(f"SL Sun sau xóa dup: {len(hdkd_slv)}")
		return hdkd_slv


def correct_policy(policy):
	manual = pd.read_csv(f'{source_in}/Sửa Policy Manual.csv', low_memory=False, dtype=str)
	policy = policy.merge(manual, how='left', on='Mã Hồ Sơ Nội Bộ')
	policy['Mã Hợp đồng'] = policy[['Mã Hợp đồng', 'POLICY CODE']].apply(lambda a: a[1] if pd.notna(a[1]) else a[0], axis=1)
	policy = policy.drop(columns=['POLICY CODE'])
	return policy

def cross_check_between_col(a):
	lst = list(dict.fromkeys([str(_) for _ in a if pd.notna(_)]))
	if len(lst) > 1:
		return ','.join(lst)
	elif len(lst) == 1:
		return lst[0]

def check_data_policy(hdkd_hlv=None, hdkd_slv=None, policy=None, b3_hdkd=None, b3_ns_hdkd=None, policy_match_HLV=None, policy_match_SLV=None, policy_not_match_HLV=None, policy_not_match_SLV=None):
	# Check lại data
	# print(', '.join([f'{_}=None' for _ in kwargs.keys()]))

	# 1. Check vấn đề rớt data khi sau khi cắt nhỏ
	print('--- Data Policy và Đối tác sau xóa dup ---')
	print(f"policy_match_HLV = {len(policy_match_HLV)}, policy_match_SLV={len(policy_match_SLV)}, policy_not_match_HLV={len(policy_not_match_HLV)}, policy_not_match_SLV={len(policy_not_match_SLV)}")
	print(f"Data có bị rớt không: {not(len(policy) == sum([len(policy_match_HLV), len(policy_match_SLV), len(policy_not_match_HLV), len(policy_not_match_SLV)]))}")

	# 2. Data hlv slv not match
	print('Số HLV not match =', len(hlv_not_match))
	print('Số SLV not match =', len(slv_not_match))

	# 3. Check vấn đề Trùng mã HĐ
	print('policy HLV =', len(policy.loc[policy['Đối tác'] == 'Hanwha Life', 'Mã Hợp đồng']), 'policy_match_HLV =', len(policy_match_HLV), 'drop dup =', len(policy_match_HLV.drop_duplicates(subset=['Mã Hợp đồng'])))
	print('policy SLV =', len(policy.loc[policy['Đối tác'] == 'Sun Life VN', 'Mã Hợp đồng']), 'policy_match_SLV =', len(policy_match_SLV), 'drop dup =', len(policy_match_SLV.drop_duplicates(subset=['Mã Hợp đồng'])))
	print('policy HLV =', len(policy.loc[(policy['Đối tác'] == 'Hanwha Life') & (policy['Mã Hợp đồng'].notna()), 'Mã Hồ Sơ Nội Bộ']), 'policy_match_HLV =', len(policy_match_HLV))
	print('policy SLV =', len(policy.loc[(policy['Đối tác'] == 'Sun Life VN') & (policy['Mã Hợp đồng'].notna()), 'Mã Hồ Sơ Nội Bộ']), 'policy_match_SLV =', len(policy_match_SLV))
	print('policy HLV =', len(policy.loc[(policy['Đối tác'] == 'Hanwha Life') & (policy['Mã Hợp đồng'].notna()), 'Mã Hợp đồng'].drop_duplicates()), 'policy_match_HLV =', len(policy_match_HLV))
	print('policy SLV =', len(policy.loc[(policy['Đối tác'] == 'Sun Life VN') & (policy['Mã Hợp đồng'].notna()), 'Mã Hợp đồng'].drop_duplicates()), 'policy_match_SLV =', len(policy_match_SLV))
	x = policy.loc[(policy['Đối tác'] == 'Sun Life VN') & (policy['Mã Hợp đồng'].notna()), ['Mã Hợp đồng']].drop_duplicates().reset_index(drop=True)
	y = policy_match_SLV[['Mã Hợp đồng']].drop_duplicates().reset_index(drop=True)
	x['key_x'] = 1
	y['key_y'] = 1
	print(x)
	print(y)
	x = y.merge(x, how='outer', on='Mã Hợp đồng')
	print(x)
	print(x.loc[x['key_x'] != x['key_y']])

def crosscheck_hdkd():
	policy = pd.read_csv(f'{rd_in_in}/PolicyInfo.csv', low_memory=False, dtype=str)
	adv = pd.read_csv(f'{rd_in_in}/AdvisorInfo.csv', low_memory=False, dtype=str)
	hdkd = crosscheck_hdkd_calculate(policy=policy, adv=adv)
	hdkd.to_csv(f'{rd_out_in}/Policy.csv', index=False, encoding='utf-8-sig')
	hdkd.to_csv(f'{pool_in}/Policy.csv', index=False, encoding='utf-8-sig')

def transform_doitac(doitac=None):
	""" Chuẩn hóa data đối tác và chuyển format thành 1 HĐ 1 dòng """
	columns_dict = {'hlv': {'POLICY CODE': 'Policy Id',												'POLICY STATUS': 'Policy Status',	'APPLICATION STATUS': 'Application Status', 'SERVICING AGENT': 'Agent Code',		'SERVICING AGENT NAME': 'Agent Name',	'SUBMISSION DATE': 'NGÀY NỘP', 'ISSUE DATE': 'NGÀY PH', 'ACKNOWLEDGEMENT DATE': 'NGÀY ACK', 'POLICY PREMIUM': 'FYP', 'PRODUCT PRIMIUM': 'FYP SP', 'TOPUP PREM': 'Top up', 'FREQUENCY': 'FREQUENCY'},
					'slv': {'POLICY NUMBER': 'Policy Id',	'APPLICATION NUMBER': 'Application Id', 'POLICY STATUS': 'Policy Status', 												'SERVICING AGENT CODE': 'Agent Code',	'SERVICING AGENT NAME': 'Agent Name',	'APPLICATION RECEIVE DATE': 'NGÀY NỘP', 'SETTLED DATE': 'NGÀY PH', 'ACKNOWLEDGEMENT DATE': 'NGÀY ACK', 'POLICY PREMIUM (1) + (2) + (3)': 'FYP', 'TOPUP PREM (2)': 'Top up', 'FREQUENCY': 'FREQUENCY'},
					'clv': {'Policy Code': 'Policy Id',		'Application Code': 'Application Id', 	'Policy Status': 'Policy Status',												'Agent_code': 'Agent Code',				'Agent Name': 'Agent Name',				'Submit Date': 'NGÀY NỘP', 'Issue Date': 'NGÀY PH', 'ePolicy Confirm Date': 'NGÀY ACK', 'FYTP RYTP Total': 'FYP SP', 'Top Up Total': 'Top up SP', 'Payment Frequency': 'FREQUENCY'},
					'clv-submit': {							'APPLICATION_CODE': 'Application Id', 										'APPLICATION_STATUS': 'Application Status', 'AGENT_CODE': 'Agent Code',				'AGENT_NAME': 'Agent Name', 'SUBMISSION_DATE': 'NGÀY NỘP', 'TARGET_PREMIUM': 'FYP', 'FYP': 'FYP incl.Top up', 'PAYMENT_MODE': 'FREQUENCY'}
				   }
	data_doitac = pd.read_csv(f'{rd_in_in}/Data_{doitac.upper().replace("-", "_")}.csv', low_memory=False, dtype=str)
	data_doitac = data_doitac.rename(columns=columns_dict[doitac])

	# Xử lý FYP
	if doitac == 'hlv':
		data_doitac['FYP SP'] = data_doitac['FYP SP'].apply(lambda a: int(a) if pd.notna(a) else 0)
		data_doitac['FYP'] = data_doitac[['Policy Id', 'Policy Status', 'FYP', 'FYP SP']].apply(lambda a: data_doitac.loc[data_doitac['Policy Id'] == a[0], 'FYP SP'].sum() if a[1] in ['Waiting for Validate', 'Terminated'] else a[2], axis=1)
	elif doitac == 'clv':  # Đối với Chubb, chỉ có 1 cột FYP và nếu HĐ hủy thì sẽ có 2 dòng FYP - dương và âm, cộng lại bằng 0
		data_doitac['FYP SP'] = data_doitac['FYP SP'].apply(lambda a: int(a) if pd.notna(a) else 0)
		data_doitac['FYP'] = data_doitac[['Policy Id', 'Policy Status']].apply(lambda a: data_doitac.loc[data_doitac['Policy Id'] == a[0], 'FYP SP'].sum() if a[1] == 'INFORCE' else
																						 data_doitac.loc[(data_doitac['Policy Id'] == a[0]) & (data_doitac['FYP SP'] > 0), 'FYP SP'].sum() if a[1] == 'TERMINATED' else pd.NaT, axis=1)
	if 'Top up SP' in data_doitac.columns:
		data_doitac['Top up SP'] = data_doitac['Top up SP'].apply(lambda a: int(a) if pd.notna(a) else 0)
		data_doitac['Top up'] = data_doitac['Policy Id'].apply(lambda a: data_doitac.loc[data_doitac['Policy Id'] == a, 'Top up SP'].sum())
	elif 'Top up' in data_doitac.columns:
		data_doitac['Top up'] = data_doitac['Top up'].apply(lambda a: int(a) if pd.notna(a) else 0)

	data_doitac['FREQUENCY'] = data_doitac['FREQUENCY'].apply(lambda a: 'Yearly' if a == '12' or a == 12 else 'Half Yearly' if a == '6' or a == 6 else 'Quarterly' if a == '3' or a == 3 else a.title() if pd.notna(a) else pd.NaT)
	data_doitac['LOẠI HĐ'] = 'CLV' if doitac == 'clv-submit' else doitac.upper()

	# Xử lý trạng thái Hủy trước PH
	if doitac == 'hlv':
		data_doitac['Đối tác Hủy trước PH'] = data_doitac['Application Status'].apply(lambda a: a if a in ['Withdrawn', 'Postponed', 'Declined'] else pd.NaT)
		data_doitac['Đối tác Hủy sau PH'] = data_doitac['Policy Status'].apply(lambda a: a if a in ['Terminated'] else pd.NaT)
		data_doitac['Mất hiệu lực'] = data_doitac['Policy Status'].apply(lambda a: a if a in ['Lapsed'] else pd.NaT)
		data_doitac['Hủy theo Đối tác'] = data_doitac[['Đối tác Hủy trước PH', 'Đối tác Hủy sau PH', 'Mất hiệu lực']].apply(lambda a: a[0] if pd.notna(a[0]) else a[1] if pd.notna(a[1]) else a[2] if pd.notna(a[2]) else pd.NaT, axis=1)
	elif doitac == 'slv':
		data_doitac['Đối tác Hủy trước PH'] = data_doitac['Policy Status'].apply(lambda a: a if a in ['REJECTED'] else pd.NaT)
		data_doitac['Đối tác Hủy sau PH'] = data_doitac['Policy Status'].apply(lambda a: a if a in ['NOT TAKEN'] else pd.NaT)
		data_doitac['Mất hiệu lực'] = data_doitac['Policy Status'].apply(lambda a: a if a in ['LAPSED'] else pd.NaT)
		data_doitac['Hủy theo Đối tác'] = data_doitac[['Đối tác Hủy trước PH', 'Đối tác Hủy sau PH', 'Mất hiệu lực']].apply(lambda a: a[0] if pd.notna(a[0]) else a[1] if pd.notna(a[1]) else a[2] if pd.notna(a[2]) else pd.NaT, axis=1)
	elif doitac == 'clv':
		data_doitac['Đối tác Hủy trước PH'] = pd.NaT
		data_doitac['Đối tác Hủy sau PH'] = data_doitac['Policy Status'].apply(lambda a: a if a in ['TERMINATED'] else pd.NaT)  # Có trong BC CLV PH thì chắc chắn đã được PH
		data_doitac['Mất hiệu lực'] = pd.NaT
		data_doitac['Hủy theo Đối tác'] = data_doitac[['Đối tác Hủy trước PH', 'Đối tác Hủy sau PH', 'Mất hiệu lực']].apply(lambda a: a[0] if pd.notna(a[0]) else a[1] if pd.notna(a[1]) else a[2] if pd.notna(a[2]) else pd.NaT, axis=1)
	elif doitac == 'clv-submit':
		data_doitac['Đối tác Hủy trước PH'] = data_doitac['Application Status'].apply(lambda a: a if a in ['Withdrawal before Accept', 'Reject, waiting for refund'] else pd.NaT)
		data_doitac['Đối tác Hủy sau PH'] = pd.NaT
		data_doitac['Mất hiệu lực'] = pd.NaT
		data_doitac['Hủy theo Đối tác'] = data_doitac[['Đối tác Hủy trước PH', 'Đối tác Hủy sau PH', 'Mất hiệu lực']].apply(lambda a: a[0] if pd.notna(a[0]) else a[1] if pd.notna(a[1]) else a[2] if pd.notna(a[2]) else pd.NaT, axis=1)	

	data_doitac['Application Id'] = data_doitac['Policy Id'] if doitac == 'hlv' else data_doitac['Application Id']

	if doitac in ['hlv', 'slv', 'clv']:
		data_doitac = data_doitac[['Policy Id', 'Application Id', 'LOẠI HĐ', 'NGÀY NỘP', 'NGÀY PH', 'NGÀY ACK', 'FYP', 'Top up', 'FREQUENCY', 'Hủy theo Đối tác', 'Đối tác Hủy trước PH', 'Đối tác Hủy sau PH', 'Mất hiệu lực', 'Agent Code', 'Agent Name']].drop_duplicates()
	elif doitac == 'clv-submit':
		data_doitac = data_doitac[['Application Id', 'LOẠI HĐ', 'NGÀY NỘP', 'FYP', 'FREQUENCY', 'Hủy theo Đối tác', 'Đối tác Hủy trước PH', 'Đối tác Hủy sau PH', 'Mất hiệu lực', 'Agent Code', 'Agent Name']]

	return data_doitac

def crosscheck_with_adm_and_b3(policy_samo=None, data_doitac=None, io=False, doitac=None):
	b3_hdkd = pd.read_csv(f'{rd_in_in}/b3_hdkd.csv', low_memory=False, dtype=str)[['CHECK SỐ HĐ', 'MÃ HỒ SƠ (MOMI)', 'HỦY/ HOÃN', 'NGÀY HỦY/ HOÃN', 'LÝ DO HỦY', 'KÊNH', 'MÃ GT ĐDKD']]
	b3_hdkd['KÊNH'] = b3_hdkd['KÊNH'].apply(lambda a: 'Phượng Hoàng' if a == 'PHƯỢNG HOÀNG' else '3PNVC3P' if a == '3P' else '3PCORP' if a == 'PARTNERSHIP & CORPORATE SALE' else 
													'3P  Finlife' if a == '3P FINLIFE' else 'NVCER' if a == 'ENTERPRISE' else 'SAMO1' if a == 'KHỐI VP SAMO' else a)

	# Fix lỗi Mã Hợp đồng Sun nhiều khi export sai thành Mã hồ sơ 'E' + các lỗi sai mã hợp đồng thành mã hồ sơ nói chung
	policy_samo = policy_samo.merge(data_doitac[['Policy Id', 'Application Id']], how='left', left_on='Mã Hợp đồng', right_on='Application Id')
	policy_samo['Mã Hợp đồng'] = policy_samo[['Mã Hợp đồng', 'Policy Id']].apply(lambda a: a[1] if pd.notna(a[1]) else a[0], axis=1)
	policy_samo = policy_samo.drop(columns=['Policy Id', 'Application Id'])
	# policy_samo.to_csv('test2.csv', index=False, encoding='utf-8-sig')

	# Old
	hdkd = data_doitac.merge(policy_samo[['Mã Hợp đồng', 'Mã Hồ Sơ Nội Bộ', 'Ngày hủy', 'Lý do huỷ', 'Mã đại diện kinh doanh', 'Kênh Kinh Doanh']], how='left', left_on='Policy Id', right_on='Mã Hợp đồng')
	
	# # New
	# data_doitac['Key'] = data_doitac[['Policy Id', 'Application Id']].apply(lambda a: a[0] if pd.notna(a[0]) else a[1], axis=1)
	# policy_samo['Key'] = policy_samo[['Mã Hợp đồng', 'Mã HSYCBH']].apply(lambda a: a[0] if pd.notna(a[0]) else a[1], axis=1)
	# hdkd = data_doitac.merge(policy_samo[['Mã Hợp đồng', 'Mã Hồ Sơ Nội Bộ', 'Mã HSYCBH', 'Ngày hủy', 'Lý do huỷ', 'Mã đại diện kinh doanh', 'Kênh Kinh Doanh', 'Key']], how='left', on='Key')
	# hdkd = hdkd.drop(columns=['Key'])

	hdkd = hdkd.rename(columns={'Mã Hồ Sơ Nội Bộ': 'Id Nội bộ'})

	match1 = hdkd.loc[hdkd['Id Nội bộ'].notna()].merge(b3_hdkd.loc[b3_hdkd['MÃ HỒ SƠ (MOMI)'].notna()], how='inner', left_on='Id Nội bộ', right_on='MÃ HỒ SƠ (MOMI)')
	match2 = hdkd.loc[hdkd['Policy Id'].notna()].merge(b3_hdkd.loc[b3_hdkd['CHECK SỐ HĐ'].notna()], how='inner', left_on='Policy Id', right_on='CHECK SỐ HĐ')
	match3 = hdkd.loc[hdkd['Policy Id'].notna()].merge(b3_hdkd.loc[b3_hdkd['CHECK SỐ HĐ'].notna()], how='inner', left_on='Application Id', right_on='CHECK SỐ HĐ')
	hdkd = pd.concat([match1, match2, match3, hdkd]).drop_duplicates(subset=['Policy Id', 'Id Nội bộ'])

	# Ưu tiên B3
	hdkd['HỦY/ HOÃN'] = hdkd[['Ngày hủy', 'HỦY/ HOÃN', 'Đối tác Hủy trước PH', 'Đối tác Hủy sau PH', 'Mất hiệu lực']].apply(lambda a: 'Hủy trước PH' if a[1] == 'hủy trước PH' else a[1] if pd.notna(a[1]) else
																																	  'Hủy trước PH' if pd.notna(a[2]) else 'Hủy sau PH' if pd.notna(a[3]) else 'Mất hiệu lực' if pd.notna(a[4]) else
																																	  'Hủy theo Policy' if pd.notna(a[0]) else pd.NaT, axis=1)
	hdkd['NGÀY HỦY/ HOÃN'] = hdkd[['Ngày hủy', 'NGÀY HỦY/ HOÃN']].apply(lambda a: a[1] if pd.notna(a[1]) else a[0] if pd.notna(a[0]) else pd.NaT, axis=1)
	hdkd['LÝ DO HỦY'] = hdkd[['Lý do huỷ', 'LÝ DO HỦY']].apply(lambda a: a[1] if pd.notna(a[1]) else a[0] if pd.notna(a[0]) else pd.NaT, axis=1)

	# transform
	hdkd['NGÀY NỘP'] = pd.to_datetime(hdkd['NGÀY NỘP'])
	hdkd['NGÀY PH'] = pd.to_datetime(hdkd['NGÀY PH'])
	hdkd['NGÀY ACK'] = pd.to_datetime(hdkd['NGÀY ACK'])
	hdkd['NGÀY HỦY/ HOÃN'] = pd.to_datetime(hdkd['NGÀY HỦY/ HOÃN'])
	hdkd['THÁNG NỘP'] = hdkd['NGÀY NỘP'].apply(lambda a: a.replace(day=1))
	hdkd['THÁNG PH'] = hdkd['NGÀY PH'].apply(lambda a: a.replace(day=1))

	# Tạm thời lấy data từ B3 - sau đó phải thay đổi
	if not io:
		hdkd['MÃ GT'] = hdkd[['Mã đại diện kinh doanh', 'MÃ GT ĐDKD']].apply(lambda a: a[1] if pd.notna(a[1]) else a[0], axis=1)
		hdkd['KÊNH'] = hdkd[['Kênh Kinh Doanh', 'KÊNH']].apply(lambda a: a[1] if pd.notna(a[1]) else a[0], axis=1)
	else:
		hdkd['MÃ GT'] = hdkd['Mã đại diện kinh doanh']
		hdkd['KÊNH'] = hdkd['Kênh Kinh Doanh']

	# output
	hdkd = hdkd[['Policy Id', 'Id Nội bộ', 'Application Id', 'LOẠI HĐ', 'KÊNH', 'NGÀY NỘP', 'NGÀY PH', 'NGÀY ACK', 'HỦY/ HOÃN', 'Hủy theo Đối tác', 'NGÀY HỦY/ HOÃN', 'LÝ DO HỦY', 'FYP', 'Top up', 'FREQUENCY', 'THÁNG NỘP', 'THÁNG PH', 'MÃ GT', 'Agent Code', 'Agent Name']]
	return hdkd
	
def crosscheck_hlv(policy_samo=None, io=False):
	data_doitac = transform_doitac(doitac='hlv')
	hdkd = crosscheck_with_adm_and_b3(policy_samo=policy_samo, data_doitac=data_doitac, io=io, doitac='hlv')
	hdkd = hdkd.drop_duplicates(subset=['Policy Id'])
	return hdkd

def crosscheck_slv(policy_samo=None, io=False):
	data_doitac = transform_doitac(doitac='slv')
	hdkd = crosscheck_with_adm_and_b3(policy_samo=policy_samo, data_doitac=data_doitac, io=io, doitac='slv')
	hdkd = hdkd.drop_duplicates(subset=['Policy Id'])
	return hdkd

def crosscheck_clv(policy_samo=None, io=False):
	data_doitac = transform_doitac(doitac='clv')
	hdkd = crosscheck_with_adm_and_b3(policy_samo=policy_samo, data_doitac=data_doitac, io=io, doitac='clv')
	return hdkd

def crosscheck_clv_submit(policy_samo=None, io=False):
	# Có nên crosscheck với B3 ?
	data_doitac = transform_doitac(doitac='clv-submit')
	data_doitac['NGÀY NỘP'] = pd.to_datetime(data_doitac['NGÀY NỘP'])
	data_doitac['THÁNG NỘP'] = data_doitac['NGÀY NỘP'].apply(lambda a: a.replace(day=1))
	data_doitac['HỦY/ HOÃN'] = data_doitac[['Đối tác Hủy trước PH', 'Đối tác Hủy sau PH', 'Mất hiệu lực']].apply(lambda a: 'Hủy trước PH' if pd.notna(a[0]) else 'Hủy sau PH' if pd.notna(a[1]) else 'Mất hiệu lực' if pd.notna(a[2]) else pd.NaT, axis=1)
	data_doitac = data_doitac.drop(columns=['Đối tác Hủy trước PH', 'Đối tác Hủy sau PH', 'Mất hiệu lực'])
	return data_doitac


def fix_loi_adm(policy):
	# Fix lỗi Mã Hợp đồng Chubb trên adm thường xuyên bị thiếu số 0 ở đâu - Lưu ý: Mã Hợp đồng Chubb luôn luôn đủ 15 số và bắt đầu bằng '0100', kết thúc bằng '008'
	policy['Mã Hợp đồng'] = policy[['Mã Hợp đồng', 'Đối tác']].apply(lambda a: '0' + a[0] if a[1] == 'Chubb Life' and pd.notna(a[0]) and len(a[0]) == 14 else a[0], axis=1)
	return policy

def crosscheck_hdkd_calculate(policy=None, adv=None, io=False):
	policy = fix_loi_adm(policy)
	policy_hlv = crosscheck_hlv(policy_samo=policy, io=io)
	policy_slv = crosscheck_slv(policy_samo=policy, io=io)
	policy_clv = crosscheck_clv(policy_samo=policy, io=io)
	policy_clv_submit = crosscheck_clv_submit(policy_samo=policy, io=io)
	hdkd_clv = pd.concat([policy_clv, policy_clv_submit]).drop_duplicates(subset=['Application Id'], keep='first')
	hdkd = pd.concat([policy_hlv, policy_slv, hdkd_clv])
	return hdkd


def standardize(file):
	return file

def policy_check():
	'''Receive PolicyInfo as source, check and response issues to source (OP, Tech)
	Relationship is 1 to 1'''
	# input
	policy = pd.read_csv(f'{rd_in_in}/PolicyInfo.csv', low_memory=False, dtype=str)
	# check
	policy = standardize(policy)
	# output
	policy.to_csv(f'{rd_in_in}/PolicyInfo.csv', index=False, encoding='utf-8-sig')


def crosscheck_hdkd_io():
	''' "Bởi vì IO đến 30/11 chỉ thay đổi người bán, thông tin khác như cũ.
	Sau này cũng ko định thay đổi thêm vì nguồn lực Tech có hạn.
	=> Policy IO = Policy Adm + Thay đổi người bán
	Sau đó để ra được Policy theo format chuẩn để chạy Cohort thì đi đường giống như Adm
	(Policy IO + Vá + Đối tác + B3 = Policy IO format chuẩn)"
	'''
	# input
	policy = pd.read_excel(f'{source_io}/PolicyInfo-io.xlsx', dtype=str)
	adv = pd.read_csv(f'{pool_ex}/AdvisorInfo_io_no_code.csv', dtype=str)
	# print(policy.loc[policy['Mã Hồ Sơ Nội Bộ'] == 'NTH-TH-22A9-00031'])

	# Vá policy 1
	policy = va_policy1(policy_data=policy)
	# print(policy.loc[policy['Mã Hồ Sơ Nội Bộ'] == 'NTH-TH-22A9-00031'])

	# Đi đường giống policy
	hdkd = crosscheck_hdkd_calculate(policy=policy, adv=adv, io=True)
	# print(hdkd.loc[hdkd['Id Nội bộ'] == 'NTH-TH-22A9-00031'])
	hdkd.to_csv(f'{pool_ex}/Policy_io_standard_format.csv', index=False, encoding='utf-8-sig')


def crosscheck_doi_soat():
	# Input
	hlv = pd.read_csv(f'{rd_in_in}/DS_Han.csv', low_memory=False)
	slv = pd.read_csv(f'{rd_in_in}/DS_Sun.csv', low_memory=False)
	clv = pd.read_csv(f'{rd_in_in}/DS_Chubb.csv', low_memory=False)

	# Process
	# Transform col name
	col_dict = {}

	# tinh

	# concat
	df = pd.concat([])

	# Output
	df.to_csv(f'{pool_in}/Doi_soat.csv', index=False, encoding='utf-8-sig')