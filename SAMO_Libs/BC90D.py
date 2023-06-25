from .Base_libs_and_options import *
from .constants import *
from .General_func import *
from .Layer1 import *

# 90D
def prepare_data_for_BC90D():
    # Output: lst_channel and lst_bc
    # Get data
    ns = pd.read_csv(f'{pool_in}/b3_ns.csv', low_memory=False)
    
    ns['NGÀY TẠO MÃ GT'] = pd.to_datetime(ns['NGÀY TẠO MÃ GT'])
    ns['NGÀY DEACTIVE'] = pd.to_datetime(ns['NGÀY DEACTIVE'])

    # Đổi ngày tạo mã GT của ns về input for invest để có bc chuẩn
    ns = convert_ngay_tao_ma_GT(ns)
    
    # Create data file
    # Source (RDBMS) -> Python -> Data file -> Excel
    lst_bc = pd.DataFrame(['2022-06-30', '2022-07-31', '2022-08-31'], columns=['NGÀY BC'])
    lst_bc['NGÀY BC'] = pd.to_datetime(lst_bc['NGÀY BC'])
    lst_bc['key'] = 1

    # Convert DSSD, DSRD, DSTD
    df = ns[['TÊN KÊNH', 'TÊN CHI NHÁNH', 'TÊN ĐẠI LÝ', 'MÃ GIỚI THIỆU', 'TÊN CHUYÊN VIÊN', 'NGÀY TẠO MÃ GT', 'NGÀY DEACTIVE', 'VỊ TRÍ', 'DSD', 'DSSD', 'DTD', 'DSTD', 'DND', 'DSND']]
    df['TÊN KÊNH'] = df[['TÊN KÊNH', 'TÊN CHI NHÁNH']].apply(lambda a: a[1] if pd.isna(a[0]) or a[0] == '.' or a[0] == 0 or a[0] == '0' else a[0], axis=1)
    df['TÊN KÊNH'] = df[['TÊN KÊNH', 'TÊN ĐẠI LÝ']].apply(lambda a: a[1] if pd.isna(a[0]) or a[0] == '.' or a[0] == 0 or a[0] == '0' else a[0], axis=1)
    for _ in ['S', 'T', 'N']:
        df[f'D{_}D'] = df[[f'D{_}D', f'DS{_}D']].apply(lambda a: a[1] if pd.notna(a[1]) and a[1] != '.' else a[0], axis=1)
        df = df.merge(ns[['MÃ GIỚI THIỆU', 'TÊN CHUYÊN VIÊN']], how='left', left_on=f'D{_}D', right_on='MÃ GIỚI THIỆU')
        df = df.drop(columns=['MÃ GIỚI THIỆU_y'])
        df = df.rename(columns={'TÊN CHUYÊN VIÊN_x': 'TÊN CHUYÊN VIÊN', 'TÊN CHUYÊN VIÊN_y': f'TÊN D{_}D', 'MÃ GIỚI THIỆU_x': 'MÃ GIỚI THIỆU'})
    df[['TÊN DSD', 'TÊN DTD', 'TÊN DND']] = df[['TÊN DSD', 'TÊN DTD', 'TÊN DND']].fillna('.')
    df = df.drop(columns=['DSSD', 'DSTD', 'DSND'])
    df[['TÊN KÊNH', 'TÊN DSD', 'TÊN DTD', 'TÊN DND']].drop_duplicates().to_csv(f'{for_reports_in}/lst_channel-for-90D-report.csv', index=False, encoding='utf-8-sig')

    process_HD(df=df, lst_bc=lst_bc)
    process_NS(df=df, lst_bc=lst_bc)
    process_AA(df=df, lst_bc=lst_bc)


def convert_ngay_tao_ma_GT(ns=None):  # Đoạn code này chỉ dùng cho 90D, ns_adjusted có code riêng
    # Lấy source trong input for invest
    ns2 = pd.read_csv(f'{source_ex}/input_for_invest.csv', low_memory=False, dtype=str)
    ns2['NGÀY TẠO MÃ GT'] = pd.to_datetime(ns2['NGÀY TẠO MÃ GT'])

    # Xác định Mã GT chuẩn nhất
    ns2['MÃ GT'] = ns2[['MÃ GT', 'MÃ GT Final']].apply(lambda a: a[1] if pd.notna(a[1]) and a[1] != '0' and a[1] != 0 else a[0], axis=1)
    ns4 = pd.read_csv(f'{rd_in_in}/b3_ns.csv', low_memory=False, dtype=str)
    ns2 = auto_match_DFA(ns2, ns4)
    ns = ns.merge(ns2[['MÃ GT', 'NGÀY TẠO MÃ GT']], how='left', left_on='MÃ GIỚI THIỆU', right_on='MÃ GT')
    ns['NGÀY TẠO MÃ GT'] = ns[['NGÀY TẠO MÃ GT_x', 'NGÀY TẠO MÃ GT_y']].apply(lambda a: a[1] if pd.notna(a[1]) else a[0], axis=1)
    ns = ns.drop(columns=['NGÀY TẠO MÃ GT_x', 'NGÀY TẠO MÃ GT_y', 'MÃ GT'])
    return ns


def process_HD(df=None, lst_bc=None):
    hdkd = pd.read_csv(f'{for_reports_in}/b3_hdkd_co_ban_real.csv', low_memory=False, converters={'CHECK SỐ HĐ': str})

    hdkd['NGÀY NỘP'] = pd.to_datetime(hdkd['NGÀY NỘP'])
    hdkd['NGÀY PH'] = pd.to_datetime(hdkd['NGÀY PH'])
    hdkd['NGÀY HỦY/ HOÃN'] = pd.to_datetime(hdkd['NGÀY HỦY/ HOÃN'])

    data1 = hdkd.merge(df[['TÊN KÊNH', 'TÊN DSD', 'TÊN DTD', 'TÊN DND', 'MÃ GIỚI THIỆU']], how='left', left_on='MÃ GT', right_on='MÃ GIỚI THIỆU')
    data1['key'] = 1
    data1 = data1.merge(lst_bc, on='key')
    data1 = data1.drop(columns=['MÃ GIỚI THIỆU', 'key'])
    data1 = data1[(data1['NGÀY NỘP'] <= data1['NGÀY BC'])]

    data1['PH trong 7 ngày'] = data1[['NGÀY PH', 'NGÀY BC', 'SỐ HĐ PH']].apply(lambda a: 1 if a[2] == 1 and (a[1] - a[0]).days < 7 and (a[1] - a[0]).days >= 0 else 0, axis=1)
    data1['PH trong 30 ngày'] = data1[['NGÀY PH', 'NGÀY BC', 'SỐ HĐ PH']].apply(lambda a: 1 if a[2] == 1 and (a[1] - a[0]).days < 30 and (a[1] - a[0]).days >= 0 else 0, axis=1)
    data1['PH trong 90 ngày'] = data1[['NGÀY PH', 'NGÀY BC', 'SỐ HĐ PH']].apply(lambda a: 1 if a[2] == 1 and (a[1] - a[0]).days < 90 and (a[1] - a[0]).days >= 0 else 0, axis=1)

    data1['NỘP trong 7 ngày'] = data1[['NGÀY NỘP', 'NGÀY BC', 'SỐ HĐ NỘP']].apply(lambda a: 1 if a[2] == 1 and (a[1] - a[0]).days < 7 and (a[1] - a[0]).days >= 0 else 0, axis=1)
    data1['NỘP trong 30 ngày'] = data1[['NGÀY NỘP', 'NGÀY BC', 'SỐ HĐ NỘP']].apply(lambda a: 1 if a[2] == 1 and (a[1] - a[0]).days < 30 and (a[1] - a[0]).days >= 0 else 0, axis=1)
    data1['NỘP trong 90 ngày'] = data1[['NGÀY NỘP', 'NGÀY BC', 'SỐ HĐ NỘP']].apply(lambda a: 1 if a[2] == 1 and (a[1] - a[0]).days < 90 and (a[1] - a[0]).days >= 0 else 0, axis=1)

    data1['HỦY trong 7 ngày'] = data1[['NGÀY HỦY/ HOÃN', 'NGÀY BC', 'SỐ HĐ HỦY']].apply(lambda a: 1 if a[2] == 1 and (a[1] - a[0]).days < 7 and (a[1] - a[0]).days >= 0 else 0, axis=1)
    data1['HỦY trong 30 ngày'] = data1[['NGÀY HỦY/ HOÃN', 'NGÀY BC', 'SỐ HĐ HỦY']].apply(lambda a: 1 if a[2] == 1 and (a[1] - a[0]).days < 30 and (a[1] - a[0]).days >= 0 else 0, axis=1)
    data1['HỦY trong 90 ngày'] = data1[['NGÀY HỦY/ HOÃN', 'NGÀY BC', 'SỐ HĐ HỦY']].apply(lambda a: 1 if a[2] == 1 and (a[1] - a[0]).days < 90 and (a[1] - a[0]).days >= 0 else 0, axis=1)

    data1.to_csv(f'{for_reports_in}/hd-for-90D-report.csv', index=False, encoding='utf-8-sig')
    lg.info('Done HD')

def process_NS(df=None, lst_bc=None):
    # Data2 - keys + NS
    df['key'] = 1
    data2 = df.merge(lst_bc, on='key')
    data2 = data2[(data2['NGÀY TẠO MÃ GT'] <= data2['NGÀY BC'])]
    
    dt = pd.read_csv(f'{pool_in}/Data dao tao chi tiet.csv', low_memory=False)
    dt['Ngày học'] = pd.to_datetime(dt['Ngày học'])
    dt2 = dt.sort_values(by=['Ngày học', 'Giờ học'])
    dt2 = dt2.drop_duplicates(subset=['Mã GT'], keep='first')
    dt2 = dt2.rename(columns={'Ngày học': 'Ngày học đầu tiên'})
    data2 = data2.merge(dt2, how='left', left_on='MÃ GIỚI THIỆU', right_on='Mã GT')
    data2 = data2.drop(columns=['key', 'Mã GT'])

    data2['TUYỂN TRONG 90D'] = data2[['NGÀY TẠO MÃ GT', 'NGÀY BC']].apply(lambda a: 1 if (a[1] - a[0]).days < 90 and (a[1] - a[0]).days >= 0 else 0, axis=1)
    data2['NGHỈ TRONG 90D'] = data2[['NGÀY DEACTIVE', 'NGÀY BC']].apply(lambda a: 1 if (a[1] - a[0]).days < 90 and (a[1] - a[0]).days >= 0 else 0, axis=1)

    data2['HỌC trong 7 ngày'] = data2[['Ngày học đầu tiên', 'NGÀY BC', 'TUYỂN TRONG 90D']].apply(lambda a: 1 if a[2] == 1 and (a[1] - a[0]).days < 7 and (a[1] - a[0]).days >= 0 else 0, axis=1)
    data2['HỌC trong 30 ngày'] = data2[['Ngày học đầu tiên', 'NGÀY BC', 'TUYỂN TRONG 90D']].apply(lambda a: 1 if a[2] == 1 and (a[1] - a[0]).days < 30 and (a[1] - a[0]).days >= 0 else 0, axis=1)
    data2['HỌC trong 90 ngày'] = data2[['Ngày học đầu tiên', 'NGÀY BC', 'TUYỂN TRONG 90D']].apply(lambda a: 1 if a[2] == 1 and (a[1] - a[0]).days < 90 and (a[1] - a[0]).days >= 0 else 0, axis=1)

    data2.to_csv(f'{for_reports_in}/ns-for-90D-report.csv', index=False, encoding='utf-8-sig')
    lg.info('Done NS')

def process_AA(df=None, lst_bc=None):
    cohort = pd.read_csv(f'{for_reports_in}/cohort_AA_real_for_mapa.csv', low_memory=False)
    cohort['Month'] = pd.to_datetime(cohort['Month'])

    # Data3 - keys + ns_hdkd -> AA6, AA1
    data3 = cohort[( (cohort['Month'] >= '2022-04-01') & (cohort['STT Tháng làm việc'] >= 1) )].merge(df[['TÊN KÊNH', 'TÊN DSD', 'TÊN DTD', 'TÊN DND', 'MÃ GIỚI THIỆU']], 
        how='left', on='MÃ GIỚI THIỆU')
    data3['key'] = 1
    data3 = data3.merge(lst_bc, on='key')
    data3['filter'] = data3[['Month', 'NGÀY BC']].apply(lambda a: 1 if (a[1]-a[0]).days < 93 and (a[1]-a[0]).days >= 0 else 0, axis=1)
    data3 = data3[(data3['filter'] == 1)]
    data3 = data3.drop(columns=['key', 'filter'])
    data3.to_csv(f'{for_reports_in}/AA-cohort-for-90D-report.csv', index=False, encoding='utf-8-sig')
    lg.info('Done AA')