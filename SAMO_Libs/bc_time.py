from .Base_libs_and_options import *
from .constants import *
from .General_func import *
from .Layer1 import *

def bc_time():
    hdkd = pd.read_csv(f'{for_reports_in}/b3_hdkd_co_ban_real.csv', low_memory=False, converters={'CHECK SỐ HĐ': str})
    ns = pd.read_csv(f'{pool_in}/b3_ns.csv', low_memory=False)
    ns_hdkd = pd.read_csv(f'{pool_in}/b3_ns_hdkd.csv', low_memory=False, converters={'CHECK SỐ HĐ': str})
    # cohort = pd.read_csv(f'{for_reports_in}/cohort_AA_real.csv', low_memory=False)

    # pnt = pd.read_csv(f'{pool_in}/pnt.csv', low_memory=False, dtype=str)
    dt = pd.read_csv(f'{pool_in}/Data dao tao chi tiet.csv', low_memory=False)
    # hlv = pd.read_csv(f'{pool_in}/Data_HLV.csv', low_memory=False, converters={'POLICY CODE': str})
    # slv = pd.read_csv(f'{pool_in}/Data_SLV.csv', low_memory=False, converters={'APPLICATION_NUMBER': str, 'POLICY_NUMBER': str})

    ns['NGÀY TẠO MÃ GT'] = pd.to_datetime(ns['NGÀY TẠO MÃ GT'])
    ns['NGÀY DEACTIVE'] = pd.to_datetime(ns['NGÀY DEACTIVE'])
    # cohort['Month'] = pd.to_datetime(cohort['Month'])
    dt['Ngày học'] = pd.to_datetime(dt['Ngày học'])

    df = ns[['MÃ GIỚI THIỆU', 'TÊN CHUYÊN VIÊN', 'NGÀY TẠO MÃ GT', 'TÊN KÊNH', 'TÊN CHI NHÁNH', 'TÊN ĐẠI LÝ', 'DSD', 'DSSD', 'DTD', 'DSTD', 'DND', 'DSND']]
    df['TÊN KÊNH'] = df[['TÊN KÊNH', 'TÊN CHI NHÁNH']].apply(lambda a: a[1] if pd.isna(a[0]) or a[0] == '.' or a[0] == 0 or a[0] == '0' else a[0], axis=1)
    df['TÊN KÊNH'] = df[['TÊN KÊNH', 'TÊN ĐẠI LÝ']].apply(lambda a: a[1] if pd.isna(a[0]) or a[0] == '.' or a[0] == 0 or a[0] == '0' else a[0], axis=1)

    for _ in ['S', 'T', 'N']:
        df[f'D{_}D'] = df[[f'D{_}D', f'DS{_}D']].apply(lambda a: a[1] if pd.notna(a[1]) and a[1] != '.' else a[0], axis=1)
        df = df.merge(ns[['MÃ GIỚI THIỆU', 'TÊN CHUYÊN VIÊN']], how='left', left_on=f'D{_}D', right_on='MÃ GIỚI THIỆU')
        df = df.drop(columns=['MÃ GIỚI THIỆU_y'])
        df = df.rename(columns={'TÊN CHUYÊN VIÊN_x': 'TÊN CHUYÊN VIÊN', 'TÊN CHUYÊN VIÊN_y': f'TÊN D{_}D', 'MÃ GIỚI THIỆU_x': 'MÃ GIỚI THIỆU'})
    df[['TÊN DSD', 'TÊN DTD', 'TÊN DND']] = df[['TÊN DSD', 'TÊN DTD', 'TÊN DND']].fillna('.')
    df = df.drop(columns=['DSSD', 'DSTD', 'DSND', 'TÊN CHI NHÁNH', 'TÊN ĐẠI LÝ'])

    dt2 = dt[['Ngày học', 'Mã GT']]
    dt2 = dt2.sort_values(by=['Ngày học']).drop_duplicates()
    dt3 = dt2.drop_duplicates(subset=['Mã GT'], keep='first')
    # dt3.loc[:, 'List ngày học'] = dt3.loc[:, 'Mã GT'].apply(lambda a: dt2.loc[dt2['Mã GT'] == a, 'Ngày học'].values.tolist())
    dt3 = dt3.rename(columns={'Ngày học': 'Ngày học đầu tiên'})

    df = df.merge(dt3, how='left', left_on='MÃ GIỚI THIỆU', right_on='Mã GT')
    df = df.drop(columns=['Mã GT'])
    ns_hdkd2 = ns_hdkd.merge(hdkd[['CHECK SỐ HĐ', 'NGÀY NỘP']], how='left', on='CHECK SỐ HĐ')
    ns_hdkd2 = ns_hdkd2[(ns_hdkd2['NGÀY NỘP'].notna())]
    # df.loc[:, 'List HĐ'] = df.loc[:, 'MÃ GIỚI THIỆU'].apply(lambda a: ns_hdkd2.loc[ns_hdkd2['MÃ GT'] == a, 'CHECK SỐ HĐ'].values.tolist())

    ns_hdkd2 = ns_hdkd2.sort_values(by=['NGÀY NỘP', 'MÃ GT', 'CHECK SỐ HĐ'])
    ns_hdkd2 = ns_hdkd2.drop_duplicates(subset=['MÃ GT'], keep='first')
    df = df.merge(ns_hdkd2[['MÃ GT', 'NGÀY NỘP']], how='left', left_on='MÃ GIỚI THIỆU', right_on='MÃ GT')
    df = df.drop(columns=['MÃ GT'])
    df = df.rename(columns={'NGÀY NỘP': 'NGÀY NỘP ĐẦU TIÊN'})
    df['NGÀY TẠO MÃ GT'] = pd.to_datetime(df['NGÀY TẠO MÃ GT'])
    df['Ngày học đầu tiên'] = pd.to_datetime(df['Ngày học đầu tiên'])
    df['NGÀY NỘP ĐẦU TIÊN'] = pd.to_datetime(df['NGÀY NỘP ĐẦU TIÊN'])
    df['Time 1-2'] = df[['NGÀY TẠO MÃ GT', 'Ngày học đầu tiên']].apply(lambda a: (a[1] - a[0]).days, axis=1)
    df['Time 2-3'] = df[['Ngày học đầu tiên', 'NGÀY NỘP ĐẦU TIÊN']].apply(lambda a: (a[1] - a[0]).days, axis=1)
    df['Time 1-3'] = df[['NGÀY TẠO MÃ GT', 'NGÀY NỘP ĐẦU TIÊN']].apply(lambda a: (a[1] - a[0]).days, axis=1)

    df.to_csv(f'{for_reports_in}/time_real.csv', index=False, encoding='utf-8-sig')