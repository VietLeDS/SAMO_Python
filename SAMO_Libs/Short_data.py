from .Base_libs_and_options import *
from .constants import *
from .General_func import *

def hdkd_co_ban_to_csv():
    # LƯU Ý: KÊNH trong hdkd là KÊNH LỊCH SỬ còn KÊNH trong ns là KÊNH mới nhất mà NS đó đang làm
    hdkd = pd.read_csv(f'{pool_in}/Policy.csv', low_memory=False, dtype=str)
    ns = pd.read_csv(f'{pool_in}/b3_ns.csv', low_memory=False)
    ns_hdkd = pd.read_csv(f'{pool_in}/b3_ns_hdkd.csv', low_memory=False, converters={'CHECK SỐ HĐ': str})

    hdkd['NGÀY NỘP'] = pd.to_datetime(hdkd['NGÀY NỘP'])
    hdkd['NGÀY PH'] = pd.to_datetime(hdkd['NGÀY PH'])
    hdkd['NGÀY ACK'] = pd.to_datetime(hdkd['NGÀY ACK'])
    hdkd['NGÀY HỦY/ HOÃN'] = pd.to_datetime(hdkd['NGÀY HỦY/ HOÃN'])

    basic_hdkd = hdkd[(~hdkd['Policy Id'].isna())]

    basic_hdkd['THÁNG HỦY/ HOÃN'] = basic_hdkd['NGÀY HỦY/ HOÃN'].apply(lambda a: a.replace(day=1) if pd.notna(a) else a)

    basic_hdkd['NĂM NỘP'] = basic_hdkd['NGÀY NỘP'].apply(lambda a: a.year)
    basic_hdkd['NĂM PH'] = basic_hdkd['NGÀY PH'].apply(lambda a: a.year if pd.notna(a) else a)
    basic_hdkd['NĂM HỦY/ HOÃN'] = basic_hdkd['NGÀY HỦY/ HOÃN'].apply(lambda a: a.year if pd.notna(a) else a)

    basic_hdkd.loc[basic_hdkd['NGÀY NỘP'].notna(), 'SỐ HĐ NỘP'] = 1
    basic_hdkd.loc[ (basic_hdkd['NGÀY PH'].notna()) & (basic_hdkd['HỦY/ HOÃN'] != "Hủy trước PH"), 'SỐ HĐ PH'] = 1
    basic_hdkd.loc[ (basic_hdkd['HỦY/ HOÃN'] == "Hủy trước PH") | (basic_hdkd['HỦY/ HOÃN'] == "Hủy sau PH") | (basic_hdkd['HỦY/ HOÃN'] == "Hủy"), 'SỐ HĐ HỦY'] = 1
    basic_hdkd.loc[ (basic_hdkd['SỐ HĐ PH'] == 1) & (basic_hdkd['NGÀY ACK'].notna()), 'SỐ HĐ ACK'] = 1

    basic_hdkd.to_csv(f'{for_reports_in}/b3_hdkd_co_ban.csv', index=False, encoding='utf-8-sig')
    lg.info(f'Done hdkd co ban')