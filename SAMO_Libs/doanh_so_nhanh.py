from .Base_libs_and_options import *
from .constants import *
from .General_func import *
from .Layer1 import *

def Doanh_so_nhanh():  # Có tree hoàn chỉnh, tính rất đơn giản, vd tất cả nhân viên của 1 DSDlà người có mã DSD = người đó
    # Parameters
    lst_position = ['CEO','DSND','DND','DSTD','DTD','DSRD','DRD','DSSD','DSD','DFM','DFT','DFA','DFR']

    # input
    hdkd = pd.read_csv(f'{pool_ex}/hdkd_upgrade.csv', low_memory=False, converters={'Policy Id': str})
    ns = pd.read_csv(f'{pool_ex}/ns_adjusted.csv', low_memory=False, converters={'CMND/CCCD': str})[['Mã đại diện kinh doanh', 'Tên đại diện kinh doanh', 'Trạng thái chuyên viên', 'Chức vụ'] + lst_position[::-1]]
    hdkd_nop = hdkd[['Policy Id', 'Id Nội bộ', 'FYP', 'NGÀY NỘP', 'MÃ GT']].sort_values(by=['FYP'], ascending=False)
    hdkd_nop['NGÀY NỘP'] = pd.to_datetime(hdkd_nop['NGÀY NỘP'])
    hdkd_nop['THÁNG NỘP'] = hdkd_nop['NGÀY NỘP'].apply(lambda a: a.month)
    hdkd_nop['NĂM NỘP'] = hdkd_nop['NGÀY NỘP'].apply(lambda a: a.year)
    hdkd_nop = hdkd_nop[(hdkd_nop['NĂM NỘP'] == 2022)]
    lg.info('Done input')

    # calculate
    ns.insert(loc=4, column='FYP Cá nhân 2022', value=ns.merge(hdkd_nop.groupby('MÃ GT')['FYP'].sum(), how='left', left_on='Mã đại diện kinh doanh', right_on='MÃ GT')['FYP'])
    ns.insert(loc=5, column='FYP Nhánh 2022', value=0)
    lg.info('Done cá nhân năm')

    for _ in range(1, 12):
        ns['THÁNG'] = _
        ns[f'FYP Cá nhân tháng {_}/22'] = ns.merge(hdkd_nop.groupby(['MÃ GT', 'THÁNG NỘP'])['FYP'].sum().reset_index(), how='left', left_on=['Mã đại diện kinh doanh', 'THÁNG'], right_on=['MÃ GT', 'THÁNG NỘP'])['FYP'].fillna(0)
        # lg.info(f'Done cá nhân tháng {_}')
    lg.info('Done cá nhân tháng')
    ns.to_csv('test.csv', index=False, encoding='utf-8-sig')

    for _ in lst_position[::-1]:
        fyp_nhanh = ns.groupby(_)['FYP Cá nhân 2022'].sum().reset_index().rename(columns={_: 'key1', 'FYP Cá nhân 2022': 'key2'})
        ns = ns.merge(fyp_nhanh, how='left', left_on='Mã đại diện kinh doanh', right_on='key1')
        ns.loc[ns['Chức vụ'] == _, 'FYP Nhánh 2022'] = ns.loc[ns['Chức vụ'] == _, 'key2'].fillna(0)
        ns = ns.drop(columns=['key1', 'key2'])
        # lg.info(f'Done nhánh năm, Chức vụ {_}')
        for i in range(1, 12):
            fyp_nhanh = ns.groupby([_, 'THÁNG'])[f'FYP Cá nhân tháng {i}/22'].sum().reset_index().rename(columns={_: 'key1', 'THÁNG': 'key2', f'FYP Cá nhân tháng {i}/22': 'key3'})
            ns = ns.merge(fyp_nhanh, how='left', left_on=['Mã đại diện kinh doanh', 'THÁNG'], right_on=['key1', 'key2'])
            ns.loc[ns['Chức vụ'] == _, f'{i}/22'] = ns.loc[ns['Chức vụ'] == _, 'key3'].fillna(0)
            ns = ns.drop(columns=['key1', 'key2', 'key3'])
            # lg.info(f'Done nhánh tháng {i}, Chức vụ {_}')

    lg.info('Done nhánh tháng')
    ns = ns.drop(columns=['THÁNG'])
    
    ns.to_csv(f'{for_reports_in}/doanh_so_nhanh.csv', index=False, encoding='utf-8-sig')