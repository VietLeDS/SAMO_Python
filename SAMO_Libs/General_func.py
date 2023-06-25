from .Base_libs_and_options import *
from .constants import *

def create_lst_month(start=2019, end=datetime.datetime.now().year):
    this_month = datetime.datetime.now().strftime('%Y-%m-01')
    # this_month = '2022-09-01'
    lst_month = [f'{yyyy}-{str(mm).zfill(2)}-01' for yyyy in range(start, end+1) for mm in range(1, 13) if f'{yyyy}-{str(mm).zfill(2)}-01' <= this_month]
    lst_month = pd.DataFrame(lst_month, columns=['Month'])
    lst_month.to_csv(f'{for_reports_in}/lst_month.csv', index=False, encoding='utf-8-sig')
    return lst_month, this_month
    
def plus_cus(_hdkd, id_noi_bo_col='MÃ HỒ SƠ (MOMI)'):
    _customer_info = pd.read_csv(f'{pool_in}/CustomerInfo.csv', low_memory=False, usecols=['Ngày báo cáo',
       'App', 'Sản phẩm', 'Tổ chức', 'Trạng thái',
       'Họ và Tên Khách Hàng', 'CMND/CCCD', 'Số điện thoại', 'Ngày sinh',
       'Tuổi', 'Giới tính', 'Tỉnh/TP', 'Quận/ Huyện', 'Nghề nghiệp',
       'Nhóm Khách hàng', 'Mã hợp đồng nội bộ'])
    _customer_info = _customer_info[(_customer_info['Sản phẩm'] == 'Bảo hiểm nhân thọ')]
    _hdkd_plus_cus = _hdkd.merge(_customer_info, how='left', left_on=id_noi_bo_col, right_on='Mã hợp đồng nội bộ')
    _hdkd_plus_cus.to_csv(f'{for_reports_in}/hdkd_plus_cus.csv', index=False, encoding='utf-8-sig')
    return _hdkd_plus_cus

def log_info(func):
    def inner(*args, **kwargs):
        lg.info(f'Start {func.__name__}')
        func(*args, **kwargs)
        lg.info(f'Done {func.__name__}')
    return inner

def auto_match_DFA(df, ns, ma_gt_name='MÃ GT', cccd_name='CMND/CCCD KH', ns_ma_gt_name='MÃ GIỚI THIỆU'):  # df này chỉ cần có cột CMND/CCCD
    ns = ns[[ns_ma_gt_name, 'CMND/CCCD']].drop_duplicates(subset='CMND/CCCD')
    ns['CMND/CCCD'] = ns['CMND/CCCD'].apply(lambda a: a.replace("'", ""))
    df_ = df.merge(ns, how='left', left_on=cccd_name, right_on='CMND/CCCD')

    if ma_gt_name == ns_ma_gt_name:
        df_[ma_gt_name] = df_[[f'{ns_ma_gt_name}_x', f'{ns_ma_gt_name}_y']].apply(lambda a: a[1] if pd.notna(a[1]) else a[0], axis=1)
        df_ = df_.drop(columns=[f'{ns_ma_gt_name}_x', f'{ns_ma_gt_name}_y'])
    else:
        df_[ma_gt_name] = df_[[ma_gt_name, ns_ma_gt_name]].apply(lambda a: a[1] if pd.notna(a[1]) else a[0], axis=1)
        df_ = df_.drop(columns=[ns_ma_gt_name])

    if cccd_name != 'CMND/CCCD':
        df_ = df_.drop(columns=['CMND/CCCD'])
    return df_

def identify_change(lst_run=None):
    source = csource[['name', 'path']].drop_duplicates()
    if lst_run is None:
        pass
    elif lst_run is not None:
        source = source.loc[source['name'].isin(lst_run)]
    elif type(lst_run) != list:
        raise Exception('Wrong args')

    last_modified = pd.read_csv(f'{log_path}/last_modified.csv', low_memory=False)
    source['last_modified'] = source['path'].apply(lambda a: datetime.datetime.fromtimestamp(os.path.getmtime(globals()[a])).strftime('%Y-%m-%d %H:%M:%S'))
    last_modified = last_modified.merge(source, how='left', on='path')
    last_modified['check'] = last_modified[['last_modified_x', 'last_modified_y']].apply(lambda a: True if a[0] != a[1] else False, axis=1)
    has_changed = any(last_modified['check'])
    if has_changed:
        last_modified['last_modified'] = last_modified['last_modified_y']
        last_modified = last_modified.drop(columns=['last_modified_x', 'last_modified_y', 'check'])
        # print(last_modified)
        # last_modified.to_csv(f'{log_path}/last_modified.csv', index=False, encoding='utf-8-sig')
    return has_changed

def remove_accent(text):
    return unidecode.unidecode(text)
