from .Base_libs_and_options import *
from .constants import *
from .General_func import *
from .Layer1 import *

# cohort
def cohort_AA(additional_col=None, month_min=None, for_mapa=False):
    hdkd = pd.read_csv(f'{pool_in}/Policy.csv', low_memory=False, converters={'Policy Id': str})
    ns = pd.read_csv(f'{pool_in}/AdvisorInfo_no_code.csv', low_memory=False)
    ns_hdkd = hdkd[['Policy Id', 'MÃ GT']]

    pnt = pd.read_csv(f'{pool_in}/pnt.csv', low_memory=False, dtype=str)
    dt = pd.read_csv(f'{pool_in}/Data dao tao chi tiet.csv', low_memory=False)
    hdkd['NGÀY NỘP'] = pd.to_datetime(hdkd['NGÀY NỘP'])
    hdkd['NGÀY PH'] = pd.to_datetime(hdkd['NGÀY PH'])
    hdkd['NGÀY HỦY/ HOÃN'] = pd.to_datetime(hdkd['NGÀY HỦY/ HOÃN'])
    ns['Ngày tạo chuyên viên'] = pd.to_datetime(ns['Ngày tạo chuyên viên'])
    ns['Ngày tạo code Life đầu tiên'] = pd.to_datetime(ns['Ngày tạo code Life đầu tiên'])
    dt['Ngày học'] = pd.to_datetime(dt['Ngày học'])

    def khung_cohort(additional_col=None, month_min=None):
        lst_month, this_month = create_lst_month()

        if additional_col is not None:
            lst_ns = ns[['Mã đại diện kinh doanh', 'Ngày tạo code Life đầu tiên'] + additional_col].loc[ns['Ngày tạo code Life đầu tiên'].notna()]
            lst_ns.loc[len(lst_ns)] = ['.', '2019-01-01'] + ['.' for _ in additional_col]
        else:
            lst_ns = ns[['Mã đại diện kinh doanh', 'Ngày tạo code Life đầu tiên']].loc[ns['Ngày tạo code Life đầu tiên'].notna()]
            lst_ns.loc[len(lst_ns)] = ['.', '2019-01-01']
        lst_ns['Ngày tạo code Life đầu tiên'] = pd.to_datetime(lst_ns['Ngày tạo code Life đầu tiên'])
        lst_ns['THÁNG TẠO MÃ GT'] = lst_ns['Ngày tạo code Life đầu tiên'].apply(lambda a: str(a)[:7] + '-01')

        lst_ns['key'] = 1
        lst = lst_month['Month'].tolist()
        lst_ns['STT THÁNG TẠO MÃ GT'] = lst_ns['THÁNG TẠO MÃ GT'].apply(lambda a: lst.index(a))

        lst_month['key'] = 1
        lst_month['STT Tháng'] = lst_month.index

        # Khâu này giảm từ 7s xuống 0.02s, nhờ tạo thêm 2 cột int sau đó trừ nhau ra STT Tháng làm việc rất dễ dàng, thay vì xử lý str
        cohort = lst_month.merge(lst_ns, on='key')
        cohort = cohort.loc[cohort['Month'] <= this_month, :]
        cohort.insert(loc=5, column='STT Tháng làm việc', value=cohort['STT Tháng'] - cohort['STT THÁNG TẠO MÃ GT'] + 1)
        cohort = cohort.drop(columns=['key', 'STT Tháng', 'STT THÁNG TẠO MÃ GT'])

        # Thay đổi STT cột
        cohort = cohort.rename(columns={'THÁNG TẠO MÃ GT': 'THÁNG TẠO MÃ GT 2'})
        cohort.insert(loc=3, column='THÁNG TẠO MÃ GT', value=cohort['THÁNG TẠO MÃ GT 2'])
        cohort = cohort.drop(columns=['THÁNG TẠO MÃ GT 2'])

        cohort = cohort.sort_values(by=['Ngày tạo code Life đầu tiên', 'Mã đại diện kinh doanh', 'Month'])
        print(f'Len cohort = {len(cohort)}')
        cohort.to_csv(f'{for_reports_in}/khung_cohort_in.csv', index=False, encoding='utf-8-sig')
        lg.info('Done khung_cohort')
        return cohort

    def so_hd_BHNT(cohort):
        # SỐ HĐ và Active tính theo ngày nộp, FYP tính theo phát hành
        # Phương án là Chỉ lấy những HĐ đã phát hành nhưng quy ngày về ngày nộp
        hdkd['THÁNG'] = hdkd['THÁNG NỘP']
        hdkd.loc[hdkd['NGÀY NỘP'].notna(), 'SỐ HĐ NỘP'] = 1
        hdkd.loc[ (hdkd['NGÀY PH'].notna()) & (hdkd['HỦY/ HOÃN'] != "Hủy trước PH"), 'SỐ HĐ'] = 1
        # Đổi FYP NỘP thành FYP PH
        hdkd.loc[ (hdkd['NGÀY PH'].isna()) | (hdkd['HỦY/ HOÃN'] == "Hủy trước PH"), 'FYP'] = 0
        hdkd['MÃ GT'] = hdkd['MÃ GT'].fillna('.')

        count_hd = pd.DataFrame(hdkd.groupby(by=['THÁNG', 'MÃ GT'])[['SỐ HĐ NỘP', 'SỐ HĐ', 'FYP']].sum()).reset_index()
        count_hd['key'] = count_hd[['THÁNG', 'MÃ GT']].apply(lambda a: a[0] + a[1], axis=1)
        count_hd['List HĐ'] = count_hd[['THÁNG', 'MÃ GT']].apply(lambda a: 
                               hdkd.loc[ (hdkd['THÁNG'] == a[0]) & (hdkd['MÃ GT'] == a[1])
                               & (hdkd['NGÀY NỘP'].notna()), 'Policy Id'].values.tolist(), axis=1)

        # Chính là vấn đề ns bị duplicates và lại nhô ra 1 đám có HĐ trước khi gia nhập, max tới -11 tháng
        cohort['key'] = cohort[['Month', 'Mã đại diện kinh doanh']].apply(lambda a: a[0] + a[1], axis=1)
        cohort = cohort.merge(count_hd, how='left', on='key')

        cohort = cohort.drop(columns=['THÁNG', 'MÃ GT'])
        cohort = cohort.rename(columns={'SỐ HĐ': 'Số HĐ BHNT'})
        cohort['SỐ HĐ NỘP'] = cohort['SỐ HĐ NỘP'].fillna(0)
        cohort['Số HĐ BHNT'] = cohort['Số HĐ BHNT'].fillna(0)
        cohort['FYP'] = cohort['FYP'].fillna(0)
        return cohort

    def so_hd_BHPNT(cohort):
        count_hd = pnt[['Month', 'Số hợp đồng VBI', 'mã cv']]
        count_hd = count_hd[(count_hd['Số hợp đồng VBI'].notna())]
        count_hd['SỐ HĐ'] = 1
        count_hd = pd.DataFrame(count_hd.groupby(by=['Month', 'mã cv'])['SỐ HĐ'].sum()).reset_index()
        count_hd['key'] = count_hd[['Month', 'mã cv']].apply(lambda a: str(a[0])[:10] + str(a[1]), axis=1)
        cohort = cohort.merge(count_hd, how='left', on='key')
        cohort = cohort.drop(columns=['Month_y', 'mã cv', 'key'])
        cohort = cohort.rename(columns={'SỐ HĐ': 'Số HĐ BHPNT', 'Month_x': 'Month'})
        cohort['Số HĐ BHPNT'] = cohort['Số HĐ BHPNT'].fillna(0)
        return cohort

    def so_hd_total(cohort):
        if for_mapa:  # Nếu for mapa thì Life only, nếu không thì tính cả HĐPNT
            cohort['SỐ HĐ'] = cohort['SỐ HĐ NỘP']
        else:
            cohort['SỐ HĐ'] = cohort['SỐ HĐ NỘP'] + cohort['Số HĐ BHPNT']
        cohort['SỐ HĐ'] = cohort['SỐ HĐ'].fillna(0)
        cohort = cohort.drop(columns=['Số HĐ BHPNT'])
        cohort = cohort.rename(columns={'Số HĐ BHNT': 'SỐ HĐ PH'})
        # Filter bỏ những cột trước khi gia nhập mà ko có HĐ cho nhẹ cohort
        cohort = cohort[(cohort[['STT Tháng làm việc', 'SỐ HĐ']].apply(lambda a: False if a[0] <= 0 and a[1] == 0 else True, axis=1))]
        return cohort

    def tinh_trang_thai(cohort):
        # Cho dù có HĐ trước khi gia nhập vẫn tính, nhưng sẽ ko tính Active
        cohort['AA1'] = cohort[['SỐ HĐ', 'STT Tháng làm việc']].apply(lambda a: 1 if a[0] > 0 else 0, axis=1)

        # Đoạn Shift này gây lỗi vì khác TVV nhưng vẫn shift
        for i in range(1, 6):
            cohort[f'SỐ HĐ -{i} month'] = cohort['SỐ HĐ'].shift(i)
            cohort[f'SỐ HĐ -{i} month'] = cohort[f'SỐ HĐ -{i} month'].fillna(0)

        # Kinh nghiệm làm phần này, 1 phần đã từng mất tới chục phút vẫn chưa chạy xong
        # 1. Apply nhanh hơn for loop vì apply viết bằng C
        # 2. Loc từng phần nhanh hơn if toàn phần
        # 3. Sum nhanh hơn +
        # 4. Bỏ cột trung gian làm tăng tốc độ tính toán

        cohort.loc[cohort['STT Tháng làm việc'] == 1, 'AA3'] = cohort.loc[cohort['STT Tháng làm việc'] == 1, 'AA1']
        cohort.loc[cohort['STT Tháng làm việc'] == 2, 'AA3'] = cohort.loc[cohort['STT Tháng làm việc'] == 2, ['SỐ HĐ', 'SỐ HĐ -1 month']].apply(lambda a: 1 if sum(a) > 0 else 0, axis=1)
        cohort.loc[cohort['STT Tháng làm việc'] > 2, 'AA3'] = cohort.loc[cohort['STT Tháng làm việc'] > 2, ['SỐ HĐ', 'SỐ HĐ -1 month', 'SỐ HĐ -2 month']].apply(lambda a: 1 if sum(a) > 0 else 0, axis=1)
        cohort.loc[:, 'AA3'] = cohort.loc[:, 'AA3'].fillna(0)

        cohort.loc[cohort['STT Tháng làm việc'] <= 3, 'AA6'] = cohort.loc[cohort['STT Tháng làm việc'] <= 3, 'AA3']
        cohort.loc[cohort['STT Tháng làm việc'] == 4, 'AA6'] = cohort.loc[cohort['STT Tháng làm việc'] == 4, ['SỐ HĐ', 'SỐ HĐ -1 month', 'SỐ HĐ -2 month', 'SỐ HĐ -3 month']].apply(lambda a: 1 if sum(a) > 0 else 0, axis=1)
        cohort.loc[cohort['STT Tháng làm việc'] == 5, 'AA6'] = cohort.loc[cohort['STT Tháng làm việc'] == 5, ['SỐ HĐ', 'SỐ HĐ -1 month', 'SỐ HĐ -2 month', 'SỐ HĐ -3 month', 'SỐ HĐ -4 month']].apply(lambda a: 1 if sum(a) > 0 else 0, axis=1)
        cohort.loc[cohort['STT Tháng làm việc'] > 5, 'AA6'] = cohort.loc[cohort['STT Tháng làm việc'] > 5, ['SỐ HĐ', 'SỐ HĐ -1 month', 'SỐ HĐ -2 month', 'SỐ HĐ -3 month', 'SỐ HĐ -4 month', 'SỐ HĐ -5 month']].apply(lambda a: 1 if sum(a) > 0 else 0, axis=1)
        cohort.loc[:, 'AA6'] = cohort.loc[:, 'AA6'].fillna(0)

        cohort = cohort.drop(columns=['SỐ HĐ -1 month', 'SỐ HĐ -2 month', 'SỐ HĐ -3 month', 'SỐ HĐ -4 month', 'SỐ HĐ -5 month'])
        return cohort

    cohort = khung_cohort(additional_col=additional_col, month_min=month_min)
    cohort = so_hd_BHNT(cohort)
    lg.info('Done BHNT')
    cohort = so_hd_BHPNT(cohort)
    lg.info('Done BHPNT')
    cohort = so_hd_total(cohort)
    lg.info('Done BH')
    cohort = tinh_trang_thai(cohort)
    lg.info('Done trạng thái')
    if additional_col is None:
        add = ''
    else:
        add = '_add'
    if for_mapa:
        add += '_for_mapa'
        cohort = cohort.rename(columns={'SỐ HĐ': 'SỐ HĐ NỘP VÀ PNT'})
    cohort.to_csv(f'{for_reports_in}/cohort_AA_{add}.csv', index=False, encoding='utf-8-sig')
    lg.info(f'Done cohort_AA')
    return cohort

# mapa
def mapa(additional_col=None):
    if additional_col is None:
        add = ''
    else:
        add = '_add'

    cohort = cohort_AA(additional_col=additional_col, for_mapa=True)
    ns = pd.read_csv(f'{pool_in}/AdvisorInfo_no_code.csv', low_memory=False)
    
    mapa, _ = create_lst_month(2021, 2022)
    if additional_col is not None:
        mapa.loc[:, 'key'] = 1
        _ns = ns[additional_col].drop_duplicates()
        _ns.loc[:, 'key'] = 1
        mapa = mapa.merge(_ns, how='left', on='key')
        mapa = mapa.drop(columns=['key'])

        # Group by bị gì đó, kết quả ko đúng
        # Col Tuyển
        group_col = ['Month'] + additional_col
        cohort.loc[cohort['STT Tháng làm việc'] == 1, 'Tuyển'] = 1
        cohort['Tuyển'] = cohort['Tuyển'].fillna(0)
        df_group = pd.DataFrame(cohort.groupby(by=group_col)['Tuyển'].sum()).reset_index()
        mapa['Tuyển'] = mapa.merge(df_group, how='left', on=group_col)['Tuyển']
        # Col AA Total = AA6
        df_group = pd.DataFrame(cohort.groupby(by=group_col)['AA6'].sum()).reset_index()
        mapa['AA6'] = mapa.merge(df_group, how='left', on=group_col)['AA6']
        # Col AA1
        df_group = pd.DataFrame(cohort.groupby(by=group_col)['AA1'].sum()).reset_index()
        mapa['AA1'] = mapa.merge(df_group, how='left', on=group_col)['AA1']
        # Policy/AA1
        df_group = pd.DataFrame(cohort.groupby(by=group_col)['SỐ HĐ PH'].sum()).reset_index()
        mapa['Policy'] = mapa.merge(df_group, how='left', on=group_col)['SỐ HĐ PH']
        mapa['Policy/AA1'] = mapa[['Policy', 'AA1']].apply(lambda a: 0 if a[1] == 0 else round(a[0]/a[1],2), axis=1)

        # FYP/AA1
        df_group = pd.DataFrame(cohort.groupby(by=group_col)['FYP'].sum()).reset_index()
        mapa['FYP'] = mapa.merge(df_group, how='left', on=group_col)['FYP']
        mapa['FYP/AA1'] = mapa[['FYP', 'AA1']].apply(lambda a: 0 if a[1] == 0 else round(a[0]/a[1],0), axis=1)
        mapa['FYP/Policy'] = mapa[['FYP', 'Policy']].apply(lambda a: 0 if a[1] == 0 else round(a[0]/a[1],0), axis=1)

        # Fillna
        mapa.loc[:, ['Tuyển', 'AA6', 'AA1', 'Policy/AA1', 'FYP/Policy', 'FYP/AA1', 'Policy', 'FYP']] = mapa.loc[:, ['Tuyển', 'AA6', 'AA1', 'Policy/AA1', 'FYP/Policy', 'FYP/AA1', 'Policy', 'FYP']].fillna(0)
    else:
        # Col Tuyển
        cohort.loc[cohort['STT Tháng làm việc'] == 1, 'Tuyển'] = 1
        cohort['Tuyển'].fillna(0)
        df_group = pd.DataFrame(cohort.groupby('Month')['Tuyển'].sum()).reset_index()
        mapa['Tuyển'] = mapa.merge(df_group, how='left', on='Month')['Tuyển']
        # Col AA Total = AA6
        df_group = pd.DataFrame(cohort.groupby('Month')['AA6'].sum()).reset_index()
        mapa['AA6'] = mapa.merge(df_group, how='left', on='Month')['AA6']
        # Col AA1
        df_group = pd.DataFrame(cohort.groupby('Month')['AA1'].sum()).reset_index()
        mapa['AA1'] = mapa.merge(df_group, how='left', on='Month')['AA1']
        # Policy/AA1
        df_group = pd.DataFrame(cohort.groupby('Month')['SỐ HĐ PH'].sum()).reset_index()
        mapa['Policy'] = mapa.merge(df_group, how='left', on='Month')['SỐ HĐ PH']
        mapa['Policy/AA1'] = mapa[['Policy', 'AA1']].apply(lambda a: round(a[0]/a[1],2), axis=1)

        # FYP/AA1
        df_group = pd.DataFrame(cohort.groupby('Month')['FYP'].sum()).reset_index()
        mapa['FYP'] = mapa.merge(df_group, how='left', on='Month')['FYP']
        mapa['FYP/AA1'] = mapa[['FYP', 'AA1']].apply(lambda a: round(a[0]/a[1],0), axis=1)
        mapa['FYP/Policy'] = mapa[['FYP', 'Policy']].apply(lambda a: round(a[0]/a[1],0), axis=1)

    # Final
    if additional_col is not None:
        mapa = mapa[['Month'] + additional_col + ['Tuyển', 'AA6', 'AA1', 'Policy/AA1', 'FYP/Policy', 'FYP/AA1', 'Policy', 'FYP']]
    else:
        mapa = mapa[['Month', 'Tuyển', 'AA6', 'AA1', 'Policy/AA1', 'FYP/Policy', 'FYP/AA1', 'Policy', 'FYP']]

    mapa.to_csv(f'{for_reports_in}/mapa_{add}.csv', index=False, encoding='utf-8-sig')
    lg.info(f'Done mapa')