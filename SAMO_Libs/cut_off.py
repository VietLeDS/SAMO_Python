from .Base_libs_and_options import *
from .constants import *
from .General_func import *
# Layer linh hoạt

def recalculate_huy_hoan(a):
    """
    Input = ['Policy Id', 'HỦY/ HOÃN', 'NGÀY PH', 'NGÀY ACK', 'NGÀY HỦY/ HOÃN', 'NGÀY HỦY/ HOÃN calculated']
    Làm sao cut off được trạng thái Hủy Hoãn khi cột Ngày Hủy hoãn không đầy đủ - dùng ngày PH và ngày ACK để xác định với Hủy trước PH và Hủy sau PH
    - Vấn đề là cần phân biệt với Hủy có Ngày hủy nhưng chưa hủy vào thời điểm cut off
    """
    if pd.isna(a[1]):  # Chưa từng bị hủy kể cả trong tương lai
        return pd.NaT
    else:
        if pd.isna(a[5]):  # Có trạng thái hủy trong tương lai nhưng bị cut off
            return pd.NaT
        else:
            if a[1] in ['Ngưng đóng phí', 'Ngừng đóng phí']:
                return 'Ngừng đóng phí'
            elif a[1] == 'KẾT THÚC HĐ':
                return 'KẾT THÚC HĐ'
            if pd.isna(a[2]):
                return 'Hủy trước PH'
            elif pd.isna(a[3]):
                return 'Hủy sau PH'
            elif (a[5] - a[3]).days <= 21:
                return 'Hủy trong 21 ngày'
            else:
                return 'Mất hiệu lực'

def cut_off(cut_off_month='2022-10-01', lastest=False):  # cut_off tháng 9
    # Input
    hdkd = pd.read_csv(f'{pool_in}/policy.csv', low_memory=False, converters={'Policy Id': str})

    b3_hdkd = pd.read_csv(f'{pool_in}/b3_hdkd.csv', low_memory=False, converters={'CHECK SỐ HĐ': str})
    lst_sp = pd.read_excel(f'{source_in}/Danh sách SPC và SPBS của Đối tác.xlsx', sheet_name='Final')
    hlv = pd.read_csv(f'{pool_in}/Data_HLV.csv', low_memory=False, converters={'POLICY CODE': str})
    slv = pd.read_csv(f'{pool_in}/Data_SLV.csv', low_memory=False, converters={'APPLICATION NUMBER': str, 'POLICY NUMBER': str})
    clv = pd.read_csv(f'{pool_in}/Data_CLV.csv', low_memory=False, converters={'Policy Code': str})

    # transform
    hdkd['NGÀY NỘP'] = pd.to_datetime(hdkd['NGÀY NỘP'])
    hdkd['NGÀY PH'] = pd.to_datetime(hdkd['NGÀY PH'])
    hdkd['NGÀY HỦY/ HOÃN'] = pd.to_datetime(hdkd['NGÀY HỦY/ HOÃN'])
    hdkd['NGÀY ACK'] = pd.to_datetime(hdkd['NGÀY ACK'])

    # transform parameters
    now = datetime.datetime.now()
    if lastest:
      cut_off_month = now
    else:
      cut_off_month = datetime.datetime.strptime(cut_off_month, '%Y-%m-%d')

    # Process
    # Goal1: Thêm FYP chính, bổ sung, topup
    # Goal2: Thêm Cấu trúc tại lúc bán (từ B3)
    # Final Goal: Nhằm mục đích tính lương

    # Process 1: Goal2: Thêm cấu trúc
    cau_truc_ls_b3 = ['MÃ GT ĐDKD', 'MÃ GT DFR',
               'MÃ GT SA', 'MÃ GT DFA', 'MÃ GT DFT', 'MÃ GT DFM', 'MÃ GT DSD',
               'MÃ GT DSSD', 'MÃ GT DRD', 'MÃ GT DSRD', 'MÃ GT DTD', 'MÃ GT DSTD',
               'MÃ GT DND', 'MÃ GT DSND', 'MÃ GT DCEO', 'MÃ GT CEO']
    lst_position = ['CEO','DCEO','DSND','DND','DSTD','DTD','DSRD','DRD','DSSD','DSD','DFM','DFT','DFA','DFR']

    hdkd = hdkd.merge(b3_hdkd.loc[b3_hdkd['MÃ HỒ SƠ (MOMI)'].notna(), ['MÃ HỒ SƠ (MOMI)'] + cau_truc_ls_b3], how='left', left_on=['Id Nội bộ', 'MÃ GT'], right_on=['MÃ HỒ SƠ (MOMI)', 'MÃ GT ĐDKD'])
    hdkd = hdkd.drop(columns=['MÃ HỒ SƠ (MOMI)', 'MÃ GT ĐDKD'])
    hdkd = hdkd.rename(columns={_:_.replace('MÃ GT ', '') for _ in cau_truc_ls_b3[1:]})

    # Process 2: Goal1: Thêm FYP chính và bổ sung, topup ko cần thêm - Thay đổi cách tính FYP theo pp mới nhất
    hlv2 = hlv[['POLICY CODE', 'PRODUCT NAME', 'PRODUCT PRIMIUM', 'POLICY STATUS', 'PRODUCT STATUS']].merge(lst_sp[['Product Name', 'Loại SP']], how='left', left_on='PRODUCT NAME', right_on='Product Name')
    slv2 = slv[['POLICY NUMBER', 'PRODUCT NAME VIET', 'PRODUCT PREMIUM', 'POLICY STATUS', 'PRODUCT STATUS']].merge(lst_sp[['Product Name', 'Loại SP']], how='left', left_on='PRODUCT NAME VIET', right_on='Product Name')
    clv2 = clv[['Policy Code', 'Product Code Detail', 'FYTP RYTP Total']].merge(lst_sp[['Product Name', 'Loại SP']], how='left', left_on='Product Code Detail', right_on='Product Name')

    hlv2 = hlv2[( hlv2[['POLICY STATUS', 'PRODUCT STATUS']].apply(lambda a:   True if a[0] in ['Waiting for Validate', 'Terminated'] else  # 2 trạng thái này không có 'POLICY PREMIUM' nên lấy theo 'PRODUCT PRIMIUM'
                                                                            True if a[0] == 'Inforce' and a[1] in ['Inforce', 'Lapsed'] else
                                                                            True if a[0] == 'Lapsed' and a[1] == 'Lapsed' else False, axis=1) )]
    slv2 = slv2[( slv2[['POLICY STATUS', 'PRODUCT STATUS']].apply(lambda a:   True if a[0] not in ['PREMIUM PAYING', 'LAPSED'] else
                                                                            True if a[0] == 'PREMIUM PAYING' and a[1] in ['PREMIUM PAYING', 'LAPSED'] else
                                                                            True if a[0] == 'LAPSED' and a[1] == 'LAPSED' else False, axis=1) )]
    # Chubb Nộp không có data detail về SP mà Chubb PH cũng không có data về Product Status -> Mặc định là đúng hết

    hlv2 = hlv2[['POLICY CODE', 'Loại SP', 'PRODUCT PRIMIUM']]
    hlv2 = hlv2.groupby(['POLICY CODE', 'Loại SP'])['PRODUCT PRIMIUM'].sum().reset_index()
    slv2 = slv2[['POLICY NUMBER', 'Loại SP', 'PRODUCT PREMIUM']]
    slv2 = slv2.groupby(['POLICY NUMBER', 'Loại SP'])['PRODUCT PREMIUM'].sum().reset_index()
    clv2 = clv2[['Policy Code', 'Loại SP', 'FYTP RYTP Total']]
    clv2 = clv2.groupby(['Policy Code', 'Loại SP'])['FYTP RYTP Total'].sum().reset_index()

    hdkd = hdkd.merge(hlv2.loc[hlv2['Loại SP'] == "SPC", ['POLICY CODE', 'PRODUCT PRIMIUM']], how='left', left_on='Policy Id', right_on='POLICY CODE')
    hdkd = hdkd.merge(hlv2.loc[hlv2['Loại SP'] == "SPBS", ['POLICY CODE', 'PRODUCT PRIMIUM']], how='left', left_on='Policy Id', right_on='POLICY CODE')
    hdkd = hdkd.merge(slv2.loc[slv2['Loại SP'] == "SPC", ['POLICY NUMBER', 'PRODUCT PREMIUM']], how='left', left_on='Policy Id', right_on='POLICY NUMBER')
    hdkd = hdkd.merge(slv2.loc[slv2['Loại SP'] == "SPBS", ['POLICY NUMBER', 'PRODUCT PREMIUM']], how='left', left_on='Policy Id', right_on='POLICY NUMBER')
    hdkd = hdkd.merge(clv2.loc[clv2['Loại SP'] == "SPC", ['Policy Code', 'FYTP RYTP Total']], how='left', left_on='Policy Id', right_on='Policy Code')
    hdkd = hdkd.merge(clv2.loc[clv2['Loại SP'] == "SPBS", ['Policy Code', 'FYTP RYTP Total']], how='left', left_on='Policy Id', right_on='Policy Code')
    
    hdkd['FYP SPC'] = hdkd[['PRODUCT PRIMIUM_x', 'PRODUCT PREMIUM_x', 'FYTP RYTP Total_x']].apply(lambda a: a[0] if pd.notna(a[0]) else a[1] if pd.notna(a[1]) else a[2], axis=1)
    hdkd['FYP SPBS'] = hdkd[['PRODUCT PRIMIUM_y', 'PRODUCT PREMIUM_y', 'FYTP RYTP Total_y']].apply(lambda a: a[0] if pd.notna(a[0]) else a[1] if pd.notna(a[1]) else a[2], axis=1)
    hdkd = hdkd.drop(columns=[_ for _ in hdkd.columns if '_x' in _ or '_y' in _])

    # Process 3: Cutoff ngày và trạng thái Hủy/ Hoãn
    hdkd = hdkd[(hdkd['NGÀY NỘP'] < cut_off_month)]  # Ko lấy hợp đồng không nộp
    hdkd['NGÀY HỦY/ HOÃN calculated'] = hdkd[['HỦY/ HOÃN', 'NGÀY HỦY/ HOÃN', 'NGÀY NỘP', 'NGÀY PH', 'NGÀY ACK']].apply(lambda a: a[1] if pd.notna(a[1]) else pd.NaT if pd.isna(a[0]) else max(a[2], a[3], a[4]), axis=1)

    hdkd['NGÀY PH'] = hdkd['NGÀY PH'].apply(lambda a: a if a < cut_off_month else pd.NaT)
    hdkd['NGÀY HỦY/ HOÃN'] = hdkd['NGÀY HỦY/ HOÃN'].apply(lambda a: a if a < cut_off_month else pd.NaT)
    hdkd['NGÀY HỦY/ HOÃN calculated'] = hdkd['NGÀY HỦY/ HOÃN'].apply(lambda a: a if a < cut_off_month else pd.NaT)
    hdkd['NGÀY ACK'] = hdkd['NGÀY ACK'].apply(lambda a: a if a < cut_off_month else pd.NaT)
    hdkd['THÁNG NỘP'] = hdkd['NGÀY NỘP'].apply(lambda a: a.replace(day=1))
    hdkd['THÁNG PH'] = hdkd['NGÀY PH'].apply(lambda a: a.replace(day=1))

    lst_huy_special = ['0004793661', '0005030635', '0005438283']  # 3 HĐ này từ năm 2020, 2021 có trạng thái hủy sau PH nhưng ko có ngày hủy
    # Bổ sung Hủy theo Policy
    hdkd['HỦY/ HOÃN'] = hdkd[['Policy Id', 'HỦY/ HOÃN', 'NGÀY PH', 'NGÀY ACK', 'NGÀY HỦY/ HOÃN', 'NGÀY HỦY/ HOÃN calculated']].apply(lambda a: recalculate_huy_hoan(a), axis=1)
    hdkd['Hủy theo Đối tác'] = hdkd[['Hủy theo Đối tác', 'HỦY/ HOÃN']].apply(lambda a: a[0] if pd.notna(a[1]) else pd.NaT, axis=1)
    hdkd = hdkd.drop(columns=['NGÀY HỦY/ HOÃN calculated'])

    hdkd.to_csv(f"{pool_in}/hdkd_cut_off_tháng_{datetime.datetime.strftime(cut_off_month, '%m_%Y')}.csv", index=False, encoding='utf-8-sig')
    print(f"Done cut off tháng {datetime.datetime.strftime(cut_off_month, '%m/%Y')}")
