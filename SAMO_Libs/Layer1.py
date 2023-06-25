from .Base_libs_and_options import *
from .constants import *
from .General_func import *

# Layer 1: Input many, Output source_in, source_ex, pool_in

def get_source_data(lst_run=[], docs=csource):
    try:
        get_source_data_wrappered(lst_run=lst_run, docs=csource)
    except Exception as e:
        lg.info(f'Error at Layer1, get_source_data because of {e}')
        import sys
        sys.exit()

def get_source_data_wrappered(lst_run=[], docs=csource):
    user = os.getlogin()
    source = copy.deepcopy(docs)
    for _ in source.index:
        if source.loc[_, 'status'] == 'active':
            name = source.loc[_, 'name']
            # if name in lst_run and identify_change(lst_run=[name]):  # Bỏ đi vì ngoài data change còn code change cần chạy lại nữa
            if name in lst_run:
                # lg.info(f"Start get {name}")
                # Read
                if pd.isna(source.loc[_, 'sheet']):
                    try:
                        df = pd.read_excel(globals()[source.loc[_, 'path']], **eval(source.loc[_, 'kwarg']))  # path là tên đại diện trong Constants
                    except:
                        df = pd.read_excel(source.loc[_, 'path'], **eval(source.loc[_, 'kwarg']))  # path là đường dẫn trực tiếp
                else:
                    try:
                        df = pd.read_excel(globals()[source.loc[_, 'path']], sheet_name=source.loc[_, 'sheet'], **eval(source.loc[_, 'kwarg']))
                    except:
                        df = pd.read_excel(source.loc[_, 'path'], sheet_name=source.loc[_, 'sheet'], **eval(source.loc[_, 'kwarg']))
                # Remove '/n'
                df = df.replace('\n', '')
                # print(df.columns)
                df = df.rename(columns={k:k.replace('\n', '') for k in df.columns})
                # More transform
                if pd.notna(source.loc[_, 'extra_code']):
                    # Cách đã dùng thành công
                    # Phần này bị lỗi exec không return value do exec tạo ra 1 image độc lập so với chương trình chính
                    # Và bị vướng vấn đề pass by assignment của python
                    loc = locals()
                    exec(source.loc[_, 'extra_code'], globals(), loc)  # Pass by reference của locals()
                    # exec(source.loc[_, 'extra_code'], globals(), locals())  # Pass by value của locals() nên không thay đổi được df
                    df = loc['df']
                # To csv
                for i in source.loc[_, 'export_path'].split(','):
                    df.to_csv(f"{globals()[i]}\\{source.loc[_, 'export_name']}.csv", index=False, encoding='utf-8-sig')
                # lg.info(f"Done get {name}")