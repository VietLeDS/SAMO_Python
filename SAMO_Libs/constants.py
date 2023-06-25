from .Base_libs_and_options import *

user = os.getlogin()
# Default
cutoff_code_path = os.getcwd()
cutoff_source_data_path = ''
cutoff_data_path = ''

constants_path = f'{cutoff_code_path}\\SAMO_Libs\\Constants.xlsx'
cpath = pd.read_excel(constants_path, sheet_name='Path', dtype=str)
csource = pd.read_excel(constants_path, sheet_name='Source', dtype=str)
this_user_special_path = cpath.loc[cpath['user'] == user]
if len(this_user_special_path) > 0:
	for _ in this_user_special_path.index:
		if this_user_special_path.loc[_, 'name'] in ['source_in', 'source_ex']:
			globals()[this_user_special_path.loc[_, 'name']] = this_user_special_path.loc[_, 'path'].replace('User_name', user).replace('\\cutoff', cutoff_source_data_path)
		else:
			globals()[this_user_special_path.loc[_, 'name']] = this_user_special_path.loc[_, 'path'].replace('User_name', user).replace('\\cutoff', cutoff_data_path)
		for i in cpath.loc[cpath['name'] == this_user_special_path.loc[_, 'name']].index:
			cpath = cpath.drop(index=i)

for _ in cpath.index:
	if cpath.loc[_, 'user'] == 'all':
		if cpath.loc[_, 'name'] in ['source_in', 'source_ex']:
			globals()[cpath.loc[_, 'name']] = cpath.loc[_, 'path'].replace('User_name', user).replace('\\cutoff', cutoff_source_data_path)
		else:
			globals()[cpath.loc[_, 'name']] = cpath.loc[_, 'path'].replace('User_name', user).replace('\\cutoff', cutoff_data_path)