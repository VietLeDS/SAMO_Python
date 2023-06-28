from SAMO_Libs.Base_libs_and_options import *
from SAMO_Libs.constants import *
from SAMO_Libs.General_func import *
from SAMO_Libs.Layer1 import *
from SAMO_Libs.RDBMS import *
from SAMO_Libs.Short_data import *
from SAMO_Libs.Cohort_mapa import *
from SAMO_Libs.bc_time import *
from SAMO_Libs.BC90D import *
from SAMO_Libs.sql import *
from SAMO_Libs.doanh_so_nhanh import *
from SAMO_Libs.cut_off import *
from SAMO_Libs.Crosscheck import *

# Code run from here
if __name__ == '__main__':
    lg.info('Start')

    # This is daily flow, other reports just run if necessary
    
    # Source
    list_convert_function = ['Adv', 'Policy', 'Cus', 'HLV', 'SLV', 'CLV', 'CLV_Submit', 'hdkd', 'ns', 'ns_hdkd']

    thread = {}
    for _ in list_convert_function:
        thread[_] = Process(target=get_source_data, kwargs={'lst_run': [_]})
        thread[_].start()

    for _ in list_convert_function:
        thread[_].join()

    # Crosscheck
    crosscheck_ns()
    crosscheck_cus()
    crosscheck_hdkd()

    lst_position = ['CEO','DSND','DND','DSTD','DTD','DSRD','DRD','DSSD','DSD','DFM','DFT','DFA','DFR'][::-1]
    lst_tree_mapa = lst_position

    # Mapa
    list_run = [mapa, hdkd_co_ban_to_csv]

    thread = {}
    for _ in list_run:
        thread[f'{_.__name__}'] = Process(target=_)

    for _ in thread.keys():
        thread[_].start()
 
    for _ in thread.keys():
        thread[_].join()