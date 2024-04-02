"""개인사업자재무정보"""
import pandas as pd
import itertools
from pathlib import PurePath
from pdp_utils import PdpData

sb_fin_dir = './SB_Fin_info'
# 개인사업자 재무정보조희
url1 = 'http://apis.data.go.kr/1160100/service/GetSBFinanceInfoService/getFnafInfo'
fna_dir = PurePath(sb_fin_dir, '/fnaf')
# 개인사업자 매출액정보조회
url2 = 'http://apis.data.go.kr/1160100/service/GetSBFinanceInfoService/getSlsInfo'
# 개인사업자 부채정보조회
url3 = 'http://apis.data.go.kr/1160100/service/GetSBFinanceInfoService/getDbtInfo'

pdp = PdpData(url1, sb_fin_dir, 5000)
pdp.fetch_to_csv('sb_fna_info')
