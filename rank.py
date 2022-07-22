import pandas as pd
import numpy as np
import pandas_datareader.data as web
import requests
import warnings
warnings.filterwarnings( 'ignore' )
from io import BytesIO
from datetime import datetime
from pykrx import stock
from dateutil.relativedelta import relativedelta
from bs4 import BeautifulSoup

# rank 모멘텀
# 0. 대상종목 추출
# 1. 기간 수익률 산출
# 2. 순위 모멘텀 계산
# 3. 평균 상위 종목 선정

def get_bdate_info(start_date, end_date) :
    end_bdate = stock.get_nearest_business_day_in_a_week(datetime.strftime(datetime.strptime(end_date, "%Y%m%d") + relativedelta(days=3),"%Y%m%d"))
    date = pd.DataFrame(stock.get_previous_business_days(fromdate=start_date, todate=end_bdate)).rename(columns={0: '일자'})
    prevbdate = date.shift(1).rename(columns={'일자': '전영업일자'})
    date = pd.concat([date, prevbdate], axis=1).fillna(
        datetime.strftime(datetime.strptime(stock.get_nearest_business_day_in_a_week(datetime.strftime(datetime.strptime(start_date, "%Y%m%d") - relativedelta(days=1), "%Y%m%d")), "%Y%m%d"),"%Y-%m-%d %H:%M:%S"))
    date['주말'] = ''
    for i in range(0, len(date) - 1):
        if abs(datetime.strptime(datetime.strftime(date.iloc[i + 1].일자, "%Y%m%d"), "%Y%m%d") - datetime.strptime(datetime.strftime(date.iloc[i].일자, "%Y%m%d"), "%Y%m%d")).days > 1:
            date['주말'].iloc[i] = 1
        else:
            date['주말'].iloc[i] = 0
    month_list = date.일자.map(lambda x: datetime.strftime(x, '%Y-%m')).unique()
    monthly = pd.DataFrame()
    for m in month_list:
        try:
            monthly = monthly.append(date[date.일자.map(lambda x: datetime.strftime(x, '%Y-%m')) == m].iloc[-1])
        except Exception as e:
            print("Error : ", str(e))
        pass
    date['월말'] = np.where(date['일자'].isin(monthly.일자.tolist()), 1, 0)
    date = date[date.일자 <= datetime.strftime(datetime.strptime(end_date, "%Y%m%d"),"%Y-%m-%d")]
    return date


def get_kospi_code(mkt, stddate):
    if mkt == '코스피' : ind = '001'
    elif mkt == '코스피50' : ind = '035'
    elif mkt == '코스피100' : ind = '034'
    elif mkt == '코스피200' : ind = '028'
    elif mkt == '코스피100200' : ind = '167'
    elif mkt == '코스피200제외' : ind = '244'
    elif mkt == '코스피대형주' : ind = '002'
    elif mkt == '코스피중형주' : ind = '003'
    elif mkt == '코스피소형주' : ind = '004'
    query_str_parms = {
    'locale': 'ko_KR',
    'tboxindIdx_finder_equidx0_2': '',
    'indIdx': '1',
    'indIdx2': ind,
    'codeNmindIdx_finder_equidx0_2': '',
    'param1indIdx_finder_equidx0_2': '',
    'trdDd': stddate,
    'money': 3,
    'csvxls_isNo': 'false',
    'name': 'fileDown',
    'url': 'dbms/MDC/STAT/standard/MDCSTAT00601'
    }
    headers = {
        'Referer': 'http://data.krx.co.kr/contents/MDC/MDI/mdiLoader',
        'Upgrade-Insecure-Requests': '1',
        'User-Agent': 'Mozilla/5.0'
    }
    r = requests.get('http://data.krx.co.kr/comm/fileDn/GenerateOTP/generate.cmd', query_str_parms, headers=headers)
    form_data = {
        'code': r.content
    }
    r = requests.post('http://data.krx.co.kr/comm/fileDn/download_excel/download.cmd', form_data, headers=headers)
    df = pd.read_excel(BytesIO(r.content))
    for i in range(0, len(df.종목코드)):
        df.종목코드.iloc[i] = 'A'+str(df.종목코드[i]).zfill(6)
    return df

def get_kosdaq_code(mkt, stddate):
    if mkt == '코스닥' : ind = '001'
    elif mkt == '코스닥150' : ind = '203'
    elif mkt == '코스닥대형주' : ind = '002'
    elif mkt == '코스닥중형주' : ind = '003'
    elif mkt == '코스닥소형주' : ind = '004'
    query_str_parms = {
    'locale': 'ko_KR',
    'tboxindIdx_finder_equidx0_2': '',
    'indIdx': '2',
    'indIdx2': ind,
    'codeNmindIdx_finder_equidx0_2': '',
    'param1indIdx_finder_equidx0_2': '',
    'trdDd': stddate,
    'money': 3,
    'csvxls_isNo': 'false',
    'name': 'fileDown',
    'url': 'dbms/MDC/STAT/standard/MDCSTAT00601'
    }
    headers = {
        'Referer': 'http://data.krx.co.kr/contents/MDC/MDI/mdiLoader',
        'Upgrade-Insecure-Requests': '1',
        'User-Agent': 'Mozilla/5.0'
    }
    r = requests.get('http://data.krx.co.kr/comm/fileDn/GenerateOTP/generate.cmd', query_str_parms, headers=headers)
    form_data = {
        'code': r.content
    }
    r = requests.post('http://data.krx.co.kr/comm/fileDn/download_excel/download.cmd', form_data, headers=headers)
    df = pd.read_excel(BytesIO(r.content))
    for i in range(0, len(df.종목코드)):
        df.종목코드.iloc[i] = 'A'+str(df.종목코드[i]).zfill(6)
    return df

# start_date = '20200301'
# end_date = '20220701'
# mkt = '코스피200제외'

def get_unique_code(mkt, start_date, end_date):
    bdate = get_bdate_info(start_date,end_date)
    bdate_w = bdate[bdate.주말==1].reset_index(drop=True)
    df_list = pd.DataFrame()
    for i in range(0,len(bdate_w)) :
        if mkt[:3] == "코스피" :
            df_list_temp = get_kospi_code(mkt, datetime.strftime(bdate_w.iloc[i].일자, "%Y%m%d"))
            df_list = pd.concat([df_list, df_list_temp])
        else :
            df_list_temp = get_kosdaq_code(mkt, datetime.strftime(bdate_w.iloc[i].일자, "%Y%m%d"))
            df_list = pd.concat([df_list, df_list_temp])
    df_unique = df_list[['종목코드','종목명']].drop_duplicates().reset_index(drop = 'False')
    return df_unique

# df_unique = get_unique_code(start_date, end_date)
# tickers = df_unique['종목코드'].str.split('A').str[1].unique().tolist()

def get_adj_price(start_date, tickers) :
    df_prc = pd.DataFrame()
    for s in range(0, len(tickers)):
        cnt = round((datetime.today() - datetime.strptime(start_date, "%Y%m%d")).days * 25/30, 0)
        response = requests.get('https://fchart.stock.naver.com/sise.nhn?symbol={}&timeframe=day&count={}&requestType=0'.format(tickers[s],cnt))
        bs = BeautifulSoup(response.content, "html.parser")
        df_item = bs.select('item')
        columns = ['Date', 'Open', 'High', 'Low', 'Close', 'Volume']
        df = pd.DataFrame([], columns = columns, index = range(len(df_item)))
        for t in range(len(df_item)):
            df.iloc[t] = str(df_item[t]['data']).split('|')
            df['Date'].iloc[t] = datetime.strptime(df['Date'].iloc[t], "%Y%m%d")
            df['Close'].iloc[t] = int(df['Close'].iloc[t])
        df_temp = pd.DataFrame(df[['Date','Close']].set_index('Date'))
        df_temp.columns = [tickers[s]]
        df_prc_temp = df_temp
        df_prc = pd.concat([df_prc, df_prc_temp], axis=1)
    df_prc = df_prc.fillna(0).sort_index()
    df_prc = df_prc[df_prc.index >= start_date]
    return df_prc

# prc = get_adj_price(start_date, tickers)

def get_rtn_rank(prc, start_date, end_date) :
    bdate = get_bdate_info(start_date, end_date)
    bdate_w = bdate[bdate.주말 == 1].reset_index(drop=True)
    prc_w = prc[prc.index.isin(bdate_w['일자'].dt.strftime("%Y-%m-%d").tolist())]
    rtn_w = prc_w / prc_w.shift(1) - 1
    rtn_w = rtn_w.fillna(0)
    rtn_w = rtn_w.replace([np.inf, -np.inf], 0)
    rtn_rank = (rtn_w.rank(axis=1, ascending = False))[1:len(rtn_w)]
    return rtn_rank

# rtn_rank = get_rtn_rank(prc, start_date, end_date)

def get_pf(rtn_rank,selected_num) :
    mean = pd.DataFrame()
    for i in range(0,len(rtn_rank)-24 + 1 ) :
        window = rtn_rank[0+i:24+i]
        mean_temp = pd.DataFrame(window.mean()).T
        stddate = window.index[len(window)-1]
        mean_temp.index = [stddate]
        mean = pd.concat([mean, mean_temp], axis=0)
    mean_rank = (mean.rank(axis=1, ascending = True) <= selected_num).applymap(lambda x : 1 if x else 0)
    df = pd.DataFrame()
    for t in range(0,len(mean_rank)):
        rank_temp = (mean_rank.iloc[t] == 1).apply(lambda x: 1 if x else 0)
        df_list = rank_temp[rank_temp==1].index.tolist()
        df_temp  = pd.DataFrame({'StdDate' : np.repeat(mean_rank.index[t],len(df_list)), 'Code' : df_list})
        df =  pd.concat([df, df_temp], axis=0)
    df['종목명'] = ''
    for i in range(0,len(df)) :
        df['종목명'].iloc[i] = stock.get_market_ticker_name(df['Code'].iloc[i])
    # df.Code = 'A' + df['Code']
    return df

# df = get_pf(rtn_rank,10)

mkt_list = ['코스피대형주','코스피중형주','코스피소형주','코스닥대형주','코스닥중형주','코스닥소형주']

def get_df_by_mkt(start_date, end_date, mkt_list) :
    df_all = pd.DataFrame()
    for s in range(0,len(mkt_list)) :
        df_unique = get_unique_code(mkt_list[s], start_date, end_date)
        tickers = df_unique['종목코드'].str.split('A').str[1].unique().tolist()
        prc = get_adj_price(start_date, tickers)
        rtn_rank = get_rtn_rank(prc, start_date, end_date)
        df_all_temp = get_pf(rtn_rank, 10)
        df_all_temp['시장구분'] = mkt_list[s]
        df_all = pd.concat([df_all, df_all_temp], axis=0)
    df_all.Code = 'A' + df_all['Code']
    df_all = df_all.drop_duplicates(subset=['StdDate', 'Code', '종목명'])

    return df_all

start_date = '20170101'
end_date = '20220722'
df_all = get_df_by_mkt(start_date, end_date, mkt_list)


df_all.to_excel('C:/Users/ysj/Desktop/pffㄹ.xlsx')