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
import yfinance as yf
import FinanceDataReader as fdr


def get_adj_price(start_date, tickers) :
    stddate = datetime.strftime(datetime.strptime(start_date, '%Y%m%d'),'%Y-%m-%d')
    enddate = datetime.strftime(datetime.today(),'%Y-%m-%d')
    df_prc = pd.DataFrame()
    for s in range(0,len(tickers)):
        df_prc_temp = yf.Ticker(tickers[s]).history(start=stddate, end=enddate, back_adjust=False, auto_adjust=False)['Adj Close'].reset_index(drop= False).set_index('Date')
        df_prc_temp.columns = [tickers[s]]
        df_prc = pd.concat([df_prc,df_prc_temp], axis=1)
    df_prc = df_prc.fillna(0).sort_index()
    return df_prc

def get_bdate_info(start_date, end_date) :
    stddate = datetime.strftime(datetime.strptime(start_date, '%Y%m%d'), '%Y-%m-%d')
    end_date = datetime.strftime(datetime.strptime(end_date, '%Y%m%d'), '%Y-%m-%d')
    df = yf.Ticker('KO').history(start=stddate, end=end_date, back_adjust=False, auto_adjust=False)['Adj Close'].reset_index(drop=False).set_index('Date')
    date = pd.DataFrame({'일자' : df.index.tolist()})
    prevbdate = date.shift(1).rename(columns={'일자': '전영업일자'})
    date = pd.concat([date, prevbdate], axis=1).dropna()
    date['주말'] = ''
    for i in range(0, len(date) - 1):
        if abs(datetime.strptime(datetime.strftime(date.iloc[i + 1].일자, "%Y%m%d"), "%Y%m%d") - datetime.strptime(
                datetime.strftime(date.iloc[i].일자, "%Y%m%d"), "%Y%m%d")).days > 1:
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
    date.주말.iloc[len(date) - 1] = 1
    date = date[date.일자 <= end_date]
    return date

def get_rtn_rank(prc, start_date, end_date) :
    bdate = get_bdate_info(start_date, end_date)
    bdate_w = bdate[bdate.주말 == 1].reset_index(drop=True)
    prc_w = prc[prc.index.isin(bdate_w['일자'].dt.strftime("%Y-%m-%d").tolist())]
    rtn_w = prc_w / prc_w.shift(1) - 1
    rtn_w = rtn_w.fillna(0)
    rtn_w = rtn_w.replace([np.inf, -np.inf], 0)
    rtn_rank = (rtn_w.rank(axis=1, ascending = False))[1:len(rtn_w)]
    return rtn_rank

mkt = ['SP500','NASDAQ']
def get_pf(mkt, selected_num, start_date, end_date) :
    df_all = pd.DataFrame()
    for s in range(0,len(mkt)) :
        tgt = fdr.StockListing(mkt[s])[0:200][['Symbol','Name','Industry']]
        tickers = tgt['Symbol'].tolist()
        prc = get_adj_price(start_date, tickers)
        rtn_rank = get_rtn_rank(prc, start_date, end_date)
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
        df = pd.merge(df, tgt.rename(columns={'Symbol': 'Code'}), on='Code',how='inner').sort_values('StdDate')
        df['mkt'] = mkt[s]
        df_all = pd.concat([df,df_all]).reset_index(drop=True).sort_values('StdDate').reset_index(drop=True)
    df_all = df_all.drop_duplicates(subset=['StdDate', 'Code'])
    return df_all

start_date = '20180104'
end_date = '20220722'

pf = get_pf(mkt, 3, start_date, end_date)

pf.to_excel('C:/Users/ysj/Desktop/pfs.xlsx')