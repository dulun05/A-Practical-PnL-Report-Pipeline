# Difference to JQ_main：
# 1.an added function: addjiaoyijilu, to manually add transaction history
# 2. different file path (desktop), and different names in finalmain

# features：
# 1.running locally
# 2.New Function: You can have beginDay to be any previous day you need
# 3.faster calculation of assembled data

# ============================import===============================
from AlphaTCom import Alpha_time as ATime
from AlphaTCom import fileProcess as ATFile
from AlphaTExcelLib import AlphaTExcel as ATExcel
from AlphaTExcelLib1 import AlphaTExcel as ATExcel1

import os, shutil, re
import time
import datetime
import numpy as np
import pandas as pd
from numpy import NaN
import jqdatasdk as jq
jq.auth('13581880168', 'www.dulun.205')

AFileTool = ATFile()
ATimeTool = ATime()

# =============================def functions==================================
def find_files(list_files, string):
# 输入通过os.listdir()找到的文件list，过滤掉不符合条件的文件，返回新的文件list.
    import re
    newlist_files = list()
    pattern = re.compile(string)
    for file in list_files:
        file = file.strip()
        ismatch = pattern.match(file)
        if ismatch:
            newlist_files.append(file)
    return newlist_files

def listFilename(path):
# path: jiaoyipath
# 输出一个以资金账号为key，文件名为value的dict
    fileList = find_files(os.listdir(path), r'.*\.csv')  # ^(\d{11})-
    fileNameDict = {}
    for each in fileList:
        a = int(each.split('-')[-1][:-4])
        fileNameDict[a] = each
    fileNameList = fileNameDict.keys()
    return fileNameDict

def getLastWeekDay(today):
# 功能：获得上一个工作日的日期, 且这个日期一定有收益记录
# 输出：类似'2014-08-09'的str
    yuanpath = rootpath + "/" + "收益计算结果"
    filelist = find_files(os.listdir(yuanpath), r'^(\d{4})-(\d{2})-(\d{2})$')
    now = datetime.datetime.strptime(today, '%Y-%m-%d')
    if now.weekday() == 0:
        dayStep = 3
    else:
        dayStep = 1
    lastWeekDay = now - datetime.timedelta(days=dayStep)
    while lastWeekDay.strftime("%Y-%m-%d") not in filelist:
        if lastWeekDay.weekday() == 0:
            dayStep = 3
        else:
            dayStep = 1
        lastWeekDay = lastWeekDay - datetime.timedelta(days=dayStep)
    return lastWeekDay.strftime("%Y-%m-%d")

def changedate(day):
# 将'yyyy-mm-dd'日期转为整数格式，方便对比日期大小。
    return int(day.replace('-', ''))

def addjiaoyijilu(aCusTotleDF):
    # 把历史手动调仓的记录增加到某个用户相应的交易记录表格里面。
    print(ATimeTool.getnow(), ':开始添加手动调仓记录')
    Dict1 = {}
    Dict1["日期"] = "2019-04-24"
    Dict1["时间"] = "10:30:13"
    Dict1["委托号"] = "0001"
    Dict1["代码"] = "002093"
    Dict1["名称"] = "国脉科技"
    Dict1["方向"] = "买"
    Dict1["委托类型"] = "限价单"
    Dict1["委托数量"] = 3300
    Dict1["委托价格"] = 11.21
    Dict1["成交数量"] = 3300
    Dict1["成交金额"] = Dict1["委托价格"] * Dict1["成交数量"]
    Dict1["撤单数量"] = 0
    Dict1["委托状态"] = "成交"
    Dict1["手续费"] = Dict1["成交金额"] * 0.00035
    Dict1['资金账号'] = 105028828
    aRecordDf = pd.DataFrame(Dict1, index=[0])
    aCusTotleDF = pd.concat([aCusTotleDF, aRecordDf], axis=0)
    aCusTotleDF = aCusTotleDF.reset_index(drop=True)
    # print("-------------------------------------", 105028828)
    Dict1 = {}
    Dict1["日期"] = "2019-04-26"
    Dict1["时间"] = "9:31:13"
    Dict1["委托号"] = "0002"
    Dict1["代码"] = "600831"
    stockCode = normalize_code_cool(Dict1["代码"])
    Dict1["名称"] = jq.get_security_info(stockCode).display_name
    Dict1["方向"] = "买"
    Dict1["委托类型"] = "限价单"
    Dict1["委托数量"] = 4000
    Dict1["委托价格"] = 12.61
    Dict1["成交数量"] = 4000
    Dict1["成交金额"] = Dict1["委托价格"] * Dict1["成交数量"]
    Dict1["撤单数量"] = 0
    Dict1["委托状态"] = "成交"
    Dict1["手续费"] = Dict1["成交金额"] * 0.00035
    Dict1['资金账号'] = 105028828
    aRecordDf = pd.DataFrame(Dict1, index=[0])
    aCusTotleDF = pd.concat([aCusTotleDF, aRecordDf], axis=0)
    aCusTotleDF = aCusTotleDF.reset_index(drop=True)
    # print("-------------------------------------", 105028828)
    Dict1 = {}
    Dict1["日期"] = "2019-04-26"
    Dict1["时间"] = "10:11:13"
    Dict1["委托号"] = "0003"
    Dict1["代码"] = "600831"
    stockCode = normalize_code_cool(Dict1["代码"])
    Dict1["名称"] = jq.get_security_info(stockCode).display_name
    Dict1["方向"] = "卖"
    Dict1["委托类型"] = "限价单"
    Dict1["委托数量"] = 4000
    Dict1["委托价格"] = 13.2
    Dict1["成交数量"] = 4000
    Dict1["成交金额"] = Dict1["委托价格"] * Dict1["成交数量"]
    Dict1["撤单数量"] = 0
    Dict1["委托状态"] = "成交"
    Dict1["手续费"] = Dict1["成交金额"] * 0.00035 + Dict1["成交金额"] * 0.001
    Dict1['资金账号'] = 105028828
    aRecordDf = pd.DataFrame(Dict1, index=[0])
    aCusTotleDF = pd.concat([aCusTotleDF, aRecordDf], axis=0)
    aCusTotleDF = aCusTotleDF.reset_index(drop=True)
    # print("-------------------------------------", 105028828)
    Dict1 = {}
    Dict1["日期"] = "2019-04-26"
    Dict1["时间"] = "9:55:13"
    Dict1["委托号"] = "0004"
    Dict1["代码"] = "002733"
    stockCode = normalize_code_cool(Dict1["代码"])
    Dict1["名称"] = jq.get_security_info(stockCode).display_name
    Dict1["方向"] = "卖"
    Dict1["委托类型"] = "限价单"
    Dict1["委托数量"] = 4000
    Dict1["委托价格"] = 30
    Dict1["成交数量"] = 4000
    Dict1["成交金额"] = Dict1["委托价格"] * Dict1["成交数量"]
    Dict1["撤单数量"] = 0
    Dict1["委托状态"] = "成交"
    Dict1["手续费"] = Dict1["成交金额"] * 0.00035 + Dict1["成交金额"] * 0.001
    Dict1['资金账号'] = 105028828
    aRecordDf = pd.DataFrame(Dict1, index=[0])
    aCusTotleDF = pd.concat([aCusTotleDF, aRecordDf], axis=0)
    aCusTotleDF = aCusTotleDF.reset_index(drop=True)
    # print("-------------------------------------", 105028828)
    Dict1 = {}
    Dict1["日期"] = "2019-04-26"
    Dict1["时间"] = "10:16:13"
    Dict1["委托号"] = "0005"
    Dict1["代码"] = "002565"
    stockCode = normalize_code_cool(Dict1["代码"])
    Dict1["名称"] = jq.get_security_info(stockCode).display_name
    Dict1["方向"] = "买"
    Dict1["委托类型"] = "限价单"
    Dict1["委托数量"] = 4000
    Dict1["委托价格"] = 18.91
    Dict1["成交数量"] = 4000
    Dict1["成交金额"] = Dict1["委托价格"] * Dict1["成交数量"]
    Dict1["撤单数量"] = 0
    Dict1["委托状态"] = "成交"
    Dict1["手续费"] = Dict1["成交金额"] * 0.00035 + Dict1["成交金额"] * 0.001
    Dict1['资金账号'] = 105028828
    aRecordDf = pd.DataFrame(Dict1, index=[0])
    aCusTotleDF = pd.concat([aCusTotleDF, aRecordDf], axis=0)
    aCusTotleDF = aCusTotleDF.reset_index(drop=True)
    # print("-------------------------------------", 156062015)
    Dict1 = {}
    Dict1["日期"] = "2019-04-26"
    Dict1["时间"] = "10:30:13"
    Dict1["委托号"] = "0001"
    Dict1["代码"] = "300666"
    stockCode = normalize_code_cool(Dict1["代码"])
    Dict1["名称"] = jq.get_security_info(stockCode).display_name
    Dict1["方向"] = "卖"
    Dict1["委托类型"] = "限价单"
    Dict1["委托数量"] = 1900
    Dict1["委托价格"] = 41.7
    Dict1["成交数量"] = 1900
    Dict1["成交金额"] = Dict1["委托价格"] * Dict1["成交数量"]
    Dict1["撤单数量"] = 0
    Dict1["委托状态"] = "成交"
    Dict1["手续费"] = Dict1["成交金额"] * 0.00035 + Dict1["成交金额"] * 0.001
    Dict1['资金账号'] = 156062015

    aRecordDf = pd.DataFrame(Dict1, index=[0])
    aCusTotleDF = pd.concat([aCusTotleDF, aRecordDf], axis=0)
    aCusTotleDF = aCusTotleDF.reset_index(drop=True)
    # print("-------------------------------------", 105018129)
    Dict1 = {}
    Dict1["日期"] = "2019-04-26"
    Dict1["时间"] = "10:30:13"
    Dict1["委托号"] = "0001"
    Dict1["代码"] = "600487"
    stockCode = normalize_code_cool(Dict1["代码"])
    Dict1["名称"] = jq.get_security_info(stockCode).display_name
    Dict1["方向"] = "卖"
    Dict1["委托类型"] = "限价单"
    Dict1["委托数量"] = 1100
    Dict1["委托价格"] = 21.64
    Dict1["成交数量"] = 1100
    Dict1["成交金额"] = Dict1["委托价格"] * Dict1["成交数量"]
    Dict1["撤单数量"] = 0
    Dict1["委托状态"] = "成交"
    Dict1["手续费"] = Dict1["成交金额"] * 0.00035 + Dict1["成交金额"] * 0.001
    Dict1['资金账号'] = 105018129

    aRecordDf = pd.DataFrame(Dict1, index=[0])
    aCusTotleDF = pd.concat([aCusTotleDF, aRecordDf], axis=0)
    aCusTotleDF = aCusTotleDF.reset_index(drop=True)
    # print("-------------------------------------", 105018129)
    Dict1 = {}
    Dict1["日期"] = "2019-04-26"
    Dict1["时间"] = "10:30:13"
    Dict1["委托号"] = "0002"
    Dict1["代码"] = "002384"
    stockCode = normalize_code_cool(Dict1["代码"])
    Dict1["名称"] = jq.get_security_info(stockCode).display_name
    Dict1["方向"] = "卖"
    Dict1["委托类型"] = "限价单"
    Dict1["委托数量"] = 1300
    Dict1["委托价格"] = 17.97
    Dict1["成交数量"] = 1300
    Dict1["成交金额"] = Dict1["委托价格"] * Dict1["成交数量"]
    Dict1["撤单数量"] = 0
    Dict1["委托状态"] = "成交"
    Dict1["手续费"] = Dict1["成交金额"] * 0.00035 + Dict1["成交金额"] * 0.001
    Dict1['资金账号'] = 105018129

    aRecordDf = pd.DataFrame(Dict1, index=[0])
    aCusTotleDF = pd.concat([aCusTotleDF, aRecordDf], axis=0)
    aCusTotleDF = aCusTotleDF.reset_index(drop=True)
    # print("-------------------------------------", 108022585)
    Dict1 = {}
    Dict1["日期"] = "2019-04-26"
    Dict1["时间"] = "10:30:13"
    Dict1["委托号"] = "0001"
    Dict1["代码"] = "601860"
    stockCode = normalize_code_cool(Dict1["代码"])
    Dict1["名称"] = jq.get_security_info(stockCode).display_name
    Dict1["方向"] = "买"
    Dict1["委托类型"] = "限价单"
    Dict1["委托数量"] = 3800
    Dict1["委托价格"] = 8.18
    Dict1["成交数量"] = 3800
    Dict1["成交金额"] = Dict1["委托价格"] * Dict1["成交数量"]
    Dict1["撤单数量"] = 0
    Dict1["委托状态"] = "成交"
    Dict1["手续费"] = Dict1["成交金额"] * 0.00035
    Dict1['资金账号'] = 108022585

    aRecordDf = pd.DataFrame(Dict1, index=[0])
    aCusTotleDF = pd.concat([aCusTotleDF, aRecordDf], axis=0)
    aCusTotleDF = aCusTotleDF.reset_index(drop=True)
    # print("-------------------------------------", 119013885)
    Dict1 = {}
    Dict1["日期"] = "2019-04-26"
    Dict1["时间"] = "10:30:13"
    Dict1["委托号"] = "0001"
    Dict1["代码"] = "300398"
    stockCode = normalize_code_cool(Dict1["代码"])
    Dict1["名称"] = jq.get_security_info(stockCode).display_name
    Dict1["方向"] = "买"
    Dict1["委托类型"] = "限价单"
    Dict1["委托数量"] = 3800
    Dict1["委托价格"] = 17.04
    Dict1["成交数量"] = 3800
    Dict1["成交金额"] = Dict1["委托价格"] * Dict1["成交数量"]
    Dict1["撤单数量"] = 0
    Dict1["委托状态"] = "成交"
    Dict1["手续费"] = Dict1["成交金额"] * 0.00035
    Dict1['资金账号'] = 119013885

    aRecordDf = pd.DataFrame(Dict1, index=[0])
    aCusTotleDF = pd.concat([aCusTotleDF, aRecordDf], axis=0)
    aCusTotleDF = aCusTotleDF.reset_index(drop=True)
    # print("-------------------------------------", 119013885)
    Dict1 = {}
    Dict1["日期"] = "2019-04-26"
    Dict1["时间"] = "10:30:13"
    Dict1["委托号"] = "0002"
    Dict1["代码"] = "300296"
    stockCode = normalize_code_cool(Dict1["代码"])
    Dict1["名称"] = jq.get_security_info(stockCode).display_name
    Dict1["方向"] = "买"
    Dict1["委托类型"] = "限价单"
    Dict1["委托数量"] = 6000
    Dict1["委托价格"] = 8.74
    Dict1["成交数量"] = 6000
    Dict1["成交金额"] = Dict1["委托价格"] * Dict1["成交数量"]
    Dict1["撤单数量"] = 0
    Dict1["委托状态"] = "成交"
    Dict1["手续费"] = Dict1["成交金额"] * 0.00035
    Dict1['资金账号'] = 119013885

    aRecordDf = pd.DataFrame(Dict1, index=[0])
    aCusTotleDF = pd.concat([aCusTotleDF, aRecordDf], axis=0)
    aCusTotleDF = aCusTotleDF.reset_index(drop=True)
    # print("-------------------------------------", 119013885)
    Dict1 = {}
    Dict1["日期"] = "2019-04-26"
    Dict1["时间"] = "10:30:13"
    Dict1["委托号"] = "0003"
    Dict1["代码"] = "000413"
    stockCode = normalize_code_cool(Dict1["代码"])
    Dict1["名称"] = jq.get_security_info(stockCode).display_name
    Dict1["方向"] = "卖"
    Dict1["委托类型"] = "限价单"
    Dict1["委托数量"] = 3300
    Dict1["委托价格"] = 6.51
    Dict1["成交数量"] = 3300
    Dict1["成交金额"] = Dict1["委托价格"] * Dict1["成交数量"]
    Dict1["撤单数量"] = 0
    Dict1["委托状态"] = "成交"
    Dict1["手续费"] = Dict1["成交金额"] * 0.00035 + Dict1["成交金额"] * 0.001
    Dict1['资金账号'] = 119013885

    aRecordDf = pd.DataFrame(Dict1, index=[0])
    aCusTotleDF = pd.concat([aCusTotleDF, aRecordDf], axis=0)
    aCusTotleDF = aCusTotleDF.reset_index(drop=True)
    # print("-------------------------------------", 119010490)
    Dict1 = {}
    Dict1["日期"] = "2019-04-26"
    Dict1["时间"] = "10:30:13"
    Dict1["委托号"] = "0001"
    Dict1["代码"] = "000735"
    stockCode = normalize_code_cool(Dict1["代码"])
    Dict1["名称"] = jq.get_security_info(stockCode).display_name
    Dict1["方向"] = "买"
    Dict1["委托类型"] = "限价单"
    Dict1["委托数量"] = 2500
    Dict1["委托价格"] = 11.68
    Dict1["成交数量"] = 2500
    Dict1["成交金额"] = Dict1["委托价格"] * Dict1["成交数量"]
    Dict1["撤单数量"] = 0
    Dict1["委托状态"] = "成交"
    Dict1["手续费"] = Dict1["成交金额"] * 0.00035
    Dict1['资金账号'] = 119010490

    aRecordDf = pd.DataFrame(Dict1, index=[0])
    aCusTotleDF = pd.concat([aCusTotleDF, aRecordDf], axis=0)
    aCusTotleDF = aCusTotleDF.reset_index(drop=True)
    # print("-------------------------------------", 119000142)

    Dict1 = {}
    Dict1["日期"] = "2019-04-26"
    Dict1["时间"] = "10:30:13"
    Dict1["委托号"] = "0001"
    Dict1["代码"] = "600651"
    stockCode = normalize_code_cool(Dict1["代码"])
    Dict1["名称"] = jq.get_security_info(stockCode).display_name
    Dict1["方向"] = "卖"
    Dict1["委托类型"] = "限价单"
    Dict1["委托数量"] = 7500
    Dict1["委托价格"] = 4.66
    Dict1["成交数量"] = 7500
    Dict1["成交金额"] = Dict1["委托价格"] * Dict1["成交数量"]
    Dict1["撤单数量"] = 0
    Dict1["委托状态"] = "成交"
    Dict1["手续费"] = Dict1["成交金额"] * 0.00035 + Dict1["成交金额"] * 0.001
    Dict1['资金账号'] = 119000142

    aRecordDf = pd.DataFrame(Dict1, index=[0])
    aCusTotleDF = pd.concat([aCusTotleDF, aRecordDf], axis=0)
    aCusTotleDF = aCusTotleDF.reset_index(drop=True)
    # print("-------------------------------------", 300106528)

    Dict1 = {}
    Dict1["日期"] = "2019-04-26"
    Dict1["时间"] = "10:30:13"
    Dict1["委托号"] = "0001"
    Dict1["代码"] = "002909"
    stockCode = normalize_code_cool(Dict1["代码"])
    Dict1["名称"] = jq.get_security_info(stockCode).display_name
    Dict1["方向"] = "卖"
    Dict1["委托类型"] = "限价单"
    Dict1["委托数量"] = 2500
    Dict1["委托价格"] = 10.8
    Dict1["成交数量"] = 2500
    Dict1["成交金额"] = Dict1["委托价格"] * Dict1["成交数量"]
    Dict1["撤单数量"] = 0
    Dict1["委托状态"] = "成交"
    Dict1["手续费"] = Dict1["成交金额"] * 0.00035 + Dict1["成交金额"] * 0.001
    Dict1['资金账号'] = 300106528

    aRecordDf = pd.DataFrame(Dict1, index=[0])
    aCusTotleDF = pd.concat([aCusTotleDF, aRecordDf], axis=0)
    aCusTotleDF = aCusTotleDF.reset_index(drop=True)
    # print("-------------------------------------", 300120061)

    Dict1 = {}
    Dict1["日期"] = "2019-04-30"
    Dict1["时间"] = "10:30:13"
    Dict1["委托号"] = "0001"
    Dict1["代码"] = "002692"
    stockCode = normalize_code_cool(Dict1["代码"])
    Dict1["名称"] = jq.get_security_info(stockCode).display_name
    Dict1["方向"] = "买"
    Dict1["委托类型"] = "限价单"
    Dict1["委托数量"] = 11300
    Dict1["委托价格"] = 4.23
    Dict1["成交数量"] = 11300
    Dict1["成交金额"] = Dict1["委托价格"] * Dict1["成交数量"]
    Dict1["撤单数量"] = 0
    Dict1["委托状态"] = "成交"
    Dict1["手续费"] = Dict1["成交金额"] * 0.00035
    Dict1['资金账号'] = 300120061

    aRecordDf = pd.DataFrame(Dict1, index=[0])
    aCusTotleDF = pd.concat([aCusTotleDF, aRecordDf], axis=0)
    aCusTotleDF = aCusTotleDF.reset_index(drop=True)
    # print("-------------------------------------", 105011438)

    Dict1 = {}
    Dict1["日期"] = "2019-04-26"
    Dict1["时间"] = "10:30:13"
    Dict1["委托号"] = "0001"
    Dict1["代码"] = "002611"
    stockCode = normalize_code_cool(Dict1["代码"])
    Dict1["名称"] = jq.get_security_info(stockCode).display_name
    Dict1["方向"] = "卖"
    Dict1["委托类型"] = "限价单"
    Dict1["委托数量"] = 5000
    Dict1["委托价格"] = 6.04
    Dict1["成交数量"] = 5000
    Dict1["成交金额"] = Dict1["委托价格"] * Dict1["成交数量"]
    Dict1["撤单数量"] = 0
    Dict1["委托状态"] = "成交"
    Dict1["手续费"] = Dict1["成交金额"] * 0.00035 + Dict1["成交金额"] * 0.001
    Dict1['资金账号'] = 105011438

    aRecordDf = pd.DataFrame(Dict1, index=[0])
    aCusTotleDF = pd.concat([aCusTotleDF, aRecordDf], axis=0)
    aCusTotleDF = aCusTotleDF.reset_index(drop=True)
    # print("-------------------------------------", 300123937)
    Dict1 = {}
    Dict1["日期"] = "2019-04-26"
    Dict1["时间"] = "10:30:13"
    Dict1["委托号"] = "0001"
    Dict1["代码"] = "000070"
    stockCode = normalize_code_cool(Dict1["代码"])
    Dict1["名称"] = jq.get_security_info(stockCode).display_name
    Dict1["方向"] = "买"
    Dict1["委托类型"] = "限价单"
    Dict1["委托数量"] = 1500
    Dict1["委托价格"] = 16.11
    Dict1["成交数量"] = 1500
    Dict1["成交金额"] = Dict1["委托价格"] * Dict1["成交数量"]
    Dict1["撤单数量"] = 0
    Dict1["委托状态"] = "成交"
    Dict1["手续费"] = Dict1["成交金额"] * 0.00035
    Dict1['资金账号'] = 300123937

    aRecordDf = pd.DataFrame(Dict1, index=[0])
    aCusTotleDF = pd.concat([aCusTotleDF, aRecordDf], axis=0)
    aCusTotleDF = aCusTotleDF.reset_index(drop=True)
    # print("-------------------------------------", 300123937)
    return aCusTotleDF

def normalize_code_cool(code):
    if code[0] == '3' or code[0] == '0':
        code = '{}.XSHE'.format(code)
    elif code[0] == '6':
        code = '{}.XSHG'.format(code)
    return code

def getAllRecords(jiaoyipath):
    # 获取交易记录中所有用户的交易记录allTradingRecords，按照【日期 资金账号 名称 时间】排序
    # 注意：这个函数和官网有区别，加了一个addjiaoyijilu的函数
    allTradingRecords = pd.DataFrame()
    fileList = find_files(os.listdir(jiaoyipath), r'.*\.csv')
    # 收集所有交易记录
    print(ATimeTool.getnow(), ':开始获取历史交易记录数据')
    for each in fileList:
        path = jiaoyipath + '/' + each
        a = int(each.split('-')[-1][:-4])
        df1 = AFileTool.open_csv(path)
        df1['资金账号'] = a
        allTradingRecords = pd.concat([allTradingRecords, df1], axis=0)

    # 与官网不同： 增加新增记录
    allTradingRecords = addjiaoyijilu(allTradingRecords)
    # 时间转换为可以比较大小的格式
    print(ATimeTool.getnow(), '开始normalize_code_cool')
    allTradingRecords['时间'] = allTradingRecords['时间'].apply(lambda x: int(datetime.datetime.strftime(datetime.datetime.strptime(x, "%H:%M:%S"), '%H%M%S')))
    allTradingRecords = allTradingRecords[allTradingRecords['时间'] >= 93000]

    # 股票代码格式转换为聚宽的数据格式
    allTradingRecords['代码'] = allTradingRecords['代码'].map(lambda x: normalize_code_cool(x.lstrip("'")))

    print(ATimeTool.getnow(), ':获取历史交易记录数据完毕')
    # 排序
    allTradingRecords = allTradingRecords.sort_values(by=['日期', '资金账号', '名称', '时间'], ascending=(True, True, True, True))
    return allTradingRecords

# 功能：输入历史收益计算结果所存储的路径，读取最后一次跑的收益记录（也就是前一个工作日）
# rootpath：历史收益计算结果所存储的路径
# danOrComb='dan':获取单票收益；danOrComb='comb':获取组合收益；
# 输出：历史上所有客户所有股票每天的交易记录summary
# 找到上一个交易日的收益结果文件
def getolddate(rootpath, danOrComb):

    yuanpath = rootpath + "/" + "收益计算结果"
    # 之后改index可能要用到这个，现在先用着原来的index
    # if changedate(getLastWeekDay(today) ) <= changedate("2019-08-06"):
    #     index = ['新的index']
    lastWeekDay = getLastWeekDay(today)
    if danOrComb == 'dan':
        path = yuanpath + "/" + lastWeekDay + "/" + "汇总数据" + "/" + lastWeekDay + "_单票收益记录.xlsx"
    if danOrComb == 'comb':
        path = yuanpath + "/" + lastWeekDay + "/" + "汇总数据" + "/" + lastWeekDay + "_组合收益记录.xlsx"
    summary = pd.read_excel(path, encoding="gbk")[index]
    return summary

def mycopyfile(srcfile, dstfile):
    if not os.path.isfile(srcfile):
        0
    else:
        fpath, fname = os.path.split(dstfile)  # 分离文件名和路径
        if not os.path.exists(fpath):
            os.makedirs(fpath)  # 创建路径
        shutil.copyfile(srcfile, dstfile)  # 复制文件
        # print "copy %s -> %s"%( srcfile,dstfile)

def getYYBNameList(data_zu):
    # 从组合收益表里面提取所有的营业部名称，放在一个list里面输出
    outDF = data_zu.drop_duplicates(["营业部"], keep="first")
    return outDF["营业部"].tolist()

def getYYBcustomerList(data_zu, YYB):
    # 获取某个营业部中所有客户的账户名，放到一个list里面。
    tempDF = data_zu[data_zu['营业部'] == YYB]
    outDF = tempDF.drop_duplicates(["账户名"], keep="first")
    return outDF["账户名"].tolist()

def mkdir(path):
    # 该函数就是像linux的同名函数一样，用来创建路径的。
    # 引入模块
    # 去除首位空格
    path = path.strip()
    # 去除尾部 \ 符号
    path = path.rstrip("/")

    # 判断路径是否存在
    # 存在     True
    # 不存在   False
    isExists = os.path.exists(path)

    # 判断结果
    if not isExists:
        # 如果不存在则创建目录
        # 创建目录操作函数
        os.makedirs(path)

        print(path + ' 创建成功')
        return True
    else:
        # 如果目录存在则不创建，并提示目录已存在
        print(path + ' 目录已存在')
        return False

def main(data_zu, jiaoyipath, rootpath):
    # 这个是处理各个营业部中的所有用户的交割记录的
    print("开始处理交割记录")
    Path1 = jiaoyipath
    oldfilelist = find_files(os.listdir(Path1), r'.*\.csv')
    nowpath = rootpath + "/" + today + "/" + "营业部数据"
    today_zu = data_zu[data_zu["交易日期"] == today].reset_index(drop=True)
    YYBList = getYYBNameList(today_zu)
    for each1 in YYBList:
        # 获得该营业部需要处理的用户列表
        YYBCTMList = getYYBcustomerList(data_zu, each1)
        for each in oldfilelist:
            if each.split('-')[1] in YYBCTMList:
                srcPath = Path1 + "/" + each
                YYBPath = nowpath + "/" + each1 + "/" + "交割记录" + "/" + each

                # 将该营业部的文件放入目标目录
                mycopyfile(srcPath, YYBPath)
    print("交割记录处理完成")

def getdan(data_dan, data_zu, rootpath):
    # 这个函数是从总的单票收益表格中，抠出来每个营业部中相应客户的单票记录，然后存入各个时期营业部文件夹里。
    print("开始处理营业部单票划分")
    # 筛选出今天有交易记录的营业部
    today_zu = data_zu[data_zu["交易日期"] == today].reset_index(drop=True)
    YYBList = getYYBNameList(today_zu)

    # print("today is %s"%(today))
    nowpath = rootpath + "/" + today + "/" + "营业部数据"
    path1 = nowpath + "/" + '营业部单票汇总表'
    mkdir(path1)
    index = data_dan.columns
    # print(index)
    for each in YYBList:
        # 获得该营业部需要处理的用户列表
        out = pd.DataFrame(columns=index)
        out = data_dan[data_dan["营业部"] == each]
        strTemp = today + '_' + '单票汇总表' + ".xlsx"
        filename = path1 + "/" + strTemp
        # print(out)
        out.to_excel(filename, encoding='utf-8', index=False)
    print("营业部单票汇总表输出完成")

def finalmain(dan_path, zu_path, jiaoyipath, rootpath):
    # 2.0版本：减少了重复的数据读取
    # 这个函数是生成汇总数据文件夹里面的汇总数据表格的。
    # dan_path存储今日单票收益记录的路径
    # zu_path存储今日组合收益记录的路径
    official_path = rootpath + "/" + "一创聚宽（升级版）正式使用登记表.xlsx"
    yuanpath = rootpath + "/" + "收益计算结果"
    data_zu = pd.read_excel(zu_path, encoding='utf-8')
    data_dan = pd.read_excel(dan_path, encoding='utf-8')

    # 导入正式使用用户表格的信息
    official_users = pd.read_excel(official_path, encoding='utf-8')
    official_users['申请时间'] = official_users['申请时间'].apply(
        lambda x: changedate(datetime.datetime.strftime(x, '%Y-%m-%d')))

    # 生成每个营业部的单票汇总表
    getdan(data_dan, data_zu, yuanpath)
    # 处理各个营业部的交割记录 貌似可以删除
    main(data_zu, jiaoyipath, yuanpath)
    # 获取需要处理的日期列表
    datelist = data_zu.drop_duplicates(["交易日期"], keep="first")["交易日期"].values.tolist()
    # 获取当日交易额最高的客户名单
    datelist.sort()
    # 先筛选已有收益表格的日期
    datelist = datelist[60:]
    # 没有当天市值的日期
    print(len(datelist))
    print("日期：从{}到{}".format(datelist[0], datelist[-1]))
    # 按天获取组合汇总收益数据
    # 获取交易额排名前20的用户
    summary = pd.DataFrame()
    print(ATimeTool.getnow(), '开始处理汇总数据')
    for i in range(len(datelist)):
        print("开始汇总%s的数据" % (datelist[i]))
        riqi = datelist[i]

        # data_zu: 今日组合收益
        df1 = data_zu[data_zu["交易日期"] == riqi].reset_index(drop=True)
        dict1 = {}
        dict1["交易日期"] = riqi
        dict1["当天市值"] = df1["市值"].sum()
        dict1["当天交易额"] = int(df1["当天交易额"].sum())

        # 根据正式用户使用表计算正式用户交易额
        users_up_to_date = official_users[official_users['申请时间'] <= changedate(riqi)]
        customer_lists = users_up_to_date['资金账号'].unique()
        df2 = df1[df1['资金账号'].apply(lambda x: x in customer_lists)]
        dict1['正式账号当天交易额'] = int(df2['当天交易额'].sum())

        dict1["当天收益"] = int(df1["当天收益"].sum())
        # 根据组合数据计算汇总数据的交易额加权换手率
        if (not df1["当天交易额"].isnull().all()) and (not df1["换手率%"].isnull().all()):
            trading_values = list(df1["当天交易额"].values)
            turnover_rates = list(df1["换手率%"].values)
            nrow = len(df1)
            sum_product = 0
            for k in range(nrow):
                if (not pd.isnull(trading_values[k])) and (not (pd.isnull(turnover_rates[k]))):
                    product = trading_values[k] * turnover_rates[k]
                    sum_product = sum_product + product
            dict1["交易额加权换手率%"] = get2float(sum_product / dict1["当天交易额"])
        else:
            dict1["交易额加权换手率%"] = NaN
        # 根据组合数据计算汇总数据的交易额加权收益率
        if (not df1["当天交易额"].isnull().all()) and (not df1["收益率%"].isnull().all()):
            trading_values = list(df1["当天交易额"].values)
            return_rates = list(df1["收益率%"].values)
            nrow = len(df1)
            sum_product = 0
            for a in range(nrow):
                if (not pd.isnull(trading_values[a])) and (not (pd.isnull(return_rates[a]))):
                    product = trading_values[a] * return_rates[a]
                    sum_product = sum_product + product
            dict1["交易额加权收益率%"] = sum_product / dict1["当天交易额"]
        else:
            dict1["交易额加权收益率%"] = NaN

        # 计算中位数收益率
        if not df1['收益率%'].isnull().all():
            returns = df1['收益率%'].values
            returns = [i for i in returns if not pd.isnull(i)]
            median_return = np.median(returns)
            dict1["中位数收益率%"] = median_return
        else:
            dict1["中位数收益率%"] = NaN

        # 计算等权收益率
        if not df1['收益率%'].isnull().all():
            returns = df1['收益率%'].values
            returns = [i for i in returns if not pd.isnull(i)]
            mean_return = np.mean(returns)
            dict1["等权平均收益率%"] = mean_return
        else:
            dict1["等权平均收益率%"] = NaN

            # 提取上证指数每日交易额
        date = datetime.datetime.strptime(riqi, '%Y-%m-%d')
        year = date.year
        month = date.month
        day = date.day
        df_XSHG = jq.get_price('000001.XSHG', count=240, end_date=datetime.datetime(year, month, day, 15, 0, 0),
                               frequency='1m', fq=None)
        trading_money = int(df_XSHG['money'].sum() / 100000000)
        dict1['上证指数当日交易额'] = trading_money

        aRecordDf = pd.DataFrame(dict1, index=[0])
        summary = pd.concat([summary, aRecordDf], axis=0)

    path = yuanpath + "/" + today + "/" + "汇总数据" + "/" + today + "_汇总数据.xlsx"
    summary = summary[['交易日期', '上证指数当日交易额', '交易额加权收益率%', '中位数收益率%', '等权平均收益率%',
                       '当天收益', '当天市值', '当天交易额', '正式账号当天交易额', '交易额加权换手率%']]
    summary.to_excel(path, encoding='utf-8')


def getYYBName(account):
# 从资金账号推断出该用户是属于哪个营业部的。
    if account == 156062015:
        department = "深圳笋岗东路营业部"
    elif account == 35000443:
        department = '佛山绿景三路营业部'
    elif account == 135067698:
        department = '海口滨海大道营业部'
    else:
        number = int(str(account)[:3])  # 提取资金账号前三位
        df1 = YYBDF[YYBDF["资金账号前3位"] == number]  # 匹配相应的营业部
        if df1.empty:
            number = int(str(account)[:4])
            df1 = YYBDF[YYBDF["资金账号前3位"] == number]
            if df1.empty:
                print("number:", number)
                print("error!")
                department = "未知营业部"
            else:
                department = df1.iloc[0, 2]
        else:
            department = df1.iloc[0, 2]
    return department


def dealACumRecorder(tagDF):
# 从当日用户信息表里，抠出某一个用户所有交易的股票的相关信息，返回一个字典，key是股票代码，value还是一个字典，
# 存储着“目标股数”，“目标市值”，“交易市值”，“用户授权资金”，“股票名称”等等信息。
    stockCodeList = tagDF['标的代码'].values
    outStockCodeDict = {}
    for each in stockCodeList:
        tempDict = {}
        tempDF = tagDF[tagDF['标的代码'] == each]
        tempDict['股票名称'] = tempDF['标的名称'].values[0]
        tempDict['目标股数'] = tempDF['目标股数'].values[0]
        tempDict['目标市值'] = tempDF['目标市值'].values[0]
        tempDict['交易市值'] = tempDF['交易市值'].values[0]
        tempDict['用户授权资金'] = tempDF['用户授权资金'].values[0]
        tempDict['状态'] = tempDF['状态'].values[0]
        outStockCodeDict[each] = tempDict
    return outStockCodeDict


def getstocklist(account, allTradingRecords):
# 这个是针对于那些不在当日用户信息表里的用户或股票的。我们无法像上面那个函数一样获取相关信息（交易市值、目标市值等），于是我们返回
# 一个空的字典，但是该有的形式还是得有，该有的字段还是得有，格式要统一。
    file = allTradingRecords[allTradingRecords['资金账号'] == account]

    stockCodeList = file.drop_duplicates(["代码"], keep="first")["代码"].values
    outStockCodeDict = {}
    for each in stockCodeList:
        tempDict = {}
        tempDict['股票名称'] = jq.get_security_info(each).display_name
        tempDict['目标市值'] = NaN
        tempDict['交易市值'] = NaN
        tempDict['用户授权资金'] = NaN
        tempDict['状态'] = NaN
        outStockCodeDict[each] = tempDict
    # 获取股票列表
    return outStockCodeDict


# 上面两个函数都是服务这个函数的。这个函数根据“交易记录文件夹”里面所有有交易记录的用户，给他们分别提取他们的单票的信息（比如交易市值啊等）
# 结果返回一个字典，字典的key是所有有交易记录的用户的资金账号，value是该用户的信息，也是一个字典的格式。用户信息中就包括了单票信息。
# 最骚的就是，有具体股票信息的，只有当天在用户信息表里面出现过的用户，而且出现的信息也仅仅是他们正在做的票的信息，他们历史上做的票的信息不会
# 出现。其他今天没有做交易的用户，他们的股票信息都是空的。
def dealBookDate(df, allTradingRecords):
    # df:读取用户配置表t0_total_stock_list获得的DataFrame
    # 这个函数感觉有问题，每天的交易记录都包括历史上所有人的所有记录，如果用户a做过10只股票，但是今天会不会只在用户配置表中只有4只，然后此时他
    # 剩下的股票就没有
    cusDict = {}
    # 获取交易记录里的资金账号列表    
    fileList = find_files(os.listdir(jiaoyipath), r'.*\.csv')
    List = []
    List1 = []
    for each in fileList:
        a = int(each.split('-')[-1][:-4])
        b = str(each.split('-')[1])
        List.append(a)  # 资金账号list
        List1.append(b)  # 客户姓名list
    # 获取用户配置表里的资金账号列表
    tempList = df.drop_duplicates(["资金账户"], keep="last")['资金账户'].values
    # print(tempList)
    for i in range(len(List)):
        # 如果资金账号在用户配置表里，调取配置表信息
        if List[i] in tempList:
            cusDitalDict = {}
            tagDF = df[df['资金账户'] == List[i]]
            cusDitalDict['客户姓名'] = tagDF['客户姓名'].values[0]
            cusDitalDict['股票列表'] = dealACumRecorder(tagDF)  # 以标的代码为key、股票信息字典为value
            cusDitalDict['营业部'] = getYYBName(List[i])
            cusDict[List[i]] = cusDitalDict
        # 如果不在，获取相关的数据
        else:
            cusDitalDict = {}
            cusDitalDict['客户姓名'] = List1[i]
            cusDitalDict['股票列表'] = getstocklist(List[i], allTradingRecords)
            cusDitalDict['营业部'] = getYYBName(List[i])
            cusDict[List[i]] = cusDitalDict

    return cusDict


def getYongJinRate(df):
# 从交易记录估算该用户的佣金率
    # 默认佣金为万3.5
    defaultYongJinRate = 0.00035
    # 对交易数据按时间降序，取第一个手续费不为0，方向为买入的数据估算其佣金
    tempDf = df[(df['方向'] == '买') & (df['手续费'] != 0)].sort_values(by=['日期'], ascending=(False))
    # 手续费的估算要用手续费大于5的，且成交金额要足够大才准确。（为啥是20000可以问一下嘉林）
    SXFlist = tempDf[(df['手续费'] > 5) & (df["成交金额"] > 20000)]['手续费'].values
    bugMoneyList = tempDf[(df['手续费'] > 5) & (df["成交金额"] > 20000)]['成交金额'].values
    # 如果手续费记录不是空的
    if len(SXFlist) != 0:
        yongJinRate = SXFlist[0] / bugMoneyList[0]
    else:
        yongJinRate = defaultYongJinRate

    # 是否免五。如果最小手续费出现了小于5的数字，就免五了，反正默认不免五。
    if len(SXFlist) != 0 and SXFlist.min() < 5 and SXFlist.min() > 0:
        isMian5 = True
    else:
        isMian5 = False

    return yongJinRate, isMian5


def getTotleFee(df, soldMoneyList, buyMoneyList, yongJinRate, isMian5, YHSRate):
# 根据某客户某一天某一支票的交易记录计算该票交易过程中所产生的所有的手续费
    feeList = df[(df['成交金额'] != 0) & (df['手续费'] != 0)]['手续费']

    # 历史数据有佣金费率了的计算方法
    if not feeList.empty:
        return feeList.sum()
    # 没有柜台手续费，同时客户不免5的
    elif not isMian5:

        totleSoldFee = 0
        totleBuyFee = 0

        for each in soldMoneyList:
            a = each * yongJinRate
            if a >= 5:
                totleSoldFee = totleSoldFee + a
            elif a > 0:
                totleSoldFee = totleSoldFee + 5

        for each in buyMoneyList:
            b = each * yongJinRate
            if b >= 5:
                totleBuyFee = totleBuyFee + b
            elif b > 0:
                totleBuyFee = totleBuyFee + 5

        return soldMoneyList.sum() * YHSRate + totleSoldFee + totleBuyFee
    # 没有柜台手续费，客户免五
    else:

        return soldMoneyList.sum() * (yongJinRate + YHSRate) + buyMoneyList.sum() * yongJinRate


def getPingCangAndPing(soldNum, buyNum, code, day):
# 从交易记录判断该用户该票是否平仓，若未平仓或零股，就用收盘价估算平仓所需要的资金，为后面估算收益做准备
    if soldNum - buyNum == 0:
        pc = 0
        ping = "平"
    else:
        date = datetime.datetime.strptime(day, '%Y-%m-%d')
        year = date.year
        month = date.month
        day = date.day
        pc = jq.get_price(code, count=1, end_date=datetime.datetime(year, month, day, 15, 0, 0), fields=['close'],
                          frequency="1m", fq=None) * (soldNum - buyNum)  # pc为最终平仓所需金额
        pc = pd.DataFrame(pc)
        pc = pc["close"][0]
        if abs(soldNum - buyNum) < 100:
            if (soldNum - buyNum) > 0:
                ping = "零股" + "(" + "多卖" + str(soldNum - buyNum) + "股)"
            if (soldNum - buyNum) < 0:
                ping = "零股" + "(" + "少卖" + str(-soldNum + buyNum) + "股)"
        else:
            if (soldNum - buyNum) > 0:
                ping = "未平" + "(" + "多卖" + str(soldNum - buyNum) + "股)"
            if (soldNum - buyNum) < 0:
                ping = "未平" + "(" + "少卖" + str(-soldNum + buyNum) + "股)"
    return ping, pc


def dealDataProcess(df, code, yongJinRate, isMian5, day):
# 根据交易记录计算某个用户某一天某一支票的收益以及手续费等信息。上面的2个函数都是服务这个的。
    # 计算收益、计算是否平仓、计算当日佣金
    YHSRate = 0.001
    aDict = {}
    # 卖出金额
    soldMoney = df[df['方向'] == '卖']['成交金额'].sum()
    soldMoneyList = df[df['方向'] == '卖']['成交金额'].values
    # 卖出数量
    soldNum = df[df['方向'] == '卖']['成交数量'].sum()
    # 买入金额
    buyMoney = df[df['方向'] == '买']['成交金额'].sum()
    buyMoneyList = df[df['方向'] == '买']['成交金额'].values
    # 买入数量
    buyNum = df[df['方向'] == '买']['成交数量'].sum()
    # 总手续费
    totleFee = getTotleFee(df, soldMoneyList, buyMoneyList, yongJinRate, isMian5, YHSRate)
    # 印花税
    YHS = soldMoney * YHSRate
    # 佣金
    yongJin = totleFee - YHS
    # 是否平仓,平仓需要的资金
    ping, pc = getPingCangAndPing(soldNum, buyNum, code, day)
    # 当天盈利
    dayReturn = soldMoney - buyMoney - totleFee - pc
    # 扣除手续费前盈利
    Return_before_fee = soldMoney - buyMoney - pc

    aDict['dayReturn'] = dayReturn
    aDict['yongJin'] = yongJin
    aDict['ping'] = ping
    aDict['totleFee'] = totleFee
    aDict['return_before_fee'] = Return_before_fee
    return aDict


def getAcusStockCodeList(df):
    stockCodeList = df.drop_duplicates(["代码"], keep="last")['代码'].values
    return stockCodeList


def get_num_trading(df):
# 返回某个用户某一天某只股票的胜负次数，为计算胜率做准备
    df = df.reset_index(drop=True)
    n_row = len(df)
    total_num_trading = 0
    win_num_trading = 0
    buy_sell_num = 0
    buy_num = 0
    sell_num = 0
    buy_money = 0
    sell_money = 0
    for i in range(n_row):
        if df.iloc[i,]['方向'] == '买':
            buy_sell_num = buy_sell_num + df.iloc[i,]['成交数量']
            buy_num = buy_num + df.iloc[i,]['成交数量']
            buy_money = buy_money + df.iloc[i,]['成交金额']
        elif df.iloc[i,]['方向'] == '卖':
            buy_sell_num = buy_sell_num - df.iloc[i,]['成交数量']
            sell_money = sell_money + df.iloc[i,]['成交金额']
            sell_num = sell_num + df.iloc[i,]['成交数量']
        if -100 < buy_sell_num < 100:
            total_num_trading = total_num_trading + 1
            if sell_num != 0 and buy_num != 0:
                avg_sell = sell_money / sell_num
                avg_buy = buy_money / buy_num
                if avg_sell > avg_buy:
                    win_num_trading = win_num_trading + 1
                buy_sell_num = 0
                buy_num = 0
                sell_num = 0
                sell_money = 0
                buy_money = 0
            else:
                total_num_trading = total_num_trading - 1
                buy_sell_num = 0
                buy_num = 0
                sell_num = 0
                sell_money = 0
                buy_money = 0

    return total_num_trading, win_num_trading


# 返回列表的绝对值
def ABS(List):
    newList = []
    for i in List:
        a = abs(i)
        newList.append(a)
    return newList


def getDealDayList(beginDay):
    # 由于有时候雨茹会调整交易记录，所以需要将beginDay设置成她调节记录的那天，重新算从那天开始之后的收益
    # 平时没有调整交易记录的时候，
    dealDateList = list()
    for date in daylist4:
        if changedate(date) >= changedate(beginDay):
            dealDateList.append(date)
    dealDateList.append(today)
    return dealDateList


def get2float(src):
    return float('%.2f' % src)


# ----------------------------------主处理函数-------------------------------------------#
# 这里的函数都是和生成单票收益、组合收益、营业部收益报告表直接相关的函数
# 附：index=["资金账号", "营业部", "账户名", "交易日期", "股票代码", "股票级数","股票名称", "市值",
# "交易市值","当天交易额", "换手率%", "当天收益",
# "收益率%", "累计收益", "佣金", "平仓", "连续亏损天数", "胜率",
# "佣金率","用户授权资金","现金收益率%","现金七日年化收益率%","连续盈利天数","当前仓位%","撤单率%",
# "盈亏类型","十日亏损天数","十日盈亏比","十日盈利天数","五日累积收益率%"]

def processAStockADay(df, account, day, YYB, yongJinRate, isMian5, cumName, stockCode, stockDict, index, stockrankdf,
                      summary_dan):
    # 创建一条记录
    aRecord = {}
    # 资金账号
    aRecord[index[0]] = account
    # 营业部
    aRecord[index[1]] = YYB
    # 账户名
    aRecord[index[2]] = cumName
    # 交易日期
    aRecord[index[3]] = day
    # print(day,type(day))
    # 股票代码
    aRecord[index[4]] = stockCode
    # 股票级数
    # print(stockCode)
    a = stockrankdf[stockrankdf["code"] == stockCode]
    if a.empty:
        aRecord[index[5]] = 1
    else:
        aRecord[index[5]] = a["ALPHA-T回测收益分级"].values[0]

    # 股票名称
    aRecord[index[6]] = jq.get_security_info(stockCode).display_name
    # 目标市值
    if day == today:
        if pd.isnull(stockDict[stockCode]['目标市值']):
            aRecord[index[7]] = NaN
        else:
            aRecord[index[7]] = int(stockDict[stockCode]['目标市值'])
    elif day in daylist4:
        oldinfo = summary_dan[
            (summary_dan["交易日期"] == day) & (summary_dan["资金账号"] == account) & (summary_dan["股票代码"] == stockCode)]
        oldinfo = oldinfo.reset_index(drop=True)
        if oldinfo.empty:
            aRecord[index[7]] = NaN
        else:
            aRecord[index[7]] = oldinfo["市值"][0]
    else:
        aRecord[index[7]] = NaN
    # 交易市值
    if day == today:
        if pd.isnull(stockDict[stockCode]['交易市值']):
            aRecord[index[8]] = NaN
        else:
            aRecord[index[8]] = int(stockDict[stockCode]['交易市值'])
    elif day in daylist4:
        oldinfo = summary_dan[
            (summary_dan["交易日期"] == day) & (summary_dan["资金账号"] == account) & (summary_dan["股票代码"] == stockCode)]
        oldinfo = oldinfo.reset_index(drop=True)
        if oldinfo.empty:
            aRecord[index[8]] = NaN
        else:
            aRecord[index[8]] = oldinfo['交易市值'][0]
    else:
        aRecord[index[8]] = NaN
    # 当天交易额
    aRecord[index[9]] = df['成交金额'].sum()
    # 当天换手率
    if pd.isnull(aRecord[index[7]]):
        aRecord[index[10]] = NaN
    elif aRecord[index[8]] == 0:
        aRecord[index[10]] = NaN
    else:
        aRecord[index[10]] = get2float(100 * aRecord[index[9]] / aRecord[index[8]])  # 从6月21日起，换手率 = 当天交易额/交易市值，之前是= 当天交易额/目标市值

    # 处理当天收益相关数据                                
    dealDict = dealDataProcess(df, stockCode, yongJinRate, isMian5, day)
    # 当天收益
    aRecord[index[11]] = get2float(dealDict['dayReturn'])
    # 当天收益率 （与换手率不同，当天收益率采用目标市值作为基底，这对于投资者来说是比较合理的。）
    if pd.isnull(aRecord[index[7]]):
        aRecord[index[12]] = NaN
    elif aRecord[index[7]] == 0:
        aRecord[index[12]] = NaN
    else:
        aRecord[index[12]] = get2float(dealDict['dayReturn'] * 100 / aRecord[index[7]])
    # 累计收益
    aRecord[index[13]] = get2float(dealDict['dayReturn'])
    # 佣金
    aRecord[index[14]] = get2float(dealDict['yongJin'])
    # 平仓
    aRecord[index[15]] = dealDict['ping']
    # 连续亏损天数
    if dealDict['dayReturn'] < 0:
        aRecord[index[16]] = 1
    else:
        aRecord[index[16]] = 0
    # 胜率
    total_num_trading, win_num_trading = get_num_trading(df)
    if total_num_trading != 0:
        aRecord[index[17]] = get2float(win_num_trading / total_num_trading * 100)
    else:
        aRecord[index[17]] = NaN

    # 佣金率
    aRecord[index[18]] = yongJinRate
    # 使用资金
    if day == today:
        if pd.isnull(stockDict[stockCode]['用户授权资金']):
            aRecord[index[19]] = NaN
        else:
            aRecord[index[19]] = int(stockDict[stockCode]['用户授权资金'])
    elif day in daylist4:
        oldinfo = summary_dan[
            (summary_dan["交易日期"] == day) & (summary_dan["资金账号"] == account) & (summary_dan["股票代码"] == stockCode)]
        oldinfo = oldinfo.reset_index(drop=True)
        if oldinfo.empty:
            aRecord[index[19]] = NaN
        else:
            aRecord[index[19]] = oldinfo["用户授权资金"][0]
    else:
        aRecord[index[19]] = NaN

    # 现金收益率%
    if pd.isnull(aRecord[index[19]]):
        aRecord[index[20]] = NaN
    elif aRecord[index[19]] == 0:
        aRecord[index[20]] = NaN
    else:
        aRecord[index[20]] = get2float(dealDict['dayReturn'] * 100 / aRecord[index[19]])

    # 现金七日年化收益率%
    # 年化现金收益率自6月26日以来，是每日收益率乘以250，之前是乘以240
    if pd.isnull(aRecord[index[20]]):
        aRecord[index[21]] = NaN
    else:
        aRecord[index[21]] = get2float(aRecord[index[20]] * 250)

    # 连续盈利天数
    if dealDict['dayReturn'] > 0:
        aRecord[index[22]] = 1
    else:
        aRecord[index[22]] = 0

    # 当天仓位
    if not pd.isnull(aRecord[index[7]]) and not pd.isnull(aRecord[index[8]]):
        aRecord[index[23]] = get2float(aRecord[index[8]] / aRecord[index[7]]) * 100
    else:
        aRecord[index[23]] = NaN

    # 撤单率
    aRecord[index[24]] = get2float(df['撤单数量'].sum() / df['委托数量'].sum() * 100)

    # 盈亏类型
    if aRecord[index[11]] >= 0:
        aRecord[index[25]] = '当日无亏'
    else:
        if aRecord[index[15]] == '平':
            if dealDict['return_before_fee'] > 0:
                aRecord[index[25]] = '算法盈利但因手续费亏'
            if dealDict['return_before_fee'] == 0:
                aRecord[index[25]] = '算法不盈不亏但因手续费亏'
            if dealDict['return_before_fee'] < 0:
                aRecord[index[25]] = '算法亏损但因手续费更亏'
        else:
            if dealDict['return_before_fee'] > 0:
                aRecord[index[25]] = '按收盘价平仓后盈利，但因手续费亏'
            if dealDict['return_before_fee'] == 0:
                aRecord[index[25]] = '按收盘价平仓后不盈不亏，但因手续费亏'
            if dealDict['return_before_fee'] < 0:
                aRecord[index[25]] = '按收盘价平仓后亏损，但因手续费更亏'
                # 反正都是手续费的锅

    # 十日亏损天数
    aRecord[index[26]] = aRecord[index[16]]

    # 十日盈亏金额比
    if aRecord[index[11]] > 0:
        aRecord[index[27]] = '正无穷'  # 第一日
    elif aRecord[index[11]] == 0:
        aRecord[index[27]] = 1
    else:
        aRecord[index[27]] = 0

    # 十日盈利天数
    aRecord[index[28]] = aRecord[index[22]]

    # 五日累积收益率% (目标市值加权)
    if pd.isnull(aRecord[index[12]]):
        aRecord[index[29]] = NaN
    else:
        aRecord[index[29]] = aRecord[index[12]]

    return aRecord


def processADay(allTradingRecords, allCusOutDf, bookDict, isMian5Dt, day):
    aDayRecords = allTradingRecords[allTradingRecords['日期'] == day]  # 当天的交易记录
    aDayoutDf = pd.DataFrame()
    aDayAccounts = aDayRecords.drop_duplicates('资金账号')['资金账号'].values
    for account in aDayAccounts:
        # 今天，一个客户
        aCusTodayRecord = aDayRecords[aDayRecords['资金账号'] == account]
        print('开始处理{}的数据'.format(account))
        tempDict = bookDict[account]
        cumName = tempDict['客户姓名']
        stockDict = tempDict['股票列表']
        YYB = tempDict['营业部']
        # 官网的佣金率可以直接用昨天的佣金和价格计算
        yongJinRate, isMian5 = getYongJinRate(
            allTradingRecords[(allTradingRecords['资金账号'] == account) & (allTradingRecords['日期'] != today)])
        stockCodeList = getAcusStockCodeList(aCusTodayRecord)

        for stockCode in stockCodeList:
            # 今天，一个客户，一个股票
            stockCodeCodeDayTagDF = aCusTodayRecord[aCusTodayRecord['代码'] == stockCode]
            # 创建一个列表，储存最近7天的现金收益率
            rateweeklist = []
            # 创建一个列表，储存最近10天的盈亏天数
            tendayslist = []
            # 创建一个列表，储存最近10天的盈利天数
            tendayslist_win = []
            # 创建一个列别，储存最近10天的盈亏金额比
            profitlosslist = []
            # 创建一个列表，储存最近5天的收益率
            returnweek_list = []
            # 创建一个列表，储存最近5日的目标市值
            target_mark = []
            aRecorderDict = processAStockADay(stockCodeCodeDayTagDF, account, day, YYB, yongJinRate, isMian5, cumName,
                                              stockCode, stockDict, index, stockrankdf, summary_dan)

            # 从summary_dan中提取该用户该股票近十个有交易的日期的数据
            recentData = allCusOutDf[(allCusOutDf['资金账号'] == account) & (allCusOutDf['股票代码'] == stockCode)][-10:]
            recentData = recentData.sort_values(by=['交易日期'], ascending=(True))
            # 这里有个问题，recentdata可能是没有的，如果是今天刚刚开始做

            rateweeklist = list(recentData['现金收益率%'][-6:])
            rateweeklist.append(aRecorderDict["现金收益率%"])

            tendayslist = list(recentData['连续亏损天数'][-9:])
            tendayslist.append(aRecorderDict["连续亏损天数"])

            tendayslist_win = list(recentData['连续盈利天数'][-9:])
            tendayslist_win.append(aRecorderDict["连续盈利天数"])

            profitlosslist = list(recentData['当天收益'][-9:])
            profitlosslist.append(aRecorderDict["当天收益"])

            returnweek_list = list(recentData['收益率%'][-4:])
            returnweek_list.append(aRecorderDict['收益率%'])

            target_mark = list(recentData['市值'][-4:])
            target_mark.append(aRecorderDict['市值'])

            # 拿出昨天的收益
            lastDayRecord = recentData[-1:]
            # 如果股票不是第一天做，需要根据昨天的收益数据对今天进行调整
            if not lastDayRecord.empty:
                # 计算累计收益
                aRecorderDict['累计收益'] = lastDayRecord['累计收益'].values[0] + aRecorderDict['累计收益']
                # 计算连续亏损天数
                if lastDayRecord['连续亏损天数'].values[0] != 0 and aRecorderDict['连续亏损天数'] == 1:
                    aRecorderDict['连续亏损天数'] = lastDayRecord['连续亏损天数'].values[0] + 1
                # 计算连续盈利天数
                if lastDayRecord["连续盈利天数"].values[0] != 0 and aRecorderDict["连续盈利天数"] == 1:
                    aRecorderDict["连续盈利天数"] = lastDayRecord["连续盈利天数"].values[0] + 1
                # 计算过去十日亏损总天数
                if len(recentData) < 10:
                    # 如果股票还不到十天，那可以直接用昨天的天数加今天的
                    aRecorderDict["十日亏损天数"] = recentData[-1:]['十日亏损天数'].values[0] + aRecorderDict['十日亏损天数']
                    aRecorderDict["十日盈利天数"] = recentData[-1:]['十日盈利天数'].values[0] + aRecorderDict['十日盈利天数']
                else:
                    # 如果股票做了十天或者以上，要判断十天前是盈利还是亏损
                    # 如果十天前盈利，isEarned==1， 如果亏损，1-isEarned==1
                    isEarned = int(recentData[:1]['当天收益'].values[0] > 0)
                    aRecorderDict["十日亏损天数"] = recentData[-1:]['十日亏损天数'].values[0] + aRecorderDict['十日亏损天数'] - (
                                1 - isEarned)

                    aRecorderDict["十日盈利天数"] = recentData[-1:]['十日盈利天数'].values[0] + aRecorderDict['十日盈利天数'] - isEarned
                    # 计算现金七日年化收益率
            if not pd.isnull(aRecorderDict["现金收益率%"]):
                aRecorderDict["现金七日年化收益率%"] = get2float(
                    250 * np.nansum(rateweeklist) / len([i for i in rateweeklist if not pd.isnull(i)]))
            else:
                aRecorderDict["现金七日年化收益率%"] = NaN

            # 计算过去十日盈利日的利润与亏损日的损失的金额比
            profit = 0
            loss = 0
            for j in range(len(profitlosslist)):
                if profitlosslist[j] >= 0:
                    profit = profit + profitlosslist[j]
                else:
                    loss = loss + profitlosslist[j]
            if loss == 0:
                aRecorderDict["十日盈亏金额比"] = '正无穷'
            else:
                aRecorderDict["十日盈亏金额比"] = get2float(profit / ((-1) * loss))

            # 计算过去5日累积收益率%
            if not pd.isnull(aRecorderDict['收益率%']):
                if not pd.isnull(target_mark).all() and not pd.isnull(returnweek_list).all():
                    sum_product = 0
                    sum_target_mark = 0
                    count = 0
                    for i in range(len(returnweek_list)):
                        if not pd.isnull(target_mark[i]) and not pd.isnull(returnweek_list[i]):
                            product = target_mark[i] * returnweek_list[i]
                            sum_product = product + sum_product
                            sum_target_mark = sum_target_mark + target_mark[i]
                            count = count + 1
                    if sum_target_mark != 0:
                        aRecorderDict['五日累积收益率%'] = get2float(sum_product / sum_target_mark * count)
                    else:
                        aRecorderDict['五日累积收益率%'] = NaN
                else:
                    aRecorderDict["五日累积收益率%"] = NaN
            else:
                aRecorderDict['五日累积收益率%'] = NaN

            aCusaDayaStockOutDf = pd.DataFrame(aRecorderDict, index=[0])
            # aCusaDayOutDf = pd.concat([aCusOutDf, aRecordDf], axis=0)
            aDayoutDf = pd.concat([aDayoutDf, aCusaDayaStockOutDf], axis=0)
            isMian5Dt[account] = isMian5
    return aDayoutDf, isMian5Dt


def subProcess(bookDict, index, summary_dan, stockrankdf, allTradingRecords, beginDay):
    # 除非雨茹调了交易记录，否则beginDay = lastWeekDay
    # 如果雨茹调了交易记录，beginDay=被调日期中最前面的一天

    # 获取从beginDay到今天的日期列表（需要根据交易记录计算的）
    dealDayList = getDealDayList(beginDay)
    isMian5Dt = dict()

    # 获取beginDay之前的收益记录 直接加进输出的allCusOutDf
    allCusOutDf = summary_dan[summary_dan['交易日期'].apply(lambda x: changedate(x) < changedate(beginDay))]
    # beginDay以及之后的收益记录，需要根据交易记录计算
    for date in dealDayList:
        # 计算一日收益
        print(ATimeTool.getnow(), '计算{}收益'.format(date))
        aDayOutDf, isMian5Dt = processADay(allTradingRecords, allCusOutDf, bookDict, isMian5Dt, date)
        allCusOutDf = pd.concat([allCusOutDf, aDayOutDf], axis=0)

    # 更新收益结束，排序然后输出
    allCusOutDf = allCusOutDf.sort_values(by=['资金账号', '交易日期'], ascending=(True, True))
    allCusOutDf = allCusOutDf[index]
    return allCusOutDf, isMian5Dt


def processACusComADay(df, day, trad_dayrecord, yongJinRate, isMian5, index):
    # trad_dayrecord:一个用户一天的交易记录
    # df: 今日某客户所有股票的单票收益记录

    # 创建一条记录
    aRecord = df.iloc[0, :].to_dict()
    # 资金账号
    # aRecord[index[0]] = account
    # 营业部
    # aRecord[index[1]] = YYB
    # 账户名
    # aRecord[index[2]] =cumName
    # 交易日期
    # aRecord[index[3]] = day
    # 股票代码
    aRecord[index[4]] = NaN
    # 股票级数
    aRecord[index[5]] = NaN
    # 股票名称
    aRecord[index[6]] = '股票组合'
    # 市值
    if df['市值'].isnull().all():
        aRecord[index[7]] = NaN
    else:
        aRecord[index[7]] = df['市值'].sum()

    # 交易市值
    if df['交易市值'].isnull().all():
        aRecord[index[8]] = NaN
    else:
        aRecord[index[8]] = df['交易市值'].sum()

    # 当天交易额
    aRecord[index[9]] = df['当天交易额'].sum()

    # 当天换手率
    if (not df['交易市值'].isnull().all()) and (not df['换手率%'].isnull().all()):
        trading_values = list(df['交易市值'].values)
        turnover_rates = list(df['换手率%'].values)
        nrow = len(df)
        sum_product = 0
        for j in range(nrow):
            if (not pd.isnull(trading_values[j])) and (not pd.isnull(turnover_rates[j])):
                product = turnover_rates[j] * trading_values[j]
                sum_product = sum_product + product
        aRecord[index[10]] = get2float(sum_product / aRecord[index[8]])

    else:
        aRecord[index[10]] = NaN

        # 当天收益
    aRecord[index[11]] = get2float(df['当天收益'].sum())

    # 当天收益率
    if not pd.isnull(aRecord[index[7]]) and (not df["收益率%"].isnull().all()):
        target_values = list(df["市值"].values)
        return_rates = list(df["收益率%"].values)
        nrow = len(df)
        sum_product = 0
        for m in range(nrow):
            if (not pd.isnull(target_values[m])) and (not pd.isnull(return_rates[m])):
                product = target_values[m] * return_rates[m]
                sum_product = sum_product + product
        aRecord[index[12]] = get2float(sum_product / aRecord[index[7]])
    else:
        aRecord[index[12]] = NaN

    # 累计收益
    aRecord[index[13]] = aRecord[index[11]]

    # 佣金
    aRecord[index[14]] = get2float(df[index[14]].sum())

    # 平仓
    aRecord[index[15]] = NaN

    # 连续亏损天数
    if aRecord[index[11]] < 0:
        aRecord[index[16]] = 1
    else:
        aRecord[index[16]] = 0

    # 胜率
    list_of_stocks = list(df['股票代码'].values)
    total_num_trading = 0
    win_num_trading = 0
    for stock in list_of_stocks:
        trad_daystockrecord = trad_dayrecord[trad_dayrecord['代码'] == stock].reset_index(drop=True)
        total_num_stocktrading, win_num_stocktrading = get_num_trading(trad_daystockrecord)
        total_num_trading = total_num_trading + total_num_stocktrading
        win_num_trading = win_num_trading + win_num_stocktrading
    if total_num_trading != 0:
        aRecord[index[17]] = get2float(win_num_trading / total_num_trading * 100)
    else:
        aRecord[index[17]] = NaN

    # 佣金率
    aRecord[index[18]] = yongJinRate

    # 使用资金
    aRecord[index[19]] = get2float(df["用户授权资金"].iloc[0])

    # 使用目标市值加权的现金收益率
    if (not pd.isnull(aRecord[index[19]])) and (not df["市值"].isnull().all()) and (not df["现金收益率%"].isnull().all()):
        target_values = list(df["市值"].values)
        return_percash = list(df["现金收益率%"].values)
        nrow = len(df)
        sum_product = 0
        for n in range(nrow):
            if (not pd.isnull(target_values[n])) and (not pd.isnull(return_percash[n])):
                product = target_values[n] * return_percash[n]
                sum_product = sum_product + product
        aRecord[index[20]] = get2float(sum_product / aRecord[index[7]])
    else:
        aRecord[index[20]] = NaN

    # 七日年化现金收益率
    if pd.isnull(aRecord[index[20]]):
        aRecord[index[21]] = NaN
    else:
        aRecord[index[21]] = get2float(aRecord[index[20]] * 250)

    # 连续盈利天数
    if aRecord[index[11]] > 0:
        aRecord[index[22]] = 1
    else:
        aRecord[index[22]] = 0

    # 当天仓位
    if not pd.isnull(aRecord[index[7]]):
        target_markvalue = list(df['市值'].values)
        position = list(df['当天仓位%'].values)
        sum_of_product = 0
        count = 0
        row_num = len(df)
        for i in range(row_num):
            if not pd.isnull(target_markvalue[i]) and not pd.isnull(position[i]):
                product = target_markvalue[i] * position[i] / 100
                sum_of_product = sum_of_product + product
                count = count + 1
        if sum_of_product == 0 and count == 0:
            aRecord[index[23]] = NaN
        else:
            aRecord[index[23]] = get2float(sum_of_product / df['市值'].sum() * 100)
    else:
        aRecord[index[23]] = NaN

    # 撤单率
    aRecord[index[24]] = get2float(trad_dayrecord['撤单数量'].sum() / trad_dayrecord['委托数量'].sum() * 100)

    # 盈亏类型
    list_of_stocks = list(df['股票代码'].values)
    if aRecord[index[11]] >= 0:  # 收益不为负
        aRecord[index[25]] = '当日该用户组合不亏'
    else:  # 亏损情况下：
        total_returnbffee = 0
        for stock in list_of_stocks:
            stock_dayrecord = trad_dayrecord[trad_dayrecord['代码'] == stock]
            dealDict = dealDataProcess(stock_dayrecord, stock, yongJinRate, isMian5, day)
            total_returnbffee = total_returnbffee + dealDict['return_before_fee']
        if (df['平仓'] == '平').all():  # 如果该日该用户所有股票全是平仓的
            if total_returnbffee > 0:
                aRecord[index[25]] = '算法盈利但因手续费亏'
            elif total_returnbffee == 0:
                aRecord[index[25]] = '算法不盈不亏但因手续费亏'
            else:
                aRecord[index[25]] = '算法亏损但因手续费更亏'
        else:  # 该日该用户所持股票有未平仓的
            if total_returnbffee > 0:
                aRecord[index[25]] = '未平仓的按收盘价平仓后，组合整体盈利，但因手续费亏损'
            elif total_returnbffee == 0:
                aRecord[index[25]] = '未平仓的按收盘价平仓后，组合不盈不亏，但因手续费亏损'
            else:
                aRecord[index[25]] = '未平仓的按收盘价平仓后，组合整体亏损，但因手续费更亏'

                # 十日亏损天数
    aRecord[index[26]] = aRecord[index[16]]

    # 十日盈亏金额比
    if aRecord[index[11]] > 0:
        aRecord[index[27]] = '正无穷'
    elif aRecord[index[11]] == 0:
        aRecord[index[27]] = 1
    else:
        aRecord[index[27]] = 0

    # 十日盈利天数
    aRecord[index[28]] = aRecord[index[22]]

    # 五日累积收益率%
    if pd.isnull(aRecord[index[12]]):
        aRecord[index[29]] = NaN
    else:
        aRecord[index[29]] = aRecord[index[12]]

    return aRecord


def processComADay(aDayDanData, allTradingRecords, allCombOutDf, isMian5Dt, day):
    # 输入今天的单票收益记录aDayDanData, 交易记录allTradingRecords, 历史的组合记录allCombOutDf, 以及是否免五的dict, 处理的日期day
    # 输出更新组合记录allCombOutDf, 也就是把
    aDayOutDf = pd.DataFrame(columns=index)
    aDayRecords = allTradingRecords[allTradingRecords['日期'] == day]  # 当日：交易记录数据
    aDayAccounts = aDayDanData.drop_duplicates('资金账号')['资金账号'].values  # 当日：单票收益数据中的客户列表
    print('开始处理{}的交易记录数据'.format(day))
    for account in aDayAccounts:
        # 循环今天的每个用户       
        aCusComDf = pd.DataFrame()
        lastDayRecord = {}
        rateweeklist = []
        tendayslist = []
        tendayslist_win = []
        profitlosslist = []
        returnweek_list = []
        target_mark = []

        aCusAdayData = aDayDanData[aDayDanData['资金账号'] == account]  # 今日一个用户：单票收益数据
        aCusTodayRecord = aDayRecords[aDayRecords['资金账号'] == account]  # 今日一个用户：交易记录数据
        isMian5 = isMian5Dt[account]
        yongJinRate = aCusAdayData['佣金率'].values[0]
        aRecorderDict = processACusComADay(aCusAdayData, day, aCusTodayRecord, yongJinRate, isMian5, index)

        # 从allCombOutDf中提取该用户近十个有交易的日期的组合数据
        recentData = allCombOutDf[allCombOutDf['资金账号'] == account][-10:]
        recentData = recentData.sort_values(by=['交易日期'], ascending=(True))
        # 这里有个问题，recentdata可能是没有的，如果是今天刚刚开始做

        rateweeklist = list(recentData['现金收益率%'][-6:])
        rateweeklist.append(aRecorderDict["现金收益率%"])

        tendayslist = list(recentData['连续亏损天数'][-9:])
        tendayslist.append(aRecorderDict["连续亏损天数"])

        tendayslist_win = list(recentData['连续盈利天数'][-9:])
        tendayslist_win.append(aRecorderDict["连续盈利天数"])

        profitlosslist = list(recentData['当天收益'][-9:])
        profitlosslist.append(aRecorderDict["当天收益"])

        returnweek_list = list(recentData['收益率%'][-4:])
        returnweek_list.append(aRecorderDict['收益率%'])

        target_mark = list(recentData['市值'][-4:])
        target_mark.append(aRecorderDict['市值'])

        # 拿出昨天的收益
        lastDayRecord = recentData[-1:]
        # 如果股票不是第一天做，需要根据昨天的收益数据对今天进行调整
        if not lastDayRecord.empty:
            # 计算累计收益
            aRecorderDict['累计收益'] = lastDayRecord['累计收益'].values[0] + aRecorderDict['累计收益']
            # 计算连续亏损天数
            if lastDayRecord['连续亏损天数'].values[0] != 0 and aRecorderDict['连续亏损天数'] == 1:
                aRecorderDict['连续亏损天数'] = lastDayRecord['连续亏损天数'].values[0] + 1
            # 计算连续盈利天数
            if lastDayRecord["连续盈利天数"].values[0] != 0 and aRecorderDict["连续盈利天数"] == 1:
                aRecorderDict["连续盈利天数"] = lastDayRecord["连续盈利天数"].values[0] + 1
            # 计算过去十日亏损总天数
            if len(recentData) < 10:
                # 如果股票还不到十天，那可以直接用昨天的天数加今天的
                aRecorderDict["十日亏损天数"] = recentData[-1:]['十日亏损天数'].values[0] + aRecorderDict['十日亏损天数']
                aRecorderDict["十日盈利天数"] = recentData[-1:]['十日盈利天数'].values[0] + aRecorderDict['十日盈利天数']
            else:
                # 如果做了十天或者以上，要判断十天前是盈利还是亏损
                # 如果十天前盈利，isEarned==1， 如果亏损，1-isEarned==1
                isEarned = int(recentData[:1]['当天收益'].values[0] > 0)
                aRecorderDict["十日亏损天数"] = recentData[-1:]['十日亏损天数'].values[0] + aRecorderDict['十日亏损天数'] - (1 - isEarned)

                aRecorderDict["十日盈利天数"] = recentData[-1:]['十日盈利天数'].values[0] + aRecorderDict['十日盈利天数'] - isEarned
                # 计算现金七日年化收益率
        if not pd.isnull(aRecorderDict["现金收益率%"]):
            aRecorderDict["现金七日年化收益率%"] = get2float(
                250 * np.nansum(rateweeklist) / len([i for i in rateweeklist if not pd.isnull(i)]))
        else:
            aRecorderDict["现金七日年化收益率%"] = NaN

        # 计算过去十日盈利日的利润与亏损日的损失的金额比
        profit = 0
        loss = 0
        for j in range(len(profitlosslist)):
            if profitlosslist[j] >= 0:
                profit = profit + profitlosslist[j]
            else:
                loss = loss + profitlosslist[j]
        if loss == 0:
            aRecorderDict["十日盈亏金额比"] = '正无穷'
        else:
            aRecorderDict["十日盈亏金额比"] = get2float(profit / ((-1) * loss))

        # 计算过去5日累积收益率%
        if not pd.isnull(aRecorderDict['收益率%']):
            if not pd.isnull(target_mark).all() and not pd.isnull(returnweek_list).all():
                sum_product = 0
                sum_target_mark = 0
                count = 0
                for i in range(len(returnweek_list)):
                    if not pd.isnull(target_mark[i]) and not pd.isnull(returnweek_list[i]):
                        product = target_mark[i] * returnweek_list[i]
                        sum_product = product + sum_product
                        sum_target_mark = sum_target_mark + target_mark[i]
                        count = count + 1
                if sum_target_mark != 0:
                    aRecorderDict['五日累积收益率%'] = get2float(sum_product / sum_target_mark * count)
                else:
                    aRecorderDict['五日累积收益率%'] = NaN
            else:
                aRecorderDict["五日累积收益率%"] = NaN
        else:
            aRecorderDict['五日累积收益率%'] = NaN

        aCusaDayOutDf = pd.DataFrame(aRecorderDict, index=[0])
        aDayOutDf = pd.concat([aDayOutDf, aCusaDayOutDf], axis=0)
    return aDayOutDf


def combProcess(allCusOutDf, bookDict, index, summary_comb, allTradingRecords, isMian5Dt, beginDay):
    # index = ["0资金账号", "1营业部", "2账户名", "3交易日期", "4股票代码", "5股票级数", "6股票名称", "7市值", "8交易市值", "9当天交易额", "10换手率%",
    #          "11当天收益", "12收益率%", "13累计收益", "14佣金", "15平仓", "16连续亏损天数", "17胜率%", "18佣金率", "19用户授权资金", "20现金收益率%",
    #          "21现金七日年化收益率%", "22连续盈利天数", "23当天仓位%", "24撤单率%", "25盈亏类型", "26十日亏损天数", "27十日盈亏金额比", "28十日盈利天数",
    #          "29五日累积收益率%"]
    dealDayList = getDealDayList(beginDay)
    allCombOutDf = summary_comb[
        summary_comb['交易日期'].apply(lambda x: changedate(x) < changedate(beginDay))]  # beginDay之前的组合记录

    for date in dealDayList:
        # 根据当天的单票收益，计算组合收益
        print('开始处理{}的数据'.format(date))
        aDayDanData = allCusOutDf[allCusOutDf['交易日期'] == date]  # 当天单票收益数据
        aDayCombOutDf = processComADay(aDayDanData, allTradingRecords, allCombOutDf, isMian5Dt, date)  # 更新当天组合收益数据
        allCombOutDf = pd.concat([allCombOutDf, aDayCombOutDf], axis=0)

        # 更新组合收益结束，排序并输出
    allCombOutDf = allCombOutDf.sort_values(by=['资金账号', '交易日期'], ascending=(True, True))
    allCombOutDf = allCombOutDf[index]
    return allCombOutDf


# =================================processing================================================
index = ["资金账号", "营业部", "账户名", "交易日期", "股票代码", "股票级数", "股票名称", "市值", "交易市值", "当天交易额", "换手率%",
         "当天收益", "收益率%", "累计收益", "佣金", "平仓", "连续亏损天数", "胜率%", "佣金率", "用户授权资金", "现金收益率%",
         "现金七日年化收益率%", "连续盈利天数", "当天仓位%", "撤单率%", "盈亏类型", "十日亏损天数", "十日盈亏金额比", "十日盈利天数",
         "五日累积收益率%"]

today = time.strftime("%Y-%m-%d",time.localtime(time.time()))
# today = '2019-09-27'  # 这样就可以随便跑了
# 设置路径
desktop = "D:/Allen_UIBE/Career/Internship/JoinQuant/长谦财富/运行文件"
rootpath = desktop + "/运营数据"
bookName = 't0_total_stock_list_' + today.replace('-', '') + '.csv'
YYBname = '营业部配置表.xlsx'
jiaoyipath = 'trans-2019-10-11-17-01-01'
jiaoyipath = rootpath + "/" + '交易记录' + '/' + jiaoyipath  # 多个csv，每个csv记录单个用户当天和历史所有股票买卖记录
OutPath = rootpath + '/' + '收益计算结果'
bookFile = rootpath + '/' + '用户股票配置' + '/' + bookName  # 一个csv，t0_total_stock_list
YYBfile = rootpath + '/' + '营业部配置' + '/' + YYBname  # 一个csv，记录资金账号所属营业部
stockrankpath = rootpath + "/" + "Alpha-T个股近1月收益排名_20190925.xlsx"

# beginDay = '2019-09-27'
beginDay = getLastWeekDay(today)  # 默认从前一个工作日开始跑收益

jiaoYiNameDict = listFilename(jiaoyipath)  # jiaoYiNameDict：以所有资金账号为key、文件名（用户名）为value

print(ATimeTool.getnow(), ':', '开始读取数据')
# 读取股票所属收益级数表格
stockrankdf = pd.read_excel(stockrankpath, encoding="gbk")  # 票池的股票分级信息
# 获取配置表
bookDF = AFileTool.open_csv(bookFile)  # pd.read_csv(bookFile,encoding='gbk',engine='python')当天哪些用户的哪些股票在跑AlphaT
YYBDF = AFileTool.open_excel(YYBfile)  # pd.read_excel(YYBfile,encoding = "gbk")
print("开始读取已有的单票和组合收益")
# 获取已有记录的单票和组合收益表
summary_dan = getolddate(rootpath, 'dan')
summary_comb = getolddate(rootpath, 'comb')
# 获取所有交易记录
print('开始获取所有交易记录')
allTradingRecords = getAllRecords(jiaoyipath)
print('done')
# 日期列表
daylist4 = summary_dan.drop_duplicates(["交易日期"], keep="first")["交易日期"].values.tolist()
daylist4.sort()
print(daylist4)
# 获取配置表数据，创立一个以资金账号为key的dict
bookDict = dealBookDate(bookDF, allTradingRecords)
print(ATimeTool.getnow(), ':', '数据读取完成！')

print(ATimeTool.getnow(), '#----------------开始处理单票数据--------------------#')
allCusOutDf, isMian5Dt = subProcess(bookDict, index, summary_dan, stockrankdf, allTradingRecords, beginDay)
# 将这个文件写出到输出目录
OutPath = rootpath + '/' + '收益计算结果'
print(OutPath)
outFilePath = OutPath + '/' + today
outFileName = outFilePath + "/" + '汇总数据' + "/" + today + '_单票收益记录.xlsx'
dan_path = outFileName
# allCusOutDf.to_csv(outFileName,encoding="utf-8")
AFileTool.write_excel(allCusOutDf, outFileName)
print(ATimeTool.getnow(), '#----------------单票数据处理结束--------------------#')

print(ATimeTool.getnow(), '#----------------开始处理组合数据--------------------#')
allCombOutDf = combProcess(allCusOutDf, bookDict, index, summary_comb, allTradingRecords, isMian5Dt, beginDay)
outFileName = outFilePath + "/" + '汇总数据' + "/" + today + '_组合收益记录.xlsx'
zu_path = outFileName
# allCombOutDf.to_csv(outFileName,encoding="utf-8")
AFileTool.write_excel(allCombOutDf, outFileName)
print(ATimeTool.getnow(), '#----------------处理组合数据结束--------------------#')

print(ATimeTool.getnow(), '#----------------开始生成收益表格--------------------#')
dataTime = 21
OutPath = rootpath + '/' + '收益计算结果'
srcdir = OutPath + "/" + today + "/" + '汇总数据'
danPianFile = today + '_单票收益记录.xlsx'
zuHeFile = today + '_组合收益记录.xlsx'
a = ATExcel1(dataTime, OutPath, srcdir, danPianFile, zuHeFile, today)
b = ATExcel(dataTime, OutPath, srcdir, danPianFile, zuHeFile, today)
finalmain(dan_path, zu_path, jiaoyipath, rootpath)
# findpotentialclient(rootpath)

print(ATimeTool.getnow(), '#----------------收益表格处理结束--------------------#')
