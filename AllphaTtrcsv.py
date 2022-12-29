# -*- coding: utf-8 -*-
"""
Created on July 29 10:18:57 2019

@author: Lun
"""

"""
Created on Aug 10 13:54:04 2019

@author: Lun
"""

import pandas as pd 
import os,shutil
import time
import datetime
import numpy as np



def listFilename(path):
    #path 为交易记录 
    #输出一个以资金账号为key，文件名为value的dict
    fileList = os.listdir(path)
    fileNameDict = {}
    for each in fileList:
        a = int(each.split('-')[-1][:-4])
        fileNameDict[a] = each
    fileNameList = fileNameDict.keys()
    return fileNameDict
#获取股票资金下限函数
#调用参数 客户资金账号，日期，,股票代码，交易记录的路径

def changedate(day):
    numberlist=day.split("-")
    #year
    a=int(numberlist[0])
    #month
    b=int(numberlist[1])
        #day
    c=int(numberlist[2])
      #转换为数字
    daynumber=a*365+b*30+c
    return daynumber
    
def getolddate(rootpath):
    #遍历已有的收益计算结果文件
    yuanpath= rootpath + "/" + "收益计算结果"
    filelist = os.listdir(yuanpath)
    print(filelist)
    #获取需要的列
    index = ["资金账号", "营业部", "账户名", "交易日期", "股票代码", "股票名称", "市值", "当天交易额", "当天收益", "收益率%", "累计收益"]  
    #建立新的dataframe，用以储存所以的数据
    summary_dan=pd.DataFrame()
    summary_zu=pd.DataFrame()
    #按天获取组合汇总收益数据 
    #summary=pd.DataFrame()
    for riqi in filelist:
        print("开始汇总%s的数据"%(riqi),riqi)
        print(changedate(riqi))
        #读取当天数据
        if riqi=="2019-04-19" :
            path= yuanpath +"/"+riqi+"/"+"汇总数据"+"/"+riqi+"_组合收益记录.csv"
            df_zu=pd.read_csv(path,encoding="utf-8",engine="python")[index]
            path= yuanpath +"/"+riqi+"/"+"汇总数据"+"/"+riqi+"_单票收益记录.csv"
            df_dan=pd.read_csv(path,encoding="utf-8",engine="python")[index]
        elif changedate(riqi)>changedate("2019-04-20"):
            print("riqi:",riqi)
            path= yuanpath +"/"+riqi+"/"+"汇总数据"+"/"+riqi+"_组合收益记录.xlsx"
            df_zu=pd.read_excel(path,encoding="utf-8")[index]
            path= yuanpath +"/"+riqi+"/"+"汇总数据"+"/"+riqi+"_单票收益记录.xlsx"
            df_dan=pd.read_excel(path,encoding="utf-8")[index]
        else:
            path= yuanpath +"/"+riqi+"/"+"汇总数据"+"/"+riqi+"_组合收益记录.csv"
            df_zu=pd.read_csv(path,encoding="gbk",engine='python')[index]
            path= yuanpath +"/"+riqi+"/"+"汇总数据"+"/"+riqi+"_单票收益记录.csv"
            df_dan=pd.read_csv(path,encoding="gbk",engine='python')[index]
        df1=df_zu[df_zu["交易日期"]==riqi].reset_index(drop=True)
        df2=df_dan[df_dan["交易日期"]==riqi].reset_index(drop=True)
        summary_zu=pd.concat([summary_zu,df1],axis=0)
        summary_dan=pd.concat([summary_dan,df2],axis=0)
    summary_dan=summary_dan.reset_index(drop=True)
    summary_zu=summary_zu.reset_index(drop=True)
    return summary_dan

def mymovefile(srcfile,dstfile):
    if not os.path.isfile(srcfile):
        #print "%s not exist!"%(srcfile)
         0
    else:
        fpath,fname=os.path.split(dstfile)    #分离文件名和路径
        if not os.path.exists(fpath):
            os.makedirs(fpath)                #创建路径
        shutil.move(srcfile,dstfile)          #移动文件
        #print "move %s -> %s"%( srcfile,dstfile)

def mycopyfile(srcfile,dstfile):
    if not os.path.isfile(srcfile):
        #print "%s not exist!"%(srcfile)
        0
    else:
        fpath,fname=os.path.split(dstfile)    #分离文件名和路径
        if not os.path.exists(fpath):
            os.makedirs(fpath)                #创建路径
        shutil.copyfile(srcfile,dstfile)      #复制文件
                #print "copy %s -> %s"%( srcfile,dstfile)
def getYYBNameList(data_zu):
    outDF = data_zu.drop_duplicates(["营业部"], keep="first")
    return outDF["营业部"].tolist()


    # 获取今天要处理的XX营业部客户名单
def getYYBcustomerList(data_zu, YYB):
    tempDF = data_zu[data_zu['营业部'] == YYB]
    outDF = tempDF.drop_duplicates(["账户名"], keep="first")
    return outDF["账户名"].tolist()
def mkdir(path):
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

#交割记录营业部划分
def main(data_zu,jiaoyipath, rootpath ):
    print("开始处理交割记录")
    Path1 = jiaoyipath
    oldfilelist=os.listdir(Path1)
    today = time.strftime('%Y-%m-%d', time.localtime(time.time()))
    #print(today)
    nowpath= rootpath + "/" +today+"/"+"营业部数据"
    today_zu = data_zu[data_zu["交易日期"]==today].reset_index(drop=True)
    YYBList = getYYBNameList(today_zu)
    for each1 in YYBList:
        # 获得该营业部需要处理的用户列表
        YYBCTMList = getYYBcustomerList(data_zu, each1)    
        for each in oldfilelist:
            if each.split('-')[1] in YYBCTMList:
                srcPath = Path1 +"/" + each
                YYBPath = nowpath + "/" + each1+"/"+"交割记录"+"/"+ each
                
            # 将该营业部的文件放入目标目录
                mycopyfile(srcPath,YYBPath)  
    print("交割记录处理完成")
    
 #单票营业部划分
def getdan(data_dan,data_zu, rootpath ):
    print("开始处理营业部单票划分")
    #筛选出今天有交易记录的营业部
    today=time.strftime("%Y-%m-%d",time.localtime(time.time()))
    
    today_zu = data_zu[data_zu["交易日期"]==today].reset_index(drop=True)
    YYBList = getYYBNameList(today_zu)
    
    #print("today is %s"%(today))
    nowpath= rootpath + "/" +today+"/"+"营业部数据"
    index=data_dan.columns
    #print(index)
    for each in YYBList:
        # 获得该营业部需要处理的用户列表
        out=pd.DataFrame(columns=index)  
        out=data_dan[data_dan["营业部"]==each]
        strTemp = today + '_' + '单票汇总表' + ".xlsx"
        path1=nowpath+ "/"+ each
        mkdir(path1)
        filename=path1+"/"+strTemp
        # print(out)
        out.to_excel(filename, encoding='utf-8', index=False)
    print("营业部单票划分结束")
   
    
def finalmain(dan_path,zu_path,jiaoyipath,rootpath):
    rootpath = rootpath + "/" + "收益计算结果"
    #path='C:/Users/FGD_Moon/Desktop\运营数据\收益计算结果/2019-04-17\汇总数据/2019-04-17_组合收益记录.csv'    
    data_zu=pd.read_excel(zu_path, encoding='utf-8')
    #path='C:/Users/FGD_Moon/Desktop\运营数据\收益计算结果/2019-04-17\汇总数据/2019-04-17_单票收益记录.csv'
    data_dan=pd.read_excel(dan_path,encoding='utf-8')
    getdan(data_dan,data_zu,rootpath )    
    main(data_zu,jiaoyipath,rootpath )   
    today=datetime.datetime.strftime(datetime.date.today(),'%Y-%m-%d')
    print(today)
    path =rootpath +"/"+today+"/"+"汇总数据"+"/"+today+"_组合收益记录.xlsx"
    df=pd.read_excel(path,encoding="utf-8")
    #df.sort_values(by=["交易日期"],ascending=True)
    #获取需要处理的日期列表
    datelist=df.drop_duplicates(["交易日期"],keep="first")["交易日期"].values.tolist()
    #获取当日交易额最高的客户名单
    datelist.sort()
    #先筛选已有收益表格的日期
    datelist=datelist[61:]
    daylist=datelist
    #没有当天市值的日期
    print(len(daylist))
    print("日期列表：",daylist)
    #建立新的dataframe，用以储存所以的数据
    summary_dan=pd.DataFrame()
    summary_zu=pd.DataFrame()
    #按天获取组合汇总收益数据 
    #获取交易额排名前20的用户
    summary=pd.DataFrame()
    for i in range(len(daylist)):
        print("开始汇总%s的数据"%(daylist[i]))
        riqi=datelist[i]
        #读取当天数据
        if riqi=="2019-04-19" :
            path= rootpath +"/"+riqi+"/"+"汇总数据"+"/"+riqi+"_组合收益记录.csv"
            df_zu=pd.read_csv(path,encoding="utf-8",engine="python")
            path= rootpath +"/"+riqi+"/"+"汇总数据"+"/"+riqi+"_单票收益记录.csv"
            df_dan=pd.read_csv(path,encoding="utf-8",engine="python")
        elif changedate(riqi)>changedate("2019-04-20"):
            print("riqi:",riqi)
            path= rootpath +"/"+riqi+"/"+"汇总数据"+"/"+riqi+"_组合收益记录.xlsx"
            df_zu=pd.read_excel(path,encoding="utf-8")
            path= rootpath +"/"+riqi+"/"+"汇总数据"+"/"+riqi+"_单票收益记录.xlsx"
            df_dan=pd.read_excel(path,encoding="utf-8")
        else: 
            path= rootpath +"/"+riqi+"/"+"汇总数据"+"/"+riqi+"_组合收益记录.csv"
            df_zu=pd.read_csv(path,encoding="gbk",engine='python')
            path= rootpath +"/"+riqi+"/"+"汇总数据"+"/"+riqi+"_单票收益记录.csv"
            df_dan=pd.read_csv(path,encoding="gbk",engine='python')
           
        df1=df_zu[df_zu["交易日期"]==riqi].reset_index(drop=True)
        df2=df_dan[df_dan["交易日期"]==riqi].reset_index(drop=True)
        dict1={}
        dict1["交易日期"]=riqi
        dict1["当天市值"]=df1["市值"].sum()
        dict1["当天交易额"]=int(df1["当天交易额"].sum())
        dict1["当天收益"]=int(df1["当天收益"].sum())
        dict1["交易额加权换手率%"]=np.dot(df1["当天交易额"].values,df1["换手率%"].values)/dict1["当天交易额"]
        dict1["交易额加权换手率%"]=float('%.2f' % dict1["交易额加权换手率%"])
        dict1["交易额加权收益率"]=np.dot(df1["当天交易额"].values,df1["收益率%"].values)/dict1["当天交易额"]
        dict1["交易额加权换手率%"]=float('%.3f' % dict1["交易额加权换手率%"])
        aRecordDf = pd.DataFrame(dict1, index=[0])
        summary = pd.concat([summary, aRecordDf], axis=0)
        summary_zu=pd.concat([summary_zu,df1],axis=0)
        summary_dan=pd.concat([summary_dan,df2],axis=0)
        #print(summary_dan,summary_zu)
    path= rootpath +"/"+today+"/"+"汇总数据"+"/"+today+"_汇总数据.xlsx"
    summary.to_excel(path,encoding="utf-8")
    #path="C:/Users/FGD_Moon/Desktop\运营数据\收益计算结果"+"/"+today+"/"+"汇总数据"+"/"+today+"_汇总组合数据.xlsx"
    #summary_zu.to_excel(path,encoding="utf-8")
    #ath="C:/Users/FGD_Moon/Desktop\运营数据\收益计算结果"+"/"+today+"/"+"汇总数据"+"/"+today+"_汇总单票数据.xlsx"
    #summary_dan.to_excel(path,encoding="utf-8")
#定义查找潜在客户名单    
def findpotentialclient(rootPath):                      
    today=datetime.datetime.strftime(datetime.date.today(),'%Y-%m-%d')
    path= rootPath + "/" + "收益计算结果"+"/"+today+"/"+"汇总数据"+"/"+today+"_组合收益记录.xlsx"
    df=pd.read_excel(path,encoding="utf-8")
    #获取需要处理的日期列表
    datelist=df.drop_duplicates(["交易日期"],keep="first")["交易日期"].values
    #获取当日交易额最高的客户名单
    datelist.sort()
    #筛选近两周的已有收益表格的日期
    daylist=datelist[-14:]
    #建立新的dataframe，用以储存所以的数据
    summary_dan=pd.DataFrame()
    summary_zu=pd.DataFrame()
    summary=pd.DataFrame()
    #按天获取组合汇总收益数据 
    for i in range(len(daylist)):
        riqi=daylist[i]
        #读取当天数据
        if riqi=="2019-04-19" :
            path= rootPath + "/" + "收益计算结果"+"/"+riqi+"/"+"汇总数据"+"/"+riqi+"_组合收益记录.csv"
            df_zu=pd.read_csv(path,encoding="utf-8",engine="python")
            path= rootPath + "/" + "收益计算结果" +"/"+riqi+"/"+"汇总数据"+"/"+riqi+"_单票收益记录.csv"
            df_dan=pd.read_csv(path,encoding="utf-8",engine="python")
        elif changedate(riqi)>changedate("2019-04-20"):
            path= rootPath + "/" + "收益计算结果" +"/"+riqi+"/"+"汇总数据"+"/"+riqi+"_组合收益记录.xlsx"
            df_zu=pd.read_excel(path,encoding="utf-8")
            path= rootPath + "/" + "收益计算结果" +"/"+riqi+"/"+"汇总数据"+"/"+riqi+"_单票收益记录.xlsx"
            df_dan=pd.read_excel(path,encoding="utf-8")
        else:
            path= rootPath + "/" + "收益计算结果" +"/"+riqi+"/"+"汇总数据"+"/"+riqi+"_组合收益记录.csv"
            df_zu=pd.read_csv(path,encoding="gbk",engine='python')
            path= rootPath + "/" + "收益计算结果" +"/"+riqi+"/"+"汇总数据"+"/"+riqi+"_单票收益记录.csv"
            df_dan=pd.read_csv(path,encoding="gbk",engine='python')
        #汇总数据    
        df1=df_zu[df_zu["交易日期"]==riqi].reset_index(drop=True)
        df2=df_dan[df_dan["交易日期"]==riqi].reset_index(drop=True)
        summary_zu=pd.concat([summary_zu,df1],axis=0)
        summary_dan=pd.concat([summary_dan,df2],axis=0)
    #获取今天的客户名单
    daydf=df[df["交易日期"]==today].reset_index(drop=True)
    khdaylist=daydf.drop_duplicates(["资金账号"],keep="first")["资金账号"].values.tolist()
    #获取最近两周有交易记录的客户名单
    khalllist= summary_zu.drop_duplicates(["资金账号"],keep="first")["资金账号"].values.tolist()
    print(len(khdaylist),len(khalllist))
    #今天没有交易的客户名单
    plist=[]
    for i in range(len(khalllist)):
        if khalllist[i] in khdaylist:
            continue
        else:            
            plist.append(khalllist[i])      
    print(plist)
    #新建dataframe 储存潜在客户信息
    infodf=pd.DataFrame()
    #index = ["资金账号", "营业部", "账户名", "交易日期", "股票代码", "股票名称", "市值", "当天交易额", "换手率%", "当天收益", "收益率%", "累计收益", "佣金", "平仓", "亏损天数", "胜率",
         #"佣金率","估算占用资金","现金收益率%","现金七日年化收益率%","连续盈利天数"]  
    index=["资金账号","账户名","营业部","近期最大市值","当日日期","今天日期","今日市值","差额","状态"]
    for i in range(len(khalllist)):
        zjcode=khalllist[i]
        khdf=summary_zu[summary_zu["资金账号"]==zjcode].reset_index(drop=True)
        khdf.sort_values(by=["交易日期"],ascending=True)
        khdict={}
        khdict["资金账号"]=zjcode
        khdict["账户名"]=khdf["账户名"][0]
        khdict["营业部"]=khdf["营业部"][0]
        khdict["近期最大市值"]=max(khdf["市值"].values)
        khdict["当日日期"]=khdf[khdf["市值"]==khdict["近期最大市值"]].reset_index(drop=True)["交易日期"][0]
        if zjcode in plist: 
            khdict["今日市值"]=0
        else:
            khdict["今日市值"]=khdf["市值"][len(khdf)-1]    
        khdict["今天日期"]=khdf["交易日期"][len(khdf)-1]
        khdict["差额"]=khdict["近期最大市值"]-khdict["今日市值"]
        if zjcode in plist:
            khdict["状态"]="今天未交易"
        else:
            khdict["状态"]="交易进行中"
            
        if khdict["今日市值"]!=0 and khdict["差额"]>100000:
            aRecordDf = pd.DataFrame(khdict, index=[0])
            infodf = pd.concat([infodf, aRecordDf], axis=0)
        elif khdict["状态"]=="今天未交易" and khdict["近期最大市值"]>200000:
            aRecordDf = pd.DataFrame(khdict, index=[0])
            infodf = pd.concat([infodf, aRecordDf], axis=0)                
    path= rootPath + "/" + "客户市值缺失统计表.xlsx"
    infodf.to_excel(path,encoding="gbk",columns=index)
    print("over!")
    
def addjiaoyijilu(aCusTotleDF,account,newrecoraddf):
    if account==105028828:
        Dict1={}        
        Dict1["日期"]="2019-04-24"
        Dict1["时间"]="10:30:13"
        Dict1["委托号"]="0001"
        Dict1["代码"]="002093"
        Dict1["名称"]="国脉科技"
        Dict1["方向"]="买"
        Dict1["委托类型"]="限价单"
        Dict1["委托数量"]=3300
        Dict1["委托价格"]=11.21
        Dict1["成交数量"]=3300
        Dict1["成交金额"]=Dict1["委托价格"]* Dict1["成交数量"]
        Dict1["撤单数量"]=0
        Dict1["委托状态"]="成交"
        Dict1["手续费"]=Dict1["成交金额"]*0.00035
        aRecordDf = pd.DataFrame(Dict1, index=[0])
        newrecoraddf=pd.concat([newrecoraddf, aRecordDf], axis=0)
        aCusTotleDF = pd.concat([aCusTotleDF, aRecordDf], axis=0)
        aCusTotleDF=aCusTotleDF.reset_index(drop=True)
        print("-------------------------------------",account)
        Dict1={}        
        Dict1["日期"]="2019-04-26"
        Dict1["时间"]="9:31:13"
        Dict1["委托号"]="0002"
        Dict1["代码"]="600831"
        stockCode=normalize_code(Dict1["代码"])
        Dict1["名称"]=get_security_info(stockCode).display_name
        Dict1["方向"]="买"
        Dict1["委托类型"]="限价单"
        Dict1["委托数量"]=4000
        Dict1["委托价格"]=12.61
        Dict1["成交数量"]=4000
        Dict1["成交金额"]=Dict1["委托价格"]* Dict1["成交数量"]
        Dict1["撤单数量"]=0
        Dict1["委托状态"]="成交"
        Dict1["手续费"]=Dict1["成交金额"]*0.00035
        aRecordDf = pd.DataFrame(Dict1, index=[0])
        newrecoraddf=pd.concat([newrecoraddf, aRecordDf], axis=0)
        aCusTotleDF = pd.concat([aCusTotleDF, aRecordDf], axis=0)
        aCusTotleDF=aCusTotleDF.reset_index(drop=True)
        print("-------------------------------------",account)
        Dict1={}        
        Dict1["日期"]="2019-04-26"
        Dict1["时间"]="10:11:13"
        Dict1["委托号"]="0003"
        Dict1["代码"]="600831"
        stockCode=normalize_code(Dict1["代码"])
        Dict1["名称"]=get_security_info(stockCode).display_name
        Dict1["方向"]="卖"
        Dict1["委托类型"]="限价单"
        Dict1["委托数量"]=4000
        Dict1["委托价格"]=13.2
        Dict1["成交数量"]=4000
        Dict1["成交金额"]=Dict1["委托价格"]* Dict1["成交数量"]
        Dict1["撤单数量"]=0
        Dict1["委托状态"]="成交"
        Dict1["手续费"]=Dict1["成交金额"]*0.00035+Dict1["成交金额"]*0.001
        aRecordDf = pd.DataFrame(Dict1, index=[0])
        newrecoraddf=pd.concat([newrecoraddf, aRecordDf], axis=0)
        aCusTotleDF = pd.concat([aCusTotleDF, aRecordDf], axis=0)
        aCusTotleDF=aCusTotleDF.reset_index(drop=True)
        print("-------------------------------------",account)
        Dict1={}        
        Dict1["日期"]="2019-04-26"
        Dict1["时间"]="9:55:13"
        Dict1["委托号"]="0004"
        Dict1["代码"]="002733"
        stockCode=normalize_code(Dict1["代码"])
        Dict1["名称"]=get_security_info(stockCode).display_name
        Dict1["方向"]="卖"
        Dict1["委托类型"]="限价单"
        Dict1["委托数量"]=4000
        Dict1["委托价格"]=30
        Dict1["成交数量"]=4000
        Dict1["成交金额"]=Dict1["委托价格"]* Dict1["成交数量"]
        Dict1["撤单数量"]=0
        Dict1["委托状态"]="成交"
        Dict1["手续费"]=Dict1["成交金额"]*0.00035+Dict1["成交金额"]*0.001
        aRecordDf = pd.DataFrame(Dict1, index=[0])
        newrecoraddf=pd.concat([newrecoraddf, aRecordDf], axis=0)
        aCusTotleDF = pd.concat([aCusTotleDF, aRecordDf], axis=0)
        aCusTotleDF=aCusTotleDF.reset_index(drop=True)
        print("-------------------------------------",account)
        Dict1={}        
        Dict1["日期"]="2019-04-26"
        Dict1["时间"]="10:16:13"
        Dict1["委托号"]="0005"
        Dict1["代码"]="002565"
        stockCode=normalize_code(Dict1["代码"])
        Dict1["名称"]=get_security_info(stockCode).display_name
        Dict1["方向"]="买"
        Dict1["委托类型"]="限价单"
        Dict1["委托数量"]=4000
        Dict1["委托价格"]=18.91
        Dict1["成交数量"]=4000
        Dict1["成交金额"]=Dict1["委托价格"]* Dict1["成交数量"]
        Dict1["撤单数量"]=0
        Dict1["委托状态"]="成交"
        Dict1["手续费"]=Dict1["成交金额"]*0.00035+Dict1["成交金额"]*0.001
        aRecordDf = pd.DataFrame(Dict1, index=[0])
        newrecoraddf=pd.concat([newrecoraddf, aRecordDf], axis=0)
        aCusTotleDF = pd.concat([aCusTotleDF, aRecordDf], axis=0)
        aCusTotleDF=aCusTotleDF.reset_index(drop=True)
        print("-------------------------------------",account)        
    elif account==156062015:
        Dict1={}        
        Dict1["日期"]="2019-04-26"
        Dict1["时间"]="10:30:13"
        Dict1["委托号"]="0001"
        Dict1["代码"]="300666"
        stockCode=normalize_code(Dict1["代码"])
        Dict1["名称"]=get_security_info(stockCode).display_name
        Dict1["方向"]="卖"
        Dict1["委托类型"]="限价单"
        Dict1["委托数量"]=1900
        Dict1["委托价格"]=41.7
        Dict1["成交数量"]=1900
        Dict1["成交金额"]=Dict1["委托价格"]* Dict1["成交数量"]
        Dict1["撤单数量"]=0
        Dict1["委托状态"]="成交"
        Dict1["手续费"]=Dict1["成交金额"]*0.00035+Dict1["成交金额"]*0.001
        aRecordDf = pd.DataFrame(Dict1, index=[0])
        newrecoraddf=pd.concat([newrecoraddf, aRecordDf], axis=0)
        aCusTotleDF = pd.concat([aCusTotleDF, aRecordDf], axis=0)
        aCusTotleDF=aCusTotleDF.reset_index(drop=True)
        print("-------------------------------------",account)
    elif account==105018129:
        Dict1={}        
        Dict1["日期"]="2019-04-26"
        Dict1["时间"]="10:30:13"
        Dict1["委托号"]="0001"
        Dict1["代码"]="600487"
        stockCode=normalize_code(Dict1["代码"])
        Dict1["名称"]=get_security_info(stockCode).display_name
        Dict1["方向"]="卖"
        Dict1["委托类型"]="限价单"
        Dict1["委托数量"]=1100
        Dict1["委托价格"]=21.64
        Dict1["成交数量"]=1100
        Dict1["成交金额"]=Dict1["委托价格"]* Dict1["成交数量"]
        Dict1["撤单数量"]=0
        Dict1["委托状态"]="成交"
        Dict1["手续费"]=Dict1["成交金额"]*0.00035+Dict1["成交金额"]*0.001
        aRecordDf = pd.DataFrame(Dict1, index=[0])
        newrecoraddf=pd.concat([newrecoraddf, aRecordDf], axis=0)
        aCusTotleDF = pd.concat([aCusTotleDF, aRecordDf], axis=0)
        aCusTotleDF=aCusTotleDF.reset_index(drop=True)
        print("-------------------------------------",account)
        Dict1={}        
        Dict1["日期"]="2019-04-26"
        Dict1["时间"]="10:30:13"
        Dict1["委托号"]="0002"
        Dict1["代码"]="002384"
        stockCode=normalize_code(Dict1["代码"])
        Dict1["名称"]=get_security_info(stockCode).display_name
        Dict1["方向"]="卖"
        Dict1["委托类型"]="限价单"
        Dict1["委托数量"]=1300
        Dict1["委托价格"]=17.97
        Dict1["成交数量"]=1300
        Dict1["成交金额"]=Dict1["委托价格"]* Dict1["成交数量"]
        Dict1["撤单数量"]=0
        Dict1["委托状态"]="成交"
        Dict1["手续费"]=Dict1["成交金额"]*0.00035+Dict1["成交金额"]*0.001
        aRecordDf = pd.DataFrame(Dict1, index=[0])
        newrecoraddf=pd.concat([newrecoraddf, aRecordDf], axis=0)
        aCusTotleDF = pd.concat([aCusTotleDF, aRecordDf], axis=0)
        aCusTotleDF=aCusTotleDF.reset_index(drop=True)
        print("-------------------------------------",account)
    elif account==108022585:
        Dict1={}        
        Dict1["日期"]="2019-04-26"
        Dict1["时间"]="10:30:13"
        Dict1["委托号"]="0001"
        Dict1["代码"]="601860"
        stockCode=normalize_code(Dict1["代码"])
        Dict1["名称"]=get_security_info(stockCode).display_name
        Dict1["方向"]="买"
        Dict1["委托类型"]="限价单"
        Dict1["委托数量"]=3800
        Dict1["委托价格"]=8.18
        Dict1["成交数量"]=3800
        Dict1["成交金额"]=Dict1["委托价格"]* Dict1["成交数量"]
        Dict1["撤单数量"]=0
        Dict1["委托状态"]="成交"
        Dict1["手续费"]=Dict1["成交金额"]*0.00035
        aRecordDf = pd.DataFrame(Dict1, index=[0])
        newrecoraddf=pd.concat([newrecoraddf, aRecordDf], axis=0)
        aCusTotleDF = pd.concat([aCusTotleDF, aRecordDf], axis=0)
        aCusTotleDF=aCusTotleDF.reset_index(drop=True)
        print("-------------------------------------",account) 
    elif account==119013885:
        Dict1={}        
        Dict1["日期"]="2019-04-26"
        Dict1["时间"]="10:30:13"
        Dict1["委托号"]="0001"
        Dict1["代码"]="300398"
        stockCode=normalize_code(Dict1["代码"])
        Dict1["名称"]=get_security_info(stockCode).display_name
        Dict1["方向"]="买"
        Dict1["委托类型"]="限价单"
        Dict1["委托数量"]=3800
        Dict1["委托价格"]=17.04
        Dict1["成交数量"]=3800
        Dict1["成交金额"]=Dict1["委托价格"]* Dict1["成交数量"]
        Dict1["撤单数量"]=0
        Dict1["委托状态"]="成交"
        Dict1["手续费"]=Dict1["成交金额"]*0.00035
        aRecordDf = pd.DataFrame(Dict1, index=[0])
        newrecoraddf=pd.concat([newrecoraddf, aRecordDf], axis=0)
        aCusTotleDF = pd.concat([aCusTotleDF, aRecordDf], axis=0)
        aCusTotleDF=aCusTotleDF.reset_index(drop=True)
        print("-------------------------------------",account) 
        Dict1={}        
        Dict1["日期"]="2019-04-26"
        Dict1["时间"]="10:30:13"
        Dict1["委托号"]="0002"
        Dict1["代码"]="300296"
        stockCode=normalize_code(Dict1["代码"])
        Dict1["名称"]=get_security_info(stockCode).display_name
        Dict1["方向"]="买"
        Dict1["委托类型"]="限价单"
        Dict1["委托数量"]=6000
        Dict1["委托价格"]=8.74
        Dict1["成交数量"]=6000
        Dict1["成交金额"]=Dict1["委托价格"]* Dict1["成交数量"]
        Dict1["撤单数量"]=0
        Dict1["委托状态"]="成交"
        Dict1["手续费"]=Dict1["成交金额"]*0.00035
        aRecordDf = pd.DataFrame(Dict1, index=[0])
        newrecoraddf=pd.concat([newrecoraddf, aRecordDf], axis=0)
        aCusTotleDF = pd.concat([aCusTotleDF, aRecordDf], axis=0)
        aCusTotleDF=aCusTotleDF.reset_index(drop=True)
        print("-------------------------------------",account) 
        Dict1={}        
        Dict1["日期"]="2019-04-26"
        Dict1["时间"]="10:30:13"
        Dict1["委托号"]="0003"
        Dict1["代码"]="000413"
        stockCode=normalize_code(Dict1["代码"])
        Dict1["名称"]=get_security_info(stockCode).display_name
        Dict1["方向"]="卖"
        Dict1["委托类型"]="限价单"
        Dict1["委托数量"]=3300
        Dict1["委托价格"]=6.51
        Dict1["成交数量"]=3300
        Dict1["成交金额"]=Dict1["委托价格"]* Dict1["成交数量"]
        Dict1["撤单数量"]=0
        Dict1["委托状态"]="成交"
        Dict1["手续费"]=Dict1["成交金额"]*0.00035+Dict1["成交金额"]*0.001
        aRecordDf = pd.DataFrame(Dict1, index=[0])
        newrecoraddf=pd.concat([newrecoraddf, aRecordDf], axis=0)
        aCusTotleDF = pd.concat([aCusTotleDF, aRecordDf], axis=0)
        aCusTotleDF=aCusTotleDF.reset_index(drop=True)
        print("-------------------------------------",account) 
    elif account==119010490:
        Dict1={}        
        Dict1["日期"]="2019-04-26"
        Dict1["时间"]="10:30:13"
        Dict1["委托号"]="0001"
        Dict1["代码"]="000735"
        stockCode=normalize_code(Dict1["代码"])
        Dict1["名称"]=get_security_info(stockCode).display_name
        Dict1["方向"]="买"
        Dict1["委托类型"]="限价单"
        Dict1["委托数量"]=2500
        Dict1["委托价格"]=11.68
        Dict1["成交数量"]=2500
        Dict1["成交金额"]=Dict1["委托价格"]* Dict1["成交数量"]
        Dict1["撤单数量"]=0
        Dict1["委托状态"]="成交"
        Dict1["手续费"]=Dict1["成交金额"]*0.00035
        aRecordDf = pd.DataFrame(Dict1, index=[0])
        newrecoraddf=pd.concat([newrecoraddf, aRecordDf], axis=0)
        aCusTotleDF = pd.concat([aCusTotleDF, aRecordDf], axis=0)
        aCusTotleDF=aCusTotleDF.reset_index(drop=True)
        print("-------------------------------------",account)     
    elif account==119000142:
        Dict1={}        
        Dict1["日期"]="2019-04-26"
        Dict1["时间"]="10:30:13"
        Dict1["委托号"]="0001"
        Dict1["代码"]="600651"
        stockCode=normalize_code(Dict1["代码"])
        Dict1["名称"]=get_security_info(stockCode).display_name
        Dict1["方向"]="卖"
        Dict1["委托类型"]="限价单"
        Dict1["委托数量"]=7500
        Dict1["委托价格"]=4.66
        Dict1["成交数量"]=7500
        Dict1["成交金额"]=Dict1["委托价格"]* Dict1["成交数量"]
        Dict1["撤单数量"]=0
        Dict1["委托状态"]="成交"
        Dict1["手续费"]=Dict1["成交金额"]*0.00035+Dict1["成交金额"]*0.001
        aRecordDf = pd.DataFrame(Dict1, index=[0])
        newrecoraddf=pd.concat([newrecoraddf, aRecordDf], axis=0)
        aCusTotleDF = pd.concat([aCusTotleDF, aRecordDf], axis=0)
        aCusTotleDF=aCusTotleDF.reset_index(drop=True)
        print("-------------------------------------",account) 
        
        
    elif account==300106528:
        Dict1={}        
        Dict1["日期"]="2019-04-26"
        Dict1["时间"]="10:30:13"
        Dict1["委托号"]="0001"
        Dict1["代码"]="002909"
        stockCode=normalize_code(Dict1["代码"])
        Dict1["名称"]=get_security_info(stockCode).display_name
        Dict1["方向"]="卖"
        Dict1["委托类型"]="限价单"
        Dict1["委托数量"]=2500
        Dict1["委托价格"]=10.8
        Dict1["成交数量"]=2500
        Dict1["成交金额"]=Dict1["委托价格"]* Dict1["成交数量"]
        Dict1["撤单数量"]=0
        Dict1["委托状态"]="成交"
        Dict1["手续费"]=Dict1["成交金额"]*0.00035+Dict1["成交金额"]*0.001
        aRecordDf = pd.DataFrame(Dict1, index=[0])
        newrecoraddf=pd.concat([newrecoraddf, aRecordDf], axis=0)
        aCusTotleDF = pd.concat([aCusTotleDF, aRecordDf], axis=0)
        aCusTotleDF=aCusTotleDF.reset_index(drop=True)
        print("-------------------------------------",account)     
        
    elif account==300120061:
        Dict1={}        
        Dict1["日期"]="2019-04-30"
        Dict1["时间"]="10:30:13"
        Dict1["委托号"]="0001"
        Dict1["代码"]="002692"
        stockCode=normalize_code(Dict1["代码"])
        Dict1["名称"]=get_security_info(stockCode).display_name
        Dict1["方向"]="买"
        Dict1["委托类型"]="限价单"
        Dict1["委托数量"]=11300
        Dict1["委托价格"]=4.23
        Dict1["成交数量"]=11300
        Dict1["成交金额"]=Dict1["委托价格"]* Dict1["成交数量"]
        Dict1["撤单数量"]=0
        Dict1["委托状态"]="成交"
        Dict1["手续费"]=Dict1["成交金额"]*0.00035
        aRecordDf = pd.DataFrame(Dict1, index=[0])
        newrecoraddf=pd.concat([newrecoraddf, aRecordDf], axis=0)
        aCusTotleDF = pd.concat([aCusTotleDF, aRecordDf], axis=0)
        aCusTotleDF=aCusTotleDF.reset_index(drop=True)
        print("-------------------------------------",account)     
        
    elif account==105011438:
        Dict1={}        
        Dict1["日期"]="2019-04-26"
        Dict1["时间"]="10:30:13"
        Dict1["委托号"]="0001"
        Dict1["代码"]="002611"
        stockCode=normalize_code(Dict1["代码"])
        Dict1["名称"]=get_security_info(stockCode).display_name
        Dict1["方向"]="卖"
        Dict1["委托类型"]="限价单"
        Dict1["委托数量"]=5000
        Dict1["委托价格"]=6.04
        Dict1["成交数量"]=5000
        Dict1["成交金额"]=Dict1["委托价格"]* Dict1["成交数量"]
        Dict1["撤单数量"]=0
        Dict1["委托状态"]="成交"
        Dict1["手续费"]=Dict1["成交金额"]*0.00035+Dict1["成交金额"]*0.001
        aRecordDf = pd.DataFrame(Dict1, index=[0])
        newrecoraddf=pd.concat([newrecoraddf, aRecordDf], axis=0)
        aCusTotleDF = pd.concat([aCusTotleDF, aRecordDf], axis=0)
        aCusTotleDF=aCusTotleDF.reset_index(drop=True)
        print("-------------------------------------",account)     
    elif account==300123937:
        Dict1={}        
        Dict1["日期"]="2019-04-26"
        Dict1["时间"]="10:30:13"
        Dict1["委托号"]="0001"
        Dict1["代码"]="000070"
        stockCode=normalize_code(Dict1["代码"])
        Dict1["名称"]=get_security_info(stockCode).display_name
        Dict1["方向"]="买"
        Dict1["委托类型"]="限价单"
        Dict1["委托数量"]=1500
        Dict1["委托价格"]=16.11
        Dict1["成交数量"]=1500
        Dict1["成交金额"]=Dict1["委托价格"]* Dict1["成交数量"]
        Dict1["撤单数量"]=0
        Dict1["委托状态"]="成交"
        Dict1["手续费"]=Dict1["成交金额"]*0.00035
        aRecordDf = pd.DataFrame(Dict1, index=[0])
        newrecoraddf=pd.concat([newrecoraddf, aRecordDf], axis=0)
        aCusTotleDF = pd.concat([aCusTotleDF, aRecordDf], axis=0)
        aCusTotleDF=aCusTotleDF.reset_index(drop=True)
        print("-------------------------------------",account)     
    return aCusTotleDF,newrecoraddf



