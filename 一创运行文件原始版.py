#============================导入需要用到的python模块儿===============================
from AlphaTCom import Alpha_time as ATime
from AlphaTCom import fileProcess as ATFile
from AlphaTExcelLib import AlphaTExcel as ATExcel
from AlphaTExcelLib1 import AlphaTExcel as ATExcel1


import os,shutil,re
import time
import datetime
import numpy as np
import pandas as pd
from numpy import NaN

import jqdatasdk as jq
jq.auth('13581880168','www.dulun.205')


AFileTool = ATFile()
ATimeTool = ATime()

#=============================定义处理过程需要的函数==================================
def find_files(list_files,string):
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
#输入交易记录的path，输出一个以所有资金账号为key、文件名为value的dict=
    fileList = find_files(os.listdir(path),r'^(\d{11})-')
    fileNameDict = {}
    for each in fileList:
        a = int(each.split('-')[-1][:-4])
        fileNameDict[a] = each
    fileNameList = fileNameDict.keys()
    return fileNameDict

def changedate(day):
#将日期转为整数格式，方便对比日期大小。

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

#输入历史收益计算结果所存储的路径，返回一个表格记录历史每日的单票收益。
def getolddate(rootpath):
    #遍历已有的收益计算结果文件
    yuanpath= rootpath + "/" + "收益计算结果"
    filelist = find_files(os.listdir(yuanpath),r'^(\d{4})-(\d{2})-(\d{2})$')
    print(filelist)
   
    #建立新的dataframe，用以储存所以的数据
    summary_dan=pd.DataFrame()
    #按天获取组合汇总收益数据 
    #summary=pd.DataFrame()
    for riqi in filelist:
        print("开始汇总%s的数据"%(riqi),riqi)
        #读取当天数据
        if changedate(riqi)>changedate("2019-08-06"):
            index = ["资金账号", "营业部", "账户名", "交易日期", "股票代码", "股票名称", "市值","交易市值","当天交易额", "当天收益", "收益率%", "累计收益","用户授权资金"]
            path= yuanpath +"/"+riqi+"/"+"汇总数据"+"/"+riqi+"_单票收益记录.xlsx"
            df_dan=pd.read_excel(path,encoding="gbk")[index]
        else:
            if changedate(riqi)>changedate("2019-06-20"):
                index = ["资金账号", "营业部", "账户名", "交易日期", "股票代码", "股票名称", "市值","交易市值","当天交易额", "当天收益", "收益率%", "累计收益","估算占用资金"]
                path= yuanpath +"/"+riqi+"/"+"汇总数据"+"/"+riqi+"_单票收益记录.xlsx"
                df_dan=pd.read_excel(path,encoding="gbk")[index]
                df_dan.rename(columns = {'估算占用资金':'用户授权资金'},inplace = True)
            else:
                if changedate(riqi)>changedate("2019-04-22"):
                    index = ["资金账号", "营业部", "账户名", "交易日期", "股票代码", "股票名称", "市值","当天交易额", "当天收益", "收益率%", "累计收益","估算占用资金"]
                    path= yuanpath +"/"+riqi+"/"+"汇总数据"+"/"+riqi+"_单票收益记录.xlsx"
                    df_dan=pd.read_excel(path,encoding="gbk")[index]
                    df_dan.rename(columns = {'估算占用资金':'用户授权资金'},inplace = True)
                else:
                    if changedate(riqi)>changedate("2019-04-17"):
                        index = ["资金账号", "营业部", "账户名", "交易日期", "股票代码", "股票名称", "市值","当天交易额", "当天收益", "收益率%", "累计收益","使用资金"]
                        if riqi=="2019-04-19":
                            path= yuanpath +"/"+riqi+"/"+"汇总数据"+"/"+riqi+"_单票收益记录.csv"
                            df_dan=pd.read_csv(path,encoding="utf-8",engine="python")[index]
                        elif changedate(riqi)>changedate("2019-04-19"):
                            path= yuanpath +"/"+riqi+"/"+"汇总数据"+"/"+riqi+"_单票收益记录.xlsx"
                            df_dan=pd.read_excel(path,encoding="gbk")[index]
                        else:
                            path= yuanpath +"/"+riqi+"/"+"汇总数据"+"/"+riqi+"_单票收益记录.csv"
                            df_dan=pd.read_csv(path,encoding="gbk",engine='python')[index]
                        df_dan.rename(columns = {'使用资金':'用户授权资金'},inplace = True)
                        
                    else:
                        index = ["资金账号", "营业部", "账户名", "交易日期", "股票代码", "股票名称", "市值","当天交易额", "当天收益", "收益率%", "累计收益"]
                        path= yuanpath +"/"+riqi+"/"+"汇总数据"+"/"+riqi+"_单票收益记录.csv"
                        df_dan=pd.read_csv(path,encoding="gbk",engine='python')[index]
                        index.insert(11,'用户授权资金')
                        df_dan = df_dan.reindex(columns = index)
                index.insert(7,'交易市值')#即使6月20日之前没有交易市值这一栏，我们也要加上保持一致性，可以让之前的交易市值为空，毕竟没有记录嘛
                df_dan = df_dan.reindex(columns = index)          
        df_dan_now=df_dan[df_dan["交易日期"]==riqi].reset_index(drop=True)
        summary_dan=pd.concat([summary_dan,df_dan_now],axis=0)
    summary_dan=summary_dan.reset_index(drop=True)
    return summary_dan

#下面这两个这个函数用来移动交割记录的，将各个营业部的交割记录从交易记录的文件夹里移出，然后放到各个营业部的文件夹里。
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
            
#从组合收益表里面提取所有的营业部名称，放在一个list里面输出
def getYYBNameList(data_zu):
    outDF = data_zu.drop_duplicates(["营业部"], keep="first")
    return outDF["营业部"].tolist()


#获取某个营业部中所有客户的账户名，放到一个list里面。
def getYYBcustomerList(data_zu, YYB):
    tempDF = data_zu[data_zu['营业部'] == YYB]
    outDF = tempDF.drop_duplicates(["账户名"], keep="first")
    return outDF["账户名"].tolist()

#该函数就是像linux的同名函数一样，用来创建路径的。
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

#这个是处理各个营业部中的所有用户的交割记录的
def main(data_zu,jiaoyipath,rootpath):
    print("开始处理交割记录")
    Path1 = jiaoyipath
    oldfilelist=find_files(os.listdir(Path1),r'^(\d{11})-')
    today = time.strftime('%Y-%m-%d', time.localtime(time.time()))
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
    
#这个函数是从总的单票收益表格中，抠出来每个营业部中相应客户的单票记录，然后存入各个时期营业部文件夹里。
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

#这个函数是生成汇总数据文件夹里面的汇总数据表格的。
def finalmain(dan_path,zu_path,jiaoyipath,rootpath):
    official_path = rootpath + "/" + "一创聚宽（升级版）正式使用登记表.xlsx"
    rootpath = rootpath + "/" + "收益计算结果"  
    data_zu=pd.read_excel(zu_path, encoding='utf-8')
    data_dan=pd.read_excel(dan_path,encoding='utf-8')
    
    #导入正式使用用户表格的信息
    official_users = pd.read_excel(official_path,encoding ='utf-8')
    official_users['申请时间'] = official_users['申请时间'].apply(lambda x: changedate(datetime.datetime.strftime(x,'%Y-%m-%d')))
    
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
    datelist=datelist[60:]
    daylist=datelist
    #没有当天市值的日期
    print(len(daylist))
    print("日期列表：",daylist)
    #按天获取组合汇总收益数据 
    #获取交易额排名前20的用户
    summary=pd.DataFrame()
    for i in range(len(daylist)):
        print("开始汇总%s的数据"%(daylist[i]))
        riqi=datelist[i]
        if riqi == '2019-07-13':
            continue
        #读取当天数据
        if riqi=="2019-04-19" :
            path= rootpath +"/"+riqi+"/"+"汇总数据"+"/"+riqi+"_组合收益记录.csv"
            df_zu=pd.read_csv(path,encoding="utf-8",engine="python")
        elif changedate(riqi)>changedate("2019-04-20"):
            print("riqi:",riqi)
            path= rootpath +"/"+riqi+"/"+"汇总数据"+"/"+riqi+"_组合收益记录.xlsx"
            df_zu=pd.read_excel(path,encoding="utf-8")
        else: 
            path= rootpath +"/"+riqi+"/"+"汇总数据"+"/"+riqi+"_组合收益记录.csv"
            df_zu=pd.read_csv(path,encoding="gbk",engine='python')
           
        df1=df_zu[df_zu["交易日期"]==riqi].reset_index(drop=True)
        dict1={}
        dict1["交易日期"]=riqi
        dict1["当天市值"]=df1["市值"].sum()
        dict1["当天交易额"]=int(df1["当天交易额"].sum())
        
        #根据正式用户使用表计算正式用户交易额
        users_up_to_date = official_users[official_users['申请时间'] <= changedate(riqi)]
        customer_lists = users_up_to_date['资金账号'].unique()
        df2 = df1[df1['资金账号'].apply(lambda x: x in customer_lists)]
        dict1['正式账号当天交易额'] = int(df2['当天交易额'].sum())

        dict1["当天收益"]=int(df1["当天收益"].sum())
        #根据组合数据计算汇总数据的交易额加权换手率
        if (not df1["当天交易额"].isnull().all()) and (not df1["换手率%"].isnull().all()):
            trading_values = list(df1["当天交易额"].values)
            turnover_rates = list(df1["换手率%"].values)
            nrow = len(df1)
            sum_product = 0
            for k in range(nrow):
                if (not pd.isnull(trading_values[k])) and (not (pd.isnull(turnover_rates[k]))):
                    product = trading_values[k] * turnover_rates[k]
                    sum_product = sum_product + product
            dict1["交易额加权换手率%"]  = get2float(sum_product/dict1["当天交易额"]) 
        else:
            dict1["交易额加权换手率%"] = NaN
        #根据组合数据计算汇总数据的交易额加权收益率
        if (not df1["当天交易额"].isnull().all()) and (not df1["收益率%"].isnull().all()):
            trading_values = list(df1["当天交易额"].values)
            return_rates = list(df1["收益率%"].values)
            nrow = len(df1)
            sum_product = 0
            for a in range(nrow):
                if (not pd.isnull(trading_values[a])) and (not (pd.isnull(return_rates[a]))):
                    product = trading_values[a] * return_rates[a]
                    sum_product = sum_product + product
            dict1["交易额加权收益率%"] = sum_product/dict1["当天交易额"]
        else:
            dict1["交易额加权收益率%"] = NaN
            
        #计算中位数收益率
        if not df1['收益率%'].isnull().all():
            returns = df1['收益率%'].values
            returns = [i for i in returns if not pd.isnull(i)]
            median_return = np.median(returns)
            dict1["中位数收益率%"] = median_return
        else:
            dict1["中位数收益率%"] = NaN
            
        #计算等权收益率
        if not df1['收益率%'].isnull().all():
            returns = df1['收益率%'].values
            returns = [i for i in returns if not pd.isnull(i)]
            mean_return = np.mean(returns)
            dict1["等权平均收益率%"] = mean_return
        else:
            dict1["等权平均收益率%"] = NaN 
            
        #提取上证指数每日交易额
        date = datetime.datetime.strptime(riqi,'%Y-%m-%d')
        year = date.year
        month = date.month
        day = date.day
        df_XSHG = jq.get_price('000001.XSHG', count =240,end_date = datetime.datetime(year,month,day,15,0,0),frequency = '1m',fq = None)
        trading_money = int(df_XSHG['money'].sum()/100000000)
        dict1['上证指数当日交易额'] = trading_money
        
        aRecordDf = pd.DataFrame(dict1, index=[0])
        summary = pd.concat([summary, aRecordDf], axis=0).reset_index(drop =True)
    path= rootpath +"/"+today+"/"+"汇总数据"+"/"+today+"_汇总数据.xlsx"
    summary.to_excel(path,encoding = 'utf-8')
    
#定义查找潜在客户名单 （现在我们一般不跑这个功能了，该函数跑的话会出bug，因为一直没有检查运作）   
def findpotentialclient(rootpath):                      
    today=datetime.datetime.strftime(datetime.date.today(),'%Y-%m-%d')
    path= rootpath + "/" + "收益计算结果"+"/"+today+"/"+"汇总数据"+"/"+today+"_组合收益记录.xlsx"
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
            path= rootpath + "/" + "收益计算结果"+"/"+riqi+"/"+"汇总数据"+"/"+riqi+"_组合收益记录.csv"
            df_zu=pd.read_csv(path,encoding="utf-8",engine="python")
            path= rootpath + "/" + "收益计算结果" +"/"+riqi+"/"+"汇总数据"+"/"+riqi+"_单票收益记录.csv"
            df_dan=pd.read_csv(path,encoding="utf-8",engine="python")
        elif changedate(riqi)>changedate("2019-04-20"):
            path= rootpath + "/" + "收益计算结果" +"/"+riqi+"/"+"汇总数据"+"/"+riqi+"_组合收益记录.xlsx"
            df_zu=pd.read_excel(path,encoding="utf-8")
            path= rootpath + "/" + "收益计算结果" +"/"+riqi+"/"+"汇总数据"+"/"+riqi+"_单票收益记录.xlsx"
            df_dan=pd.read_excel(path,encoding="utf-8")
        else:
            path= rootpath + "/" + "收益计算结果" +"/"+riqi+"/"+"汇总数据"+"/"+riqi+"_组合收益记录.csv"
            df_zu=pd.read_csv(path,encoding="gbk",engine='python')
            path= rootpath + "/" + "收益计算结果" +"/"+riqi+"/"+"汇总数据"+"/"+riqi+"_单票收益记录.csv"
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
    path= rootpath + "/" + "客户市值缺失统计表.xlsx"
    infodf.to_excel(path,encoding="gbk",columns=index)
    print("over!")

#把历史手动调仓的记录增加到某个用户相应的交易记录表格里面。
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

#从资金账号推断出该用户是属于哪个营业部的。
def getYYBName(account):
    if account==156062015:
        department="深圳笋岗东路营业部"
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

# 从当日用户信息表里，抠出某一个用户所有交易的股票的相关信息，返回一个字典，key是股票代码，value还是一个字典，
#存储着“目标股数”，“目标市值”，“交易市值”，“用户授权资金”，“股票名称”等等信息。
def dealACumRecorder(tagDF):
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

#这个是针对于那些不在当日用户信息表里的用户或股票的。我们无法像上面那个函数一样获取相关信息（交易市值、目标市值等），于是我们返回
#一个空的字典，但是该有的形式还是得有，该有的字段还是得有，格式要统一。
def getstocklist(account):
    filemame=listFilename(jiaoyipath)[account]
    #通过资金账户返回交易记录文件名
    file=pd.read_csv(jiaoyipath + "/"+filemame,encoding="gbk",engine="python")
    file['代码'] = file['代码'].map(lambda x: normalize_code(x.lstrip("'")))
    stockCodeList=file.drop_duplicates(["代码"],keep="first")["代码"].values
    outStockCodeDict = {}
    for each in stockCodeList:
        tempDict = {}
        tempDict['股票名称'] = get_security_info(each).display_name
        tempDict['目标市值'] = NaN
        tempDict['交易市值'] = NaN
        tempDict['用户授权资金'] = NaN
        tempDict['状态'] = NaN
        outStockCodeDict[each] = tempDict
    #获取股票列表
    return outStockCodeDict

#上面两个函数都是服务这个函数的。这个函数根据“交易记录文件夹”里面所有有交易记录的用户，给他们分别提取他们的单票的信息（比如交易市值啊等）
#结果返回一个字典，字典的key是所有有交易记录的用户的资金账号，value是该用户的信息，也是一个字典的格式。用户信息中就包括了单票信息。
#最骚的就是，有具体股票信息的，只有当天在用户信息表里面出现过的用户，而且出现的信息也仅仅是他们正在做的票的信息，他们历史上做的票的信息不会
#出现。其他今天没有做交易的用户，他们的股票信息都是空的。
def dealBookDate(df):
    cusDict = {}
    # 获取交易记录里的资金账号列表    
    fileList = find_files(os.listdir(jiaoyipath),r'^(\d{11})-')
    List = [] 
    List1 = []
    for each in fileList:
        a = int(each.split('-')[-1][:-4])
        b = str(each.split('-')[1])
        List.append(a) #资金账号list
        List1.append(b) #客户姓名list
    #获取用户配置表里的资金账号列表
    tempList = df.drop_duplicates(["资金账户"], keep="last")['资金账户'].values
    # print(tempList)
    for i in range(len(List)):
        #如果资金账号在用户配置表里，调取配置表信息
        if List[i] in tempList:
            cusDitalDict = {}
            tagDF = df[df['资金账户'] == List[i]]
            cusDitalDict['客户姓名'] = tagDF['客户姓名'].values[0]
            cusDitalDict['股票列表'] = dealACumRecorder(tagDF)  #以标的代码为key、股票信息字典为value
            cusDitalDict['营业部'] = getYYBName(List[i])
            cusDict[List[i]] = cusDitalDict
        #如果不在，获取相关的数据
        else:   
            cusDitalDict = {}
            cusDitalDict['客户姓名'] = List1[i]
            cusDitalDict['股票列表'] = getstocklist(List[i])   
            cusDitalDict['营业部'] = getYYBName(List[i])    
            cusDict[List[i]] = cusDitalDict
            
    return cusDict

#从交易记录估算该用户的佣金率
def getYongJinRate(df):
    # 默认佣金为万3.5
    defaultYongJinRate = 0.00035
    # 对交易数据按时间降序，取第一个手续费不为0，方向为买入的数据估算其佣金
    tempDf = df[(df['方向'] == '买') & (df['手续费'] != 0)].sort_values(by=['日期'], ascending=(False))
    #手续费的估算要用手续费大于5的，且成交金额要足够大才准确。（为啥是20000可以问一下嘉林）
    SXFlist = tempDf[(df['手续费'] > 5) & (df["成交金额"]>20000)]['手续费'].values 
    bugMoneyList = tempDf[(df['手续费'] > 5) & (df["成交金额"]>20000)]['成交金额'].values
    #如果手续费记录不是空的
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


#根据某客户某一天某一支票的交易记录计算该票交易过程中所产生的所有的手续费
def getTotleFee(df, soldMoneyList, buyMoneyList, yongJinRate, isMian5, YHSRate):
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


#从交易记录判断该用户该票是否平仓，若未平仓或零股，就用收盘价估算平仓所需要的资金，为后面估算收益做准备
def getPingCangAndPing(soldNum, buyNum, code, day):
    if soldNum - buyNum == 0:
        pc = 0
        ping = "平"
    else:
        date =datetime.datetime.strptime(day,'%Y-%m-%d')
        year = date.year
        month = date.month
        day =  date.day
        pc = jq.get_price(code, count = 1,end_date = datetime.datetime(year,month,day,15,0,0),fields = ['close'],frequency = "1m",fq = None) * (soldNum - buyNum)  # pc为最终平仓所需金额
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

#根据交易记录计算某个用户某一天某一支票的收益以及手续费等信息。上面的2个函数都是服务这个的。
def dealDataProcess(df, code, yongJinRate, isMian5, day):
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
    #扣除手续费前盈利
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

#返回某个用户某一天某只股票的胜负次数，为计算胜率做准备
def get_num_trading(df):
    df = df.reset_index(drop = True)
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
                avg_sell = sell_money/sell_num
                avg_buy = buy_money/buy_num
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

#返回列表的绝对值
def ABS(List):
    newList=[]
    for i in List:
        a=abs(i)
        newList.append(a)
    return newList     

def getACusDealDayList(df):
    dealDayList = df.drop_duplicates(["日期"], keep="last")['日期'].values
    return dealDayList


def get2float(src):
    return float('%.2f' % src)

def get3float(src):
    return float('%.3f' % src)


#----------------------------------主处理函数-------------------------------------------#
#这里的函数都是和生成单票收益、组合收益、营业部收益报告表直接相关的函数
#附：index=["资金账号", "营业部", "账户名", "交易日期", "股票代码", "股票级数","股票名称", "市值", 
#"交易市值","当天交易额", "换手率%", "当天收益", 
#"收益率%", "累计收益", "佣金", "平仓", "连续亏损天数", "胜率",
# "佣金率","用户授权资金","现金收益率%","现金七日年化收益率%","连续盈利天数","当前仓位%","撤单率%",
#"盈亏类型","十日亏损天数","十日盈亏比","十日盈利天数","五日累积收益率%"] 

def processAStockADay(df, account, day, YYB, yongJinRate, isMian5, cumName, stockCode, stockDict, index, stockrankdf, summary_dan):
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
    #print(day,type(day))
    # 股票代码
    aRecord[index[4]] = stockCode
    #股票级数
    #print(stockCode)
    a = stockrankdf[stockrankdf["code"]==stockCode]
    if a.empty:
        aRecord[index[5]] = 1
    else:
        aRecord[index[5]] = a["ALPHA-T回测收益分级"].values[0]

    # 股票名称
    aRecord[index[6]] = get_security_info(stockCode).display_name
    # 目标市值
    if day == today:
        if pd.isnull(stockDict[stockCode]['目标市值']):
            aRecord[index[7]] = NaN
        else:
            aRecord[index[7]] = int(stockDict[stockCode]['目标市值'])
    elif day in list4:
        oldinfo=summary_dan[(summary_dan["交易日期"]==day) & (summary_dan["资金账号"]==account) & (summary_dan["股票代码"]==stockCode)]
        oldinfo=oldinfo.reset_index(drop=True)
        if oldinfo.empty:
            aRecord[index[7]] = NaN
        else:
            aRecord[index[7]]=oldinfo["市值"][0]
    else:
        aRecord[index[7]] = NaN
    #交易市值
    if day == today:
        if pd.isnull(stockDict[stockCode]['交易市值']):
            aRecord[index[8]] = NaN
        else:
            aRecord[index[8]] = int(stockDict[stockCode]['交易市值'])
    elif day in list4:
        oldinfo=summary_dan[(summary_dan["交易日期"]==day) & (summary_dan["资金账号"]==account) & (summary_dan["股票代码"]==stockCode)]
        oldinfo=oldinfo.reset_index(drop=True)
        if oldinfo.empty:
            aRecord[index[8]] = NaN
        else:
            aRecord[index[8]] = oldinfo['交易市值'][0]
    else:
        aRecord[index[8]] = NaN
    # 当天交易额
    aRecord[index[9]] = df['成交金额'].sum()
    #当天换手率
    if pd.isnull(aRecord[index[7]]):
        aRecord[index[10]] = NaN 
    elif aRecord[index[8]]==0:
        aRecord[index[10]] = NaN 
    else:
        aRecord[index[10]] = get2float(100 * aRecord[index[9]] / aRecord[index[8]]) #从6月21日起，换手率 = 当天交易额/交易市值，之前是= 当天交易额/目标市值

    # 处理当天收益相关数据                                
    dealDict = dealDataProcess(df,stockCode,yongJinRate,isMian5,day)                                
    # 当天收益
    aRecord[index[11]] = get2float(dealDict['dayReturn'])
    # 当天收益率 （与换手率不同，当天收益率采用目标市值作为基底，这对于投资者来说是比较合理的。）
    if pd.isnull(aRecord[index[7]]):
        aRecord[index[12]] = NaN
    elif aRecord[index[7]]==0:
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
    total_num_trading,  win_num_trading= get_num_trading(df)
    if total_num_trading != 0:
        aRecord[index[17]] =  get2float(win_num_trading / total_num_trading * 100)
    else:
        aRecord[index[17]] = NaN
    
    # 佣金率
    aRecord[index[18]] = yongJinRate
    #使用资金
    if day == today:
        if pd.isnull(stockDict[stockCode]['用户授权资金']):
            aRecord[index[19]] = NaN
        else:
            aRecord[index[19]] = int(stockDict[stockCode]['用户授权资金'])
    elif day in list4:
        oldinfo=summary_dan[(summary_dan["交易日期"]==day) & (summary_dan["资金账号"]==account) & (summary_dan["股票代码"]==stockCode)]
        oldinfo=oldinfo.reset_index(drop=True)
        if oldinfo.empty:
            aRecord[index[19]] = NaN
        else:
            aRecord[index[19]]=oldinfo["用户授权资金"][0]
    else:
        aRecord[index[19]] = NaN

    #现金收益率%
    if pd.isnull(aRecord[index[19]]):
        aRecord[index[20]] = NaN
    elif aRecord[index[19]]==0:
        aRecord[index[20]] = NaN
    else:
        aRecord[index[20]] = get2float(dealDict['dayReturn'] * 100 / aRecord[index[19]])


    #现金七日年化收益率%
    #年化现金收益率自6月26日以来，是每日收益率乘以250，之前是乘以240
    if pd.isnull(aRecord[index[20]]) :
        aRecord[index[21]] = NaN
    else:
        aRecord[index[21]] = get2float(aRecord[index[20]]*250)
    
    #连续盈利天数
    if dealDict['dayReturn'] > 0:
        aRecord[index[22]] = 1
    else:
        aRecord[index[22]] = 0


    #当天仓位
    if not pd.isnull(aRecord[index[7]]) and not pd.isnull(aRecord[index[8]]):
        aRecord[index[23]] = get2float(aRecord[index[8]]/aRecord[index[7]]) * 100
    else:
        aRecord[index[23]] = NaN
        
    #撤单率
    aRecord[index[24]] = get2float(df['撤单数量'].sum()/df['委托数量'].sum() * 100)
    
    #盈亏类型
    if aRecord[index[11]] >= 0:
        aRecord[index[25]] = '当日无亏'
    else:
        if aRecord[index[15]] == '平':
            if dealDict['return_before_fee'] > 0:
                aRecord[index[25]] = '算法盈利但因手续费亏'
            if dealDict['return_before_fee'] ==0:
                aRecord[index[25]] = '算法不盈不亏但因手续费亏'
            if dealDict['return_before_fee'] < 0:
                aRecord[index[25]] = '算法亏损但因手续费更亏'
        else:
            if dealDict['return_before_fee'] > 0:
                aRecord[index[25]] = '按收盘价平仓后盈利，但因手续费亏'
            if dealDict['return_before_fee'] ==0:
                aRecord[index[25]] = '按收盘价平仓后不盈不亏，但因手续费亏'
            if dealDict['return_before_fee'] < 0:
                aRecord[index[25]] = '按收盘价平仓后亏损，但因手续费更亏'
                #反正都是手续费的锅
                
    #十日亏损天数
    aRecord[index[26]] = aRecord[index[16]]
    
    #十日盈亏金额比
    if aRecord[index[11]] > 0:
        aRecord[index[27]] = '正无穷' #第一日
    elif aRecord[index[11]] == 0:
        aRecord[index[27]] = 1
    else:
        aRecord[index[27]] = 0
    
    #十日盈利天数
    aRecord[index[28]] = aRecord[index[22]]
    
    #五日累积收益率% (目标市值加权)
    if pd.isnull(aRecord[index[12]]) :
        aRecord[index[29]] = NaN
    else:
        aRecord[index[29]] = aRecord[index[12]]
    
    return aRecord


def processACusDF(df, account, bookDict,index, stockrankdf, summary_dan):
    # 处理一个客户的数据,以一个dataFrame的格式输出出去
    aCusOutDf = pd.DataFrame()
    # 先对这个文件排序
    df = df.sort_values(by=['日期', '名称', '时间'], ascending=(True, True, True))
    # 对DF的股票代码格式进行转换，转换为聚宽的数据格式
    df['代码'] = df['代码'].map(lambda x: normalize_code(x.lstrip("'")))
    print('对文件进行重新排序完成！')
    # 循环股票来做吧
    tempDict = bookDict[account]
    cumName = tempDict['客户姓名']
    stockDict = tempDict['股票列表']
    YYB = tempDict['营业部']
    print(ATimeTool.getnow(), ':', '开始获取这个客户的佣金比例。')
    yongJinRate, isMian5 = getYongJinRate(df)
    #print(ATimeTool.getnow(), ':', '获取这个客户的佣金比例成功：', yongJinRate)
    # 获取这个客户待处理的股票列表
    stockCodeList = getAcusStockCodeList(df)

    #print(ATimeTool.getnow(), ':', '需要处理的股票列表：', stockCodeList)
    # 处理一个客户，一个股票
    for stockCode in stockCodeList:
        # 处理每个股票的数据
        stockCodeTagDf = df[df['代码'] == stockCode]
        # 计算每一天的股票收益
        # 获取该股票的交易天数
        dealDayList = getACusDealDayList(stockCodeTagDf)
        #创建一个列表，储存最近7天的现金收益率
        rateweeklist=[]
        #创建一个列表，储存最近10天的亏损天数
        tendayslist = []
        #创建一个列表，储存最近10天的盈利天数
        tendayslist_win = []
        #创建一个列表，储存最近10天的盈亏金额比
        profitlosslist = []
        #创建一个列表，储存最近5天的收益率
        returnweek_list = []
        #创建一个列表，储存最近5日的目标市值
        target_mark = []
        # 处理一个客户，一个股票，一天的数据
        for i in range(len(dealDayList)):
            day = dealDayList[i]
            stockCodeCodeDayTagDF = stockCodeTagDf[stockCodeTagDf['日期'] == day]
            aRecorderDict = processAStockADay(stockCodeCodeDayTagDF, account, day, YYB, yongJinRate, isMian5, cumName,stockCode,stockDict,index, stockrankdf, summary_dan)
            rateweeklist.append(aRecorderDict["现金收益率%"])
            rateweeklist=rateweeklist[-7:]
            tendayslist.append(aRecorderDict["连续亏损天数"])
            tendayslist = tendayslist[-10:]
            tendayslist_win.append(aRecorderDict["连续盈利天数"])
            tendayslist_win = tendayslist_win[-10:]
            profitlosslist.append(aRecorderDict["当天收益"])
            profitlosslist = profitlosslist[-10:]
            returnweek_list.append(aRecorderDict['收益率%'])
            returnweek_list = returnweek_list[-5:]
            target_mark.append(aRecorderDict['市值'])
            target_mark = target_mark[-5:]
            
            # 如果是第一天，所有累计的因子都是等于当天的因子的值
            if i == 0:
                # 保存这次记录，用作下一次的历史数据
                lastDayRecord = aRecorderDict
                # 将当前的数据写出去
                aRecordDf = pd.DataFrame(aRecorderDict, index=[0])
                aCusOutDf = pd.concat([aCusOutDf, aRecordDf], axis=0)
                continue
            else:
                # 计算累计收益
                aRecorderDict['累计收益'] = lastDayRecord['累计收益'] + aRecorderDict['累计收益']
                # 计算连续亏损天数
                if lastDayRecord['连续亏损天数'] != 0 and aRecorderDict['连续亏损天数'] == 1:
                    aRecorderDict['连续亏损天数'] = lastDayRecord['连续亏损天数'] + 1
                #计算连续盈利天数
                if lastDayRecord["连续盈利天数"] != 0 and aRecorderDict["连续盈利天数"] == 1:
                    aRecorderDict["连续盈利天数"] = lastDayRecord["连续盈利天数"] + 1
            
                #计算现金七日年化收益率
                if not pd.isnull(aRecorderDict["现金收益率%"]):           
                    aRecorderDict["现金七日年化收益率%"] = get2float(250*np.nansum(rateweeklist)/len([i for i in rateweeklist if not pd.isnull(i)]))                    
                else:
                    aRecorderDict["现金七日年化收益率%"] = NaN
                
                #计算过去十日亏损总天数
                aRecorderDict["十日亏损天数"] = sum(tendayslist)
                
                #计算过去十日盈利总天数
                aRecorderDict["十日盈利天数"] = sum(tendayslist_win)
                
                #计算过去十日盈利日的利润与亏损日的损失的金额比
                profit = 0
                loss = 0
                for j in range(len(profitlosslist)):
                    if profitlosslist[j]>=0:
                        profit = profit + profitlosslist[j]
                    else:
                        loss = loss + profitlosslist[j]
                if loss == 0:
                    aRecorderDict["十日盈亏金额比"] = '正无穷'
                else:
                    aRecorderDict["十日盈亏金额比"] = get2float(profit/((-1)*loss))
                
                #计算过去5日累积收益率%
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
                            aRecorderDict['五日累积收益率%'] = get2float(sum_product/sum_target_mark * count)
                        else:
                            aRecorderDict['五日累积收益率%'] = NaN
                    else:
                        aRecorderDict["五日累积收益率%"] = NaN
                else:
                    aRecorderDict['五日累积收益率%'] = NaN
               
                # 保存这次记录，用作下一次的历史数据
                lastDayRecord = aRecorderDict

                # 将当前的数据写出去
                aRecordDf = pd.DataFrame(aRecorderDict, index=[0])
                aCusOutDf = pd.concat([aCusOutDf, aRecordDf], axis=0)
                
    aCusOutDf = aCusOutDf.reindex(columns = index)

    return aCusOutDf

def subProcess(bookDict,index, stockrankdf, summary_dan):
    # 循环处理今天在交易的客户数据
    procesList = list(bookDict.keys()) # 资金账号
    allCusOutDf = pd.DataFrame()
    newrecorddf = pd.DataFrame()
    for account in procesList:
        print(ATimeTool.getnow(), ':', '开始处理单票，资金账号：', account, '客户姓名：', bookDict[account]['客户姓名'])
        # 读取这个哥们的交易文件
        filename = jiaoYiNameDict[account]
        filePath = jiaoyipath + '/' + filename
        aCusTotleDF = AFileTool.open_csv(filePath)
        aCusTotleDF['时间'] = aCusTotleDF['时间'].apply(lambda x: int(datetime.datetime.strftime(datetime.datetime.strptime(x,"%H:%M:%S"),'%H%M%S')))
        aCusTotleDF = aCusTotleDF[aCusTotleDF['时间'] >= 93000] # 筛选出时间大于九点半交易
        #改变点： 增加新增记录
        aCusTotleDF,newrecorddf=addjiaoyijilu(aCusTotleDF,account,newrecorddf)

        print(ATimeTool.getnow(), ':', 'open file:', filePath, ' 成功！')
        if aCusTotleDF.empty:
            #print(ATimeTool.getnow(), ':', '资金账号：', account, '客户姓名：', bookDict[account]['客户姓名'],
                  #'交易记录为空，退出该客户的数据处理，进入下一个账户！')
            continue
        
        aCusOutDf = processACusDF(aCusTotleDF, account, bookDict,index, stockrankdf, summary_dan)
        allCusOutDf = pd.concat([allCusOutDf, aCusOutDf], axis=0)
       # print(ATimeTool.getnow(), ':', '单票处理完成，资金账号：', account, '客户姓名：', bookDict[account]['客户姓名'])
        # break
    return allCusOutDf

def aCusComDayProcess(df, account, day,trad_dayrecord,yongJinRate,isMian5,index):
    # 创建一条记录
    aRecord = df.iloc[0, :].to_dict()
    #资金账号
    #aRecord[index[0]] = account
    #营业部
    #aRecord[index[1]] = YYB
    #账户名
    # aRecord[index[2]] =cumName
    #交易日期
    #aRecord[index[3]] = day
    #股票代码
    aRecord[index[4]] = NaN
    #股票级数
    aRecord[index[5]] = NaN
    #股票名称
    aRecord[index[6]] = '股票组合'
    #市值
    if df['市值'].isnull().all():
        aRecord[index[7]] = NaN
    else:
        aRecord[index[7]] = df['市值'].sum()
    
    #交易市值
    if df['交易市值'].isnull().all():
        aRecord[index[8]] = NaN
    else:
        aRecord[index[8]] = df['交易市值'].sum()
    

    #当天交易额
    aRecord[index[9]] = df['当天交易额'].sum()

    #当天换手率
    if (not df['交易市值'].isnull().all()) and (not df['换手率%'].isnull().all()): 
        trading_values = list(df['交易市值'].values)
        turnover_rates = list(df['换手率%'].values)
        nrow = len(df)
        sum_product = 0
        for j in range(nrow):
            if (not pd.isnull(trading_values[j])) and (not pd.isnull(turnover_rates[j])):
                product = turnover_rates[j] * trading_values[j]
                sum_product = sum_product + product
        aRecord[index[10]] = get2float(sum_product/aRecord[index[8]])

    else:
        aRecord[index[10]] = NaN 
    
    #当天收益
    aRecord[index[11]] = get2float(df['当天收益'].sum())

        
    #当天收益率 
    if not pd.isnull(aRecord[index[7]]) and (not df["收益率%"].isnull().all()):
        target_values = list(df["市值"].values)
        return_rates = list(df["收益率%"].values)
        nrow = len(df)
        sum_product = 0
        for m in range(nrow):
            if (not pd.isnull(target_values[m])) and (not pd.isnull(return_rates[m])):
                product = target_values[m] * return_rates[m]
                sum_product = sum_product + product
        aRecord[index[12]] = get2float(sum_product/aRecord[index[7]]) 
    else:
        aRecord[index[12]] = NaN
   
    #累计收益
    aRecord[index[13]] = aRecord[index[11]]
    
    #佣金
    aRecord[index[14]] = get2float(df[index[14]].sum())
    
    #平仓
    aRecord[index[15]] = NaN
    
    #连续亏损天数
    if aRecord[index[11]]<0:
        aRecord[index[16]] = 1
    else:
        aRecord[index[16]] = 0

        
    #胜率
    list_of_stocks = list(df['股票代码'].values)
    total_num_trading = 0
    win_num_trading = 0
    for stock in list_of_stocks:
        trad_daystockrecord = trad_dayrecord[trad_dayrecord['代码']==stock].reset_index(drop = True)
        total_num_stocktrading,  win_num_stocktrading= get_num_trading(trad_daystockrecord)
        total_num_trading = total_num_trading + total_num_stocktrading
        win_num_trading = win_num_trading + win_num_stocktrading
    if total_num_trading != 0:
        aRecord[index[17]] = get2float(win_num_trading/total_num_trading * 100)
    else:
        aRecord[index[17]] = NaN
                 
    #佣金率
    #aRecord[index[18]] = NaN
                 
    #使用资金
    aRecord[index[19]] = get2float(df["用户授权资金"].iloc[0])
                 
    #使用目标市值加权的现金收益率
    if (not pd.isnull(aRecord[index[19]])) and (not df["市值"].isnull().all()) and (not df["现金收益率%"].isnull().all()): 
        target_values = list(df["市值"].values)
        return_percash = list(df["现金收益率%"].values)
        nrow = len(df)
        sum_product = 0
        for n in range(nrow):
            if (not pd.isnull(target_values[n])) and (not pd.isnull(return_percash[n])):
                product = target_values[n] * return_percash[n]
                sum_product = sum_product + product
        aRecord[index[20]] = get2float(sum_product/aRecord[index[7]]) 
    else:
        aRecord[index[20]] = NaN
                 
    #七日年化现金收益率
    if pd.isnull(aRecord[index[20]]):
        aRecord[index[21]] = NaN 
    else:
        aRecord[index[21]] = get2float(aRecord[index[20]]*250)
    
    #连续盈利天数
    if aRecord[index[11]] > 0:
        aRecord[index[22]] = 1
    else:
        aRecord[index[22]] = 0
    
    #当天仓位
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
            aRecord[index[23]]= get2float(sum_of_product/df['市值'].sum() * 100)
    else:
        aRecord[index[23]] = NaN
        
    #撤单率
    aRecord[index[24]] = get2float(trad_dayrecord['撤单数量'].sum()/trad_dayrecord['委托数量'].sum() * 100)
    
    
    #盈亏类型
    list_of_stocks = list(df['股票代码'].values)
    if aRecord[index[11]] >= 0: #收益不为负
            aRecord[index[25]] = '当日该用户组合不亏'
    else:#亏损情况下：
        total_returnbffee = 0
        for stock in list_of_stocks:
            stock_dayrecord = trad_dayrecord[trad_dayrecord['代码']==stock]
            dealDict = dealDataProcess(stock_dayrecord,stock,yongJinRate,isMian5,day)
            total_returnbffee = total_returnbffee + dealDict['return_before_fee']
        if (df['平仓'] == '平').all(): #如果该日该用户所有股票全是平仓的
            if total_returnbffee > 0:
                aRecord[index[25]] = '算法盈利但因手续费亏'
            elif total_returnbffee == 0:
                aRecord[index[25]] = '算法不盈不亏但因手续费亏'
            else:
                aRecord[index[25]] = '算法亏损但因手续费更亏'
        else:#该日该用户所持股票有未平仓的
            if total_returnbffee > 0:
                aRecord[index[25]] = '未平仓的按收盘价平仓后，组合整体盈利，但因手续费亏损'
            elif total_returnbffee == 0:
                aRecord[index[25]] = '未平仓的按收盘价平仓后，组合不盈不亏，但因手续费亏损'
            else:
                aRecord[index[25]] = '未平仓的按收盘价平仓后，组合整体亏损，但因手续费更亏' 
    
    #十日亏损天数
    aRecord[index[26]] = aRecord[index[16]]
    
    
    #十日盈亏金额比
    if aRecord[index[11]] > 0:
        aRecord[index[27]] = '正无穷' 
    elif aRecord[index[11]] == 0:
        aRecord[index[27]] = 1
    else:
        aRecord[index[27]] = 0
        
    #十日盈利天数
    aRecord[index[28]] = aRecord[index[22]]
    
    #五日累积收益率%
    if pd.isnull(aRecord[index[12]]) :
        aRecord[index[29]] = NaN
    else:
        aRecord[index[29]] = aRecord[index[12]]
    
    return aRecord
    

def aCusComProcess(df, account,trad_record,index):
    # 一个账户，循环处理日期
    aCusComDf = pd.DataFrame()
    lastDayRecord = {}
    rateweeklist = []
    tendayslist = []
    tendayslist_win = []
    profitlosslist = []
    returnweek_list = []
    target_mark = []
    dayList = df.drop_duplicates(["交易日期"], keep="last")['交易日期'].values.tolist()
    dayList.sort()
    #获取该用户的佣金信息
    yongJinRate, isMian5 = getYongJinRate(trad_record)
    #print(ATimeTool.getnow(), ':', '需要处理的时间为：', dayList)
    for i in range(len(dayList)):
        day = dayList[i]
        aCusComDayTagDf = df[df['交易日期'] == day]
        trad_dayrecord = trad_record[trad_record['日期']==day].reset_index(drop = True)
        aRecorderDict = aCusComDayProcess(aCusComDayTagDf, account, day,trad_dayrecord,yongJinRate,isMian5,index)
        rateweeklist.append(aRecorderDict['现金收益率%'])
        rateweeklist = rateweeklist[-7:]
        tendayslist.append(aRecorderDict['连续亏损天数'])
        tendayslist = tendayslist[-10:]
        tendayslist_win.append(aRecorderDict['连续盈利天数'])
        tendayslist_win = tendayslist_win[-10:]
        profitlosslist.append(aRecorderDict["当天收益"])
        profitlosslist = profitlosslist[-10:]
        returnweek_list.append(aRecorderDict['收益率%'])
        returnweek_list = returnweek_list[-5:]
        target_mark.append(aRecorderDict['市值'])
        target_mark = target_mark[-5:]
        # 如果是第一天，所有记录都不需要改变
        if i == 0:
            # 保存这次记录，用作下一次的历史数据
            lastDayRecord = aRecorderDict
            # 将当前的数据写出去
            aRecordDf = pd.DataFrame(aRecorderDict, index=[0])
            aCusComDf = pd.concat([aCusComDf, aRecordDf], axis=0)
            continue
        else:
            #计算累计收益
            aRecorderDict['累计收益'] = lastDayRecord['累计收益'] + aRecorderDict['累计收益']
            # 计算连续亏损天数
            if lastDayRecord['连续亏损天数'] != 0 and aRecorderDict['连续亏损天数'] == 1:
                aRecorderDict['连续亏损天数'] = lastDayRecord['连续亏损天数'] + 1
            #计算连续盈利天数
            if lastDayRecord["连续盈利天数"] != 0 and aRecorderDict["连续盈利天数"] == 1:
                aRecorderDict["连续盈利天数"] = lastDayRecord["连续盈利天数"] + 1

            #计算现金七日年化收益率
            if not pd.isnull(aRecorderDict["现金收益率%"]):           
                aRecorderDict["现金七日年化收益率%"] = get2float(250*np.nansum(rateweeklist)/len([i for i in rateweeklist if not pd.isnull(i)]))                    
            else:
                aRecorderDict["现金七日年化收益率%"] = NaN

            #计算过去十日亏损总天数
            aRecorderDict["十日亏损天数"] = sum(tendayslist)
            
             #计算过去十日盈利日的利润与亏损日的损失的金额比
            profit = 0
            loss = 0
            for j in range(len(profitlosslist)):
                if profitlosslist[j]>=0:
                    profit = profit + profitlosslist[j]
                else:
                    loss = loss + profitlosslist[j]
            if loss == 0:
                aRecorderDict["十日盈亏金额比"] = '正无穷'
            else:
                aRecorderDict["十日盈亏金额比"] = get2float(profit/((-1)*loss))
            
            #计算过去十日盈利总天数
            aRecorderDict["十日盈利天数"] = sum(tendayslist_win)
            
            #计算五日累积收益率
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
                        aRecorderDict['五日累积收益率%'] = get2float(sum_product/sum_target_mark * count)
                    else:
                        aRecorderDict['五日累积收益率%'] = NaN
                else:
                    aRecorderDict["五日累积收益率%"] = NaN
            else:
                aRecorderDict['五日累积收益率%'] = NaN
               
            # 保存这次记录，用作下一次的历史数据
            lastDayRecord = aRecorderDict

            # 将当前的数据写出去
            aRecordDf = pd.DataFrame(aRecorderDict, index=[0])
            aCusComDf = pd.concat([aCusComDf, aRecordDf], axis=0)
                                  
    return aCusComDf.reindex(columns = index)


def combProcess(df, bookDict, index):
    allCombOutDf = pd.DataFrame()
    # 资金账号列表
    accountList = df.drop_duplicates(["资金账号"], keep="last")['资金账号'].values
    newrecorddf = pd.DataFrame()
    for account in accountList:
        print(ATimeTool.getnow(), ':', '开始处理组合，资金账号：', account, '客户姓名：', bookDict[account]['客户姓名'])
        aCusTagDf = df[df['资金账号'] == account]
        #获取该用户交易记录以供备用
        filename = jiaoYiNameDict[account]
        filePath = jiaoyipath + '/' + filename
        trad_record = AFileTool.open_csv(filePath)
        trad_record['时间'] = trad_record['时间'].apply(lambda x: int(datetime.datetime.strftime(datetime.datetime.strptime(x,"%H:%M:%S"),'%H%M%S')))
        trad_record = trad_record[trad_record['时间'] >= 93000]
        #新增记录
        trad_record,newrecorddf=addjiaoyijilu(trad_record,account,newrecorddf)
        #将交易记录中股票代码正规化
        trad_record['代码'] = trad_record['代码'].map(lambda x: normalize_code(x.lstrip("'")))
        
        aCusOutDf = aCusComProcess(aCusTagDf, account, trad_record,index)
        allCombOutDf = pd.concat([allCombOutDf, aCusOutDf], axis=0)
        #print(ATimeTool.getnow(), ':', '组合处理完成，资金账号：', account, '客户姓名：', bookDict[account]['客户姓名'])
    return allCombOutDf


#=================================主要处理过程================================================

index = ["资金账号", "营业部", "账户名", "交易日期", "股票代码", "股票级数","股票名称", "市值", "交易市值","当天交易额", "换手率%", "当天收益", "收益率%", "累计收益", "佣金", "平仓", "连续亏损天数", "胜率%",
         "佣金率","用户授权资金","现金收益率%","现金七日年化收益率%","连续盈利天数","当天仓位%","撤单率%","盈亏类型","十日亏损天数","十日盈亏金额比","十日盈利天数","五日累积收益率%"] 


desktop = "D:/Allen_UIBE/Career/Internship/JoinQuant/长谦财富/运行文件"
rootpath = desktop + "/运营数据"   
#获取今天日期
today=time.strftime("%Y-%m-%d",time.localtime(time.time()))

bookName = 't0_total_stock_list_'+ time.strftime("%Y%m%d",time.localtime(time.time())) +'.csv' 
YYBname = '营业部配置表.xlsx'
jiaoyipath = 'trans-2019-09-24-15-22-16' 

jiaoyipath = rootpath + "/" + '交易记录' + '/' + jiaoyipath # 多个csv，每个csv记录单个用户当天的各股票的买卖记录
OutPath = rootpath + '/' + '收益计算结果' 
bookFile = rootpath + '/' + '用户股票配置' + '/' + bookName # 一个csv，记录当天哪些用户的哪些股票在跑AlphaT
YYBfile = rootpath + '/' + '营业部配置' + '/' + YYBname     # 一个csv，记录营业部的基本信息
stockrankpath = rootpath + "/" + "Alpha-T个股近1月收益排名_20190910.xlsx"

jiaoYiNameDict = listFilename(jiaoyipath) # jiaoYiNameDict：以所有资金账号为key、文件名（用户名）为value

#读取股票所属收益级数表格
stockrankdf = pd.read_excel(stockrankpath,encoding="gbk") # 票池的股票分级信息

# 获取配置表
bookDF = AFileTool.open_csv(bookFile) # pd.read_csv(bookFile,encoding='gbk',engine='python')当天哪些用户的哪些股票在跑AlphaT
YYBDF = AFileTool.open_excel(YYBfile) # pd.read_excel(YYBfile,encoding = "gbk")
print("开始读取已有的单票收益计算表格")
# 获取已有记录的单票收益表
summary_dan=getolddate(rootpath)
#日期列表
list4=summary_dan.drop_duplicates(["交易日期"],keep="first")["交易日期"].values.tolist()
print(list4)
print('配置表处理完成！')
#获取配置表数据，创立一个以资金账号为key的dict
bookDict = dealBookDate(bookDF)
print(ATimeTool.getnow(), ':', '配置表处理完成！')


print('#----------------开始处理单票数据--------------------#')
allCusOutDf = subProcess(bookDict,index,stockrankdf,summary_dan) 
# 将这个文件写出到输出目录
today = ATimeTool.getToday()
OutPath = rootpath + '/' + '收益计算结果'
print(OutPath)
outFilePath = OutPath + '/' + today   
outFileName = outFilePath + "/" + '汇总数据' + "/" + today + '_单票收益记录.xlsx'
dan_path = outFileName
#allCusOutDf.to_csv(outFileName,encoding="utf-8")
AFileTool.write_excel(allCusOutDf, outFileName)    
print('#----------------单票数据处理结束--------------------#')


print('#----------------开始处理组合数据--------------------#')
allCombOutDf = combProcess(allCusOutDf, bookDict,index)
outFileName = outFilePath + "/" + '汇总数据' + "/" + today + '_组合收益记录.xlsx'
zu_path =  outFileName
#allCombOutDf.to_csv(outFileName,encoding="utf-8")
AFileTool.write_excel(allCombOutDf, outFileName)
print('#----------------处理组合数据结束--------------------#')


print('#----------------开始生成收益表格--------------------#')
dataTime = 21
OutPath = rootpath + '/' + '收益计算结果' 
srcdir = OutPath + "/" + today + "/" + '汇总数据'
danPianFile = today + '_单票收益记录.xlsx'
zuHeFile = today + '_组合收益记录.xlsx'
a = ATExcel1(dataTime, OutPath, srcdir, danPianFile, zuHeFile)
b = ATExcel(dataTime, OutPath, srcdir, danPianFile, zuHeFile)
finalmain(dan_path,zu_path,jiaoyipath,rootpath)
#findpotentialclient(rootpath)

print('#----------------收益表格处理结束--------------------#')