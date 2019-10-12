'''
    专门为处理聚宽-Alpha-T相关业务数据的包
    主要提供基础的业务函数
    editor：zhanjialin
    time:20190330
'''

import os,shutil
import pandas as pd
import numpy as np
import time,datetime
import calendar

class Alpha_time:

    def __init__(self,zhangQi='201903'):
        #self.name=name
        #self.price=price
        self._zhangQi = zhangQi
        self._year = self._zhangQi[:4]
        self._month = self._zhangQi[-2:]


    def getMonthFirstDayAndLastDay(self,year=None, month=None):
        """
        :param year: 年份，默认是本年，可传int或str类型
        :param month: 月份，默认是本月，可传int或str类型
        :return: firstDay: 当月的第一天，datetime.date类型
                  lastDay: 当月的最后一天，datetime.date类型
        """
        if year:
            year = int(year)
        else:
            year = datetime.date.today().year

        if month:
            month = int(month)
        else:
            month = datetime.date.today().month

        # 获取当月第一天的星期和当月的总天数
        firstDayWeekDay, monthRange = calendar.monthrange(year, month)

        # 获取当月的第一天
        firstDay = datetime.date(year=year, month=month, day=1)
        lastDay = datetime.date(year=year, month=month, day=monthRange)

        return firstDay, lastDay

    def getBeginTime(self,applyTime):
        #获取该账户的结算开始时间

        paymentBeginTime = self.getMonthFirstDayAndLastDay(self._year,self._month)[0]

        applyTime = datetime.datetime.strptime(applyTime,'%Y-%m-%d').date()
        #print(paymentBeginTime,applyTime)

        if applyTime>paymentBeginTime:
            #申请日期加一天为计费日
            return applyTime+datetime.timedelta(days=1)
        else:
            return paymentBeginTime

    def getEndTime(self):
        #获取该账期的结束时间
        return self.getMonthFirstDayAndLastDay(self._year,self._month)[1]

    def dataTimeToStr(self,orgTime):
        #将一个时间格式转换为str
        return datetime.datetime.strftime(orgTime,'%Y-%m-%d')

    def dataMiaoToStr(self,orgTime):
        #将一个精确到秒得时间格式转为str
        return datetime.datetime.strftime(orgTime,'%Y-%m-%d %H:%M:%S')

    def TimeAddDay(self,orgTime,numDay):
        return 0

    def date(self,dates):  # 定义转化日期戳的函数,dates为日期戳
        delta = 1
        today = 1  # 将1899-12-30转化为可以计算的时间格式并加上要转化的日期戳
        return datetime.datetime.strftime(
            datetime.datetime.strptime('1899-12-30', '%Y-%m-%d') + datetime.timedelta(days=x), '%Y-%m-%d')  # 制定输出日期的格式

    def getToday(self):
        return self.dataTimeToStr(datetime.date.today())

    def getnow(self):
        return  self.dataMiaoToStr(datetime.datetime.now())

class fileProcess():
    def __init__(self):
        self._name = "zhanjialin"

    def open_csv(self,filePath):
        return pd.read_csv(filePath,encoding='gbk',engine='python')

    def open_excel(self,filePath):
        return pd.read_excel(filePath,encoding = "gbk")

    def write_excel(self,df,filePath):
        self.mkdir(filePath[:filePath.rfind('\\')])
        return df.to_excel(filePath,encoding = 'utf-8',index=False)

    def write_csv(self,df,filePath):
        self.mkdir(filePath[:filePath.rfind('\\')])
        return df.to_csv(filePath,encoding= 'utf-8',index = False)

    def mkdir(self,path):
        # 去除首位空格
        path = path.strip()
        # 去除尾部 \ 符号
        path = path.rstrip("\\")

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

    def listFilename(self,path):
        #输出一个以资金账号为key，文件名为value的dict
        fileList = os.listdir(path)
        fileNameDict = {}
        for each in fileList:
            a = int(each.split('-')[-1][:-4])
            fileNameDict[a] = each
        fileNameList = fileNameDict.keys()
        return fileNameDict
    
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
        yuanpath= rootpath + "\\" + "收益计算结果"
        filelist = os.listdir(pathyuan)
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
                path= yuanpath+"\\"+riqi+"\\"+"汇总数据"+"\\"+riqi+"_组合收益记录.csv"
                df_zu=pd.read_csv(path,encoding="utf-8",engine="python")[index]
                path= yuanpath+"\\"+riqi+"\\"+"汇总数据"+"\\"+riqi+"_单票收益记录.csv"
                df_dan=pd.read_csv(path,encoding="utf-8",engine="python")[index]
            elif changedate(riqi)>changedate("2019-04-20"):
                print("riqi:",riqi)
                path= yuanpath +"\\"+riqi+"\\"+"汇总数据"+"\\"+riqi+"_组合收益记录.xlsx"
                df_zu=pd.read_excel(path,encoding="utf-8")[index]
                path= yuanpath+"\\"+riqi+"\\"+"汇总数据"+"\\"+riqi+"_单票收益记录.xlsx"
                df_dan=pd.read_excel(path,encoding="utf-8")[index]
            else:
                path= yuanpath+"\\"+riqi+"\\"+"汇总数据"+"\\"+riqi+"_组合收益记录.csv"
                df_zu=pd.read_csv(path,encoding="gbk",engine='python')[index]
                path= yuanpath +"\\"+riqi+"\\"+"汇总数据"+"\\"+riqi+"_单票收益记录.csv"
                df_dan=pd.read_csv(path,encoding="gbk",engine='python')[index]
            df1=df_zu[df_zu["交易日期"]==riqi].reset_index(drop=True)
            df2=df_dan[df_dan["交易日期"]==riqi].reset_index(drop=True)
            summary_zu=pd.concat([summary_zu,df1],axis=0)
            summary_dan=pd.concat([summary_dan,df2],axis=0)
        summary_dan=summary_dan.reset_index(drop=True)
        summary_zu=summary_zu.reset_index(drop=True)
        return summary_dan