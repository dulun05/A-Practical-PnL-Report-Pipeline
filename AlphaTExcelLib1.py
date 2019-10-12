# -*- coding: utf-8 -*-
"""
Created on Tue Apr 23 13:33:45 2019

@author: FGD_Moon
"""

import pandas as pd
import numpy as np
import datetime
import time
import os
import openpyxl
from openpyxl.styles import Font, colors

class AlphaTExcel():

    def __init__(self,dataTime,rootPath,srcDir,danPiaoFileName,zuHeFileName, today):
#修改日期：2019/4/10  增加资金账号 
        self.dan_index = ["资金账号","账户名", "交易日期", "股票名称", "市值","当天收益", "收益率%","用户授权资金","现金收益率%", "累计收益","股票级数","平仓","连续亏损天数","连续盈利天数"]
        # 处理这一周的数据
        self.dataTime = 21                       
        self.rootPath = rootPath     
        self.today = today              
        ii = 10000000                             
                                      
        self.srcDir = srcDir                     

        # 单票文件
        self.danPiaoFileName = danPiaoFileName
        path1 = self.srcDir + '/' + self.danPiaoFileName
        # 组合文件
        self.zuHeFileName = zuHeFileName
        path2 = self.srcDir + '/' + self.zuHeFileName

        self.data_dan = self.formatDataTime(pd.read_excel(path1, encoding='utf-8'), '交易日期')

        self.data_dan = self.data_dan.sort_values(by=['资金账号', '股票名称', '交易日期'], ascending=(True, True, True))

        # data_dan = data_dan.sort_values(by='股票名称', ascending=True)
        # data_dan = data_dan.sort_values(by='交易日期', ascending=True)

        self.data_zu = self.formatDataTime(pd.read_excel(path2, encoding='utf-8'), '交易日期')
        self.data_zu = self.data_zu.sort_values(by=['资金账号', '交易日期'], ascending=(True, True))

        self.process_main(self.data_dan, self.data_zu)

    def formatDataTime(self,orgDf, timeName):
        '''
        for i in range(len(orgDf)):
            timeStruct = time.strptime(orgDf[timeName][i], "%Y/%m/%d")
            strTime = time.strftime("%Y/%m/%d", timeStruct)
            orgDf[timeName][i] = strTime
        '''
        orgDf[timeName] = orgDf[timeName].map(lambda x: time.strftime("%Y/%m/%d", time.strptime(x, "%Y-%m-%d")))
        return orgDf

    def creatOutData(self):
        out = pd.DataFrame(columns=self.dan_index)
        return out


    def compare_time(self,time1, time2):
        # 比较两个短时间的大小 输出两个值的差
        s_time = time.mktime(time.strptime(time1, '%Y/%m/%d'))
        e_time = time.mktime(time.strptime(time2, '%Y/%m/%d'))
        return int(s_time) - int(e_time)


    def creatAdict(self,data):
        dictTemp = {}

        for i in range(len(self.dan_index)):
            dictTemp[self.dan_index[i]] = data[i]

        return dictTemp


    # 数据表里插入一行空数据
    def addAData(self,orgDF):
        b = [''] * orgDF.shape[1]
        orgDF = orgDF.append(self.creatAdict(b), ignore_index=True)
        return orgDF


    def addAIndex(self,orgDF):
        orgDF = orgDF.append(self.creatAdict(self.dan_index), ignore_index=True)
        return orgDF


    # 将一组数据插入数组末尾，并加以行空格
    def addAMountOfDate(self,orgDF, addDF, danOrZu):
        orgDF = pd.concat([orgDF, addDF], axis=0)

        # 如果是单票不加
        if danOrZu == 0:
            return orgDF
        # 如果是组合
        elif danOrZu == 1:

            orgDF = self.addAData(orgDF)

            orgDF = self.addAIndex(orgDF)

            return orgDF


    # 利用组合表今天要处理的营业部名单,返回list
    def getYYBNameList(self,data_zu):
        outDF = data_zu.drop_duplicates(["营业部"],keep = 'first')
        return outDF["营业部"].tolist()


    # 获取今天要处理的XX营业部客户名单
    def getYYBcustomerList(self,data_zu, YYB):
        tempDF = data_zu[data_zu['营业部'] == YYB]
        outDF = tempDF.drop_duplicates(["资金账号"],keep = 'first')
        return outDF["资金账号"].tolist()


    # 获取指定日前的时间
    def getDealBeginTime(self,dataTime):
        T = dataTime * 60 * 60 * 24
        beginTime = time.strftime('%Y/%m/%d', time.localtime(time.time() - T))
        return beginTime


    def dealACSMData(self,account, outSYB, data_dan, data_zu):
        # 获取dataTime前的时间
        beginTime = self.getDealBeginTime(self.dataTime)
        # 处理单票数据
        needDanData = data_dan[(data_dan['交易日期'] > beginTime) & (data_dan['资金账号'] == account)]
        # 选取需要的字段
        needDanData_filed = needDanData[self.dan_index]
        # 写入这个账户的单票数据到输出df中
        outSYB = self.addAMountOfDate(outSYB, needDanData_filed, 0)
        outSYB = outSYB.reset_index(drop=True)

        # 处理组合数据
        needZuDate = data_zu[(data_zu['交易日期'] > beginTime) &
                             (data_zu['资金账号'] == account)]
        # 选取需要的字段
        needZuDate_filed = needZuDate[self.dan_index]
        # 写入这个账户的组合数据到输出df中
        if needZuDate_filed.empty:
            outSYB=outSYB
        else:
            outSYB = self.addAMountOfDate(outSYB, needZuDate_filed, 1)
        # print(outSYB)
            outSYB = outSYB.reset_index(drop=True)
        return outSYB


    def mkdir(self,path):
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

    def get2float(self,src):
        return float('%.2f' % src)

    def covAFile(self,oldFile, newFile):
        wb = openpyxl.load_workbook(oldFile)
        ws = wb.worksheets[0]

        rows = []
        for row in ws.iter_rows():
            rows.append(row)
        # 列数
        leni = len(rows[0])
        # 行数
        leny = len(rows)

        
        # 日期字段位置
        riqi = 2
        # 收益字段的位置
        shouyi = 5
        # 总收益的位置
        totle = 9
        # 股票名称位置
        sockname = 3
        # 当日交易额
        #vol = 5
        # 市值
        market = 4
        # 收益率
        shouyiret = 6
        #使用现金
        money=7
        #现金收益率
        moneyrate=8
        #股票级数
        yrate=10
        #平仓情况
        pingc=11

        cunname = ''
        nextname = ''

        for y in range(leni):
            for i in range(leny):
                # 处理表头
                # print(i,y)
                if i == 0 and y == shouyi:
                    rows[i][y].value = 'Alpha-T当天扣税费后收益'
                if i == 0 and y == totle:
                    rows[i][y].value = 'Alpha-T扣税费后累计收益'
                if i == 0 and y == market:
                    rows[i][y].value = '做Alpha-T市值'

                # 处理表头
                if i == 0:
                    ws.row_dimensions[i+1].height=33
                    font = Font('微软雅黑', bold=True, color='FFFFFF')
                    rows[i][y].font = font
                    rows[i][y].fill = openpyxl.styles.fills.GradientFill(stop=['000000', '000000'])
                    if y == shouyi or y == totle or y==moneyrate or y==6:
                        font = Font('微软雅黑', bold=True, color='FFFF00')
                        rows[i][y].font = font
                        
                elif y == 0:
                    font = Font('微软雅黑', color='000000')
                    rows[i][y].font = font

                elif y==money:
                    font = Font('微软雅黑', color='000000')
                    rows[i][y].font = font
                    if rows[i][money].value == "用户授权资金":  
                        font = Font('微软雅黑', bold=True, color='FFFFFF')
                        rows[i][money].font = font
                        #print(i, y, rows[i][shouyiret].value)
                 
       
                elif rows[i][2].value == time.strftime('%Y/%m/%d', time.localtime(time.time())):    
                        #print(rows[i][1].value)
                        font = Font('微软雅黑', color='0000FF')
                        rows[i][0].font = font
                        rows[i][1].font = font
                        rows[i][2].font = font
                        rows[i][3].font = font   
                        rows[i][10].font = font
                        rows[i][11].font = font
                        rows[i][12].font = font
                        rows[i][13].font = font
                        font = Font('微软雅黑', bold=True, color='FF0000')
                        rows[i][4].font = font
                        rows[i][7].font = font                 
                        rows[i][9].font = font
                        if rows[i][shouyiret].value != None and rows[i][shouyiret].value < 0:
                            font = Font('微软雅黑', bold=True, color='000000')                           
                            rows[i][5].font = font
                            rows[i][6].font = font                            
                            rows[i][8].font = font                            
                        else:
                            font = Font('微软雅黑', bold=True, color='FF0000')                           
                            rows[i][5].font = font
                            rows[i][6].font = font                            
                            rows[i][8].font = font
                        
                        #rows[i][10].font = font           
                        
                elif y==moneyrate:
                    if rows[i][moneyrate].value == "现金收益率%":
                        font = Font('微软雅黑', bold=True, color='FFFF00')
                        rows[i][moneyrate].font = font
                        #print(i, y, rows[i][shouyiret].value)
                    else:
                        font = Font('微软雅黑',color='000000')
                        rows[i][moneyrate].font = font
                        
                # 处理当日收益
                elif y == shouyi:
                    # 处理这一列数据
                    # rint(i,y)
                    font = Font('微软雅黑', color='000000')
                    rows[i][y].font = font

                    if rows[i][y].value == "当天收益":
                        ws.row_dimensions[i+1].height=33
                        ws.column_dimensions["F"].width=9
                        ws.column_dimensions["H"].width=13
                        ws.column_dimensions["D"].width=15
                        ws.column_dimensions["G"].width=13
                        ws.column_dimensions["A"].width=7
                        ws.column_dimensions["B"].width=12
                        ws.column_dimensions["C"].width=9
                        ws.column_dimensions["E"].width=23
                        ws.column_dimensions["I"].width=23
                        ws.column_dimensions["L"].width=14
                        ws.column_dimensions["M"].width=14
                       # ws.column_dimensions["J"].width=18
                        rows[i][shouyi].value = 'Alpha-T当天扣税费后收益'
                        rows[i][totle].value = 'Alpha-T扣税费后累计收益'
                        rows[i][market].value = '做Alpha-T市值'
                        for x in range(leni):
                            font = Font('微软雅黑', bold=True, color='FFFFFF')
                            rows[i][x].font = font
                            rows[i][x].fill = openpyxl.styles.fills.GradientFill(stop=['000000', '000000'])
                            font = Font('微软雅黑', bold=True, color='FFFF00')
                            rows[i][shouyi].font = font
                            rows[i][totle].font = font
                            rows[i][moneyrate].font = font  
                            #rows[i][yrate].font = font 
                            
                    elif rows[i][y].value != None and rows[i][y].value >= 0:
                        font = Font('微软雅黑', color='FF0000')
                        rows[i][y].font = font 
                    else:
                        font = Font('微软雅黑', color='000000')
                        rows[i][y].font = font
                        
                # 处理累计收益
                elif y == totle:
                    # 默认值
                    font = Font('微软雅黑', color='000000')
                    rows[i][y].font = font

                    # 没有交易的股票对期
                    if cunname == '' and i < leny - 1:
                        cunname = rows[i][sockname].value
                        nextname = rows[i + 1][sockname].value

                    elif rows[i][y].value != "Alpha-T扣税费后累计收益" and i < leny - 1:
                        cunname = rows[i][sockname].value
                        nextname = rows[i + 1][sockname].value
                        
                    elif rows[i][y].value == "Alpha-T扣税费后累计收益":
                        font = Font('微软雅黑', bold=True, color='FFFF00')
                        rows[i][y].font = font  
                        
                elif y == shouyiret:
                    # print(i,y)
                    font = Font('微软雅黑', color='000000')
                    rows[i][y].font = font
                    # 如果收益率大于0.35加粗标红
                    # print(i,y,rows[i][shouyiret].value)
                    if rows[i][shouyiret].value == "收益率%":

                        font = Font('微软雅黑', bold=True, color='FFFFFF')
                        rows[i][shouyiret].font = font
                        #print(i, y, rows[i][shouyiret].value)

                    elif rows[i][shouyiret].value != None and rows[i][shouyiret].value >= 0.35:
                        font = Font('微软雅黑', bold=True, color='FF0000')
                        rows[i][shouyiret].font = font
                        rows[i][shouyi].font = font
                        rows[i][moneyrate].font = font
                        
                    elif rows[i][shouyiret].value != None and rows[i][shouyiret].value < 0:
                        font = Font('微软雅黑',color='000000')
                        rows[i][shouyiret].font = font
                        rows[i][shouyi].font = font
                        rows[i][moneyrate].font = font
                        #rows[i][yrate].font = font 
                        
                elif y==10:
                    font = Font('微软雅黑', color='000000')
                    rows[i][y].font = font
                    if rows[i][10].value == "股票级数":  
                        font = Font('微软雅黑', bold=True, color='FFFFFF')
                        rows[i][10].font = font
                        
                elif y==12:
                    font = Font('微软雅黑', color='000000')
                    rows[i][y].font = font
                    if rows[i][12].value == "连续亏损天数":  
                        font = Font('微软雅黑', bold=True, color='FFFFFF')
                        rows[i][12].font = font
                
                elif y==13:
                    font = Font('微软雅黑', color='000000')
                    rows[i][y].font = font
                    if rows[i][13].value == "连续盈利天数":  
                        font = Font('微软雅黑', bold=True, color='FFFFFF')
                        rows[i][13].font = font
                        
                elif y==11:
                    font = Font('微软雅黑', color='000000')
                    rows[i][y].font = font
                    if rows[i][11].value == "平仓":  
                        font = Font('微软雅黑', bold=True, color='FFFFFF')
                        rows[i][11].font = font
   
                else:
                    font = Font('微软雅黑', color='000000')
                    rows[i][y].font = font


        wb.save(newFile)


    # 主函数
    def process_main(self,data_dan, data_zu):
        # 获取今天要处理的营业部列表
        YYBList = self.getYYBNameList(data_zu)
        # 得到每个客户的数据写入一个结果文件,将这个数据存入变量中
        outDict = {}
        for each in YYBList:

            # 获得该营业部需要处理的用户列表
            YYBCTMList = self.getYYBcustomerList(data_zu, each)
            # 创建一个空dataFram
            outSYB = self.creatOutData()
            for account in YYBCTMList:
                outSYB = self.dealACSMData(account, outSYB, data_dan, data_zu)
            # 将得到的收益结果dataframe写会结果字典中
            # print(outSYB)
            if outSYB.empty:
                True
            else:
                outDict[each] = outSYB.ix[0:len(outSYB) - 2, :]
           
         
        # 创建今天的运行结果目录
        # 获取今天的日期
        # today = time.strftime('%Y-%m-%d', time.localtime(time.time()))
        nowPath = self.rootPath + "/" + self.today + "/" + "营业部数据"
        self.mkdir(nowPath)
        # 循环创建各营业部的目录
        YYBList = list(outDict.keys())
        for each in YYBList:
            print(each)
            # 创建对应营业部的文件夹
            YYBPath = nowPath + "/" + each
            self.mkdir(YYBPath)
            # 将该营业部的文件放入目标目录
            strTemp = self.today +"原始"+ '_' + each + '_' + '收益汇总表' + ".xlsx"
            fileName = YYBPath + "/" + strTemp
            #print(fileName)
            outDict[each].to_excel(fileName, encoding='utf-8', index=False)
            self.covAFile(fileName, fileName)
        
      # 函数： 将各营业部交割记录导出  
      

                         #print(oldfilelist)
                

                 
                 
                 
                 
                 
                 
                 
                
         
                
        
        
        
        
        
        
        
        
        
# print(data_zu)

