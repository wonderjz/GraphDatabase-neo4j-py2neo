# -*- coding: utf-8 -*-
"""
Created on Fri Jul  7 11:42:54 2023

@author: Administrator

DBMS 3  -- backup from 6.11 DBMS 9

"""
from py2neo import Graph, Node, Relationship,Subgraph
from py2neo import NodeMatcher
from py2neo.matching import *
import pymysql

from tqdm import tqdm
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
from datetime import datetime as dt
import datetime
import time
import gc

config = {
	# 数据库连接信息，详细在数据库查看
	'host': '11.11.11.111',
	'port': 10005,  # 端口
	'user': 'xxx',  # 用户名
	'password': 'xxxxx' # 密码
# 	'db': ...,  # 数据库名
	
# 	'charset': 'utf8mb4',
# 	'cursorclass': pymysql.cursors.DictCursor,
	}

graph1 = Graph('neo4j://localhost:11007', auth = ('neo4j', '12345678'))


# %%

def RunSql(sql, isSelect = True):
    # 创建连接
    con = pymysql.connect(**config)
    con.ping(reconnect=True)
    cursor = con.cursor()
    cursor.execute(sql)
    if isSelect is True:  # select语句需要返回结果
        result = cursor.fetchall()
        print("selectCount: " + str(cursor.rowcount))   # 打印受影响行数
        cursor.close()
        con.close()
        return result
    else:  # insert、update语句等 无需返回结果
        print("affectCount: " + str(cursor.rowcount))   # 打印受影响行数
        con.commit()
        cursor.close()
        con.close()

def get_every_day(begin_date_str, end_date_str):
    # 获取每天的日期字符串
    # 参数1：begin_date_str，开始日期字符串，例如：2020-01-01
    # 参数2：end_date_str，结束日期字符串，例如：2020-08-10
    date_list = []   
    begin_date = datetime.datetime.strptime(begin_date_str, "%Y-%m-%d")    
    end_date = datetime.datetime.strptime(end_date_str, "%Y-%m-%d")  
    while begin_date <= end_date:
        date_str = begin_date.strftime("%Y-%m-%d")
        date_list.append(date_str)  
        begin_date += datetime.timedelta(days=1)
    return date_list


def changecol(x):
    if str(x) != 'nan':
        x = x.split(',')
        x = [int(i) for i in x]
    elif str(x) == 'nan':
        x = 'nan'
        print(x)
    return x


def changeintcol(x):
    y = []
    y.append(x)
    return y

'''
count = 0
for i in tqdm(range(len(node_list4push))):
    thenode = graph1.nodes.match("Borrower", mobile=node_list4push[i]['mobile']).first()
    
    newlabel = node_list4push[i]['label']
    if  thenode == None:
        print(f"ERROR__ {i}th Node {node_list4push[i]} is not in DB.")
        # thenode = node_list4push[i]
        # graph1.push(thenode)
        continue
    
    elif  thenode["label"] == 0 | thenode['label'] == None:
        # 防止thenode == None
        previouslabel = thenode['label']
        
        thenode.update({'label': node_list4push[i]['label'] }) 
        graph1.push(thenode)
        # check
        thenodenew = graph1.nodes.match("Borrower", mobile=thenode['mobile']).first()
        if thenodenew['label'] != node_list4push[i]['label']:
            print(f"setERROR__ auditstatus of {thenode['mobile']} as {node_list4push[i]['label']}")
    elif thenode["label"] == 1: 
        count += 1
        #     thenode['label'] = newlabel  #//1
        # graph1.push(thenode)     
    else:
        print(thenode)
        print(f"ERROR__ {i}th Node {node_list4push[i]}.")
        continue
    
countupdate =  len(node_list4push) -  count  
 '''   
    
def propertyupdate(node_list4push,propname):
    count = 0
    count1 = 0
    matcher1 = NodeMatcher(graph1)
    for i in tqdm(range(len(node_list4push))):
        thenode = matcher1.match("Borrower", mobile=node_list4push[i]['mobile']).first()
        # thenode = graph1.nodes.match("Borrower", mobile=node_list4push[i]['mobile']).first()
        newlabel = node_list4push[i][propname]
        if  thenode == None:
            print(f"ERROR__ {i}th Node {node_list4push[i]} is not in DB.")
            # thenode = node_list4push[i]
            # graph1.push(thenode)
            continue
        
        # elif  (thenode[propname] == 0 | thenode[propname] == None):
        elif  thenode[propname] == 0:            
            # 防止thenode == None
            thenode.update({propname: newlabel })
            # print(f" thenode property has changed to {thenode[propname]}")
            graph1.push(thenode)
            # check
            thenodenew = graph1.nodes.match("Borrower", mobile=thenode['mobile']).first()
            if thenodenew[propname] != node_list4push[i][propname]:
                print(f"setERROR__ status of {thenode['mobile']} as {node_list4push[i][propname]}")
            count1 = count1+ 1
            continue
        
        elif thenode[propname] == 1: 
            count = count + 1
            continue
            #     thenode['label'] = newlabel  #//1
            # graph1.push(thenode)     
        elif thenode[propname] >1:
            print(thenode[propname])
            print(thenode)
            print(f"ERROR__ {i}th Node {node_list4push[i]}.")

    countupdate =  count1
    print(f"update {countupdate} property of nodes with {count} property without changes.")   
    return countupdate




def pushnodes_testrule(node_list4push,rule00_nodelist,rule01_nodelist):
    ck_time = []
    s_timeall = time.time() 
    for i in tqdm(range(len(node_list4push))):
        
        s_time = time.time() # 使用tqdm之后  不会打印每次的time    
        # if i == 1:
        #     graph1.run('CREATE CONSTRAINT FOR  (n:Borrower) REQUIRE n.mobile IS UNIQUE') # V5
        
        matcher1 = NodeMatcher(graph1)
        thenode = node_list4push[i]
        themobile = thenode['mobile']
        
    
        if (themobile in thenode['relationmobilelist']):
            thenode['relationmobilelist'].remove(themobile)
        premoblist = thenode['relationmobilelist']
        premoborderno = thenode['orderno']
        # preauditstatuslist = thenode['auditstatuslist']
        # prestatuslist = thenode['statuslist']
        prestatus56 = thenode['status56']    
        preliuliangclass = thenode['liuliangclass']
        precreatetime = thenode['createtime']
        
        if (str(thenode['label']) == 'nan'):
            thenode['label'] = 0
        elif (thenode['label'] == 1.0):
            thenode['label'] = int(thenode['label'])
        prelabel = thenode['label']
    
    
        node_test = matcher1.match("Borrower", mobile = thenode['mobile']).all() #node1 can be a list of Node
        # try:
        if len(node_test) ==0:
            graph1.create(thenode)
            node_test1 = matcher1.match("Borrower", mobile = thenode['mobile']).all() #node1 can be a list of Node
            if len(node_test1) ==0:
                print("\n")
                print(f"create {i} th node error>>>>>>>>>>",end="\n")           
            e_time = time.time()
            ck_time.append((e_time-s_time))
    
        elif len(node_test) ==1:
            previousmoblist = node_test[0]['relationmobilelist']
            
            previousorderno = node_test[0]['orderno']
            # previousauditstatuslist = node_test[0]['auditstatuslist']
            # previousstatuslist = node_test[0]['statuslist']
            previousliuliangclass = node_test[0]['liuliangclass']
            previousstatus56 = node_test[0]['status56']
            previouscreatetime = node_test[0]['createtime']
            previouslabel = node_test[0]['label']
            newmoblist = list(set(premoblist) | set(previousmoblist))
            
            if previousorderno== None:
                neworderno =  ['nan']+premoborderno
            else:
                neworderno =  previousorderno+premoborderno
            
            if previousstatus56 == 1:
                newstatus56 = 1
            else:
                newstatus56 = prestatus56
            
            if previouscreatetime <= precreatetime:
                newcreatetime = previouscreatetime
            else:
                newcreatetime = precreatetime
            
            if previouslabel == 0:
                newlabel = prelabel
            
            
            if previousliuliangclass== [None]:
                if preliuliangclass ==[None]:
                    newliuliangclass= ['nan']
                else:
                    newliuliangclass =  ['nan'] + preliuliangclass
            else:
                if preliuliangclass ==[None]:
                    newliuliangclass = previousliuliangclass
                else:
                    newliuliangclass =  previousliuliangclass + preliuliangclass
            
            # node_test[0]['statuslist'] = newstatuslist 
            node_test[0]['status56'] = newstatus56  
            # node_test[0]['auditstatuslist'] = newauditstatuslist
            node_test[0]['relationmobilelist'] = newmoblist
            node_test[0]['orderno'] = neworderno # xiugai
            node_test[0]['liuliangclass'] = newliuliangclass # xiugai
            node_test[0]['createtime'] = newcreatetime
            node_test[0]['label'] = newlabel
            
            graph1.push(node_test[0])
            # matcher2?? need?
            node_testpush = matcher1.match("Borrower", mobile = thenode['mobile']).all() #node1 can be a list of Node
            if not (node_testpush[0]['relationmobilelist'] == newmoblist) & (node_testpush[0]['orderno'] == neworderno):
                print(f"push {i} th node error",end="\n")
        elif len(node_test) >= 2:
            print("\n")
            print(f"#####Error {i} th node repeate {len(node_test)} times>>>>>>>>>>",end="\n")
    
        
        node_test1 = matcher1.match("Borrower", mobile = thenode['mobile']).first() #node1 can be a list of Node
    
        # ???
        # matcher1 = NodeMatcher(graph1)        
        # thenode 的 联系人 曾经出现过 thenode['relationmobilelist']=[-999]/[23345]/[12234,3435] 
        for k in range(len(thenode['relationmobilelist'])):
            relamobile = thenode['relationmobilelist'][k]
            node1 = matcher1.match("Borrower", mobile = relamobile).all() #node1 can be a list of Node
            if len(node1) ==0: # node1 is a list (can be null)
                continue
            elif len(node1) ==1:
                # print("\n")
                # print(f"The {i}th  Node has contact in mobilelist person {k} before",end='\n')
                # if node_test1['orderno'] != node1[0]['orderno']: #防止自己连接自己 在数据库加constrain实现
                graph1.create(Relationship(node_test1, "Contactp", node1[0])) #thenode
                if (node_test1['mobile'] in node_test1['relationmobilelist']):
                    node_test1['relationmobilelist'].remove(node_test1['mobile'])
                
                # 验证 创建成功！！！！！》》》》》
            elif len(node1) ==2:
                print("\n")
                print(f"#####ERROR__ The {i}th  Node contactperson {k} appears 2 before>>>>>>>>>",end='\n')
                # 比较一下 orderno 和 relationmobilelist 然后删除 少的那个
            else:
                print(f"#####ERROR__ The {i}th  Node contactperson {k} appears {len(node1)} before>>>>>>>>>",end='\n')
        # thenode 作为 其他人  的联系人
        # matcher1 = NodeMatcher(graph1) !!!
        # data1 = matcher1.match("Borrower", relationmobilelist=CONTAINS(themobile)).all() # V4
        data1 = graph1.run('MATCH(n) WHERE '+str(themobile)+' IN n.relationmobilelist RETURN n').data() #V5
        
        if (type(data1) == list) & (len(data1) >=1):
            # print("\n")
            # print(f"The {i}th  Node is others' contact",end="\n")
            for m in range(len(data1)):
                # ofrelanode = data1[m] # V4
                ofrelanode = data1[m]['n'] # V5
                # print(ofrelanode.identity)  #   待修改
                # if node_test1['orderno'] != ofrelanode[0]['orderno']: #防止自己连接自己
                R_create = Relationship(ofrelanode, "Contactp", node_test1) # thenode
                graph1.create(R_create)
                if (node_test1['mobile'] in node_test1['relationmobilelist']):
                    node_test1['relationmobilelist'].remove(node_test1['mobile'])
                
                    # graph1.push(R_create) # ???
                # graph.exists(R_create)
                # 验证  》》》》》》》》》》》》》》》
        elif type(data1) == float:            
            if str(data1) == 'nan':
                continue
            else:
                print("\n")
                print(f"#####Error 2 in {i}th mobilelist {data1} >>>>>>>")
                
        
        #测试几个规则 》》》》》》》》》》
        
        # rule 0: group numbers * yuqi numbers within the group
        thenoder1n = graph1.run('MATCH (n {'+'mobile: + {}'.format(themobile)+'}) CALL apoc.path.subgraphNodes(n, {maxDepth: -1}) YIELD node WHERE node.mobile<>' +'{} RETURN sum(node.label),count(node)'.format(themobile)).data()
        rule0_numingroup = thenoder1n[0]['count(node)']
        rule0_yuqiingroup = thenoder1n[0]['sum(node.label)']
        # addlist_rule0 = [themobile,thenode['label'],rule0_numingroup,rule0_yuqiingroup]
        if (rule0_numingroup >0) & (rule0_yuqiingroup>0):
            addlist_rule0 = [themobile,thenode['createtime'],thenode['label'],rule0_numingroup,rule0_yuqiingroup]
            rule00_nodelist.append(addlist_rule0)
            
        
        thenoder1n = graph1.run('MATCH (n {'+'mobile: + {}'.format(themobile)+'}) CALL apoc.path.subgraphAll(n, {maxDepth: -1}) YIELD nodes,relationships  RETURN size(nodes),size(relationships)').data()
        rule0_groupsize = thenoder1n[0]['size(nodes)']
        rule0_relationsize = thenoder1n[0]['size(relationships)']
        # addlist_rule0 = [themobile,thenode['label'],rule0_numingroup,rule0_yuqiingroup]
        if (rule0_groupsize >0) & (rule0_relationsize>0):
            addlist_rule0 = [themobile,thenode['createtime'],thenode['label'],rule0_groupsize,rule0_relationsize]
            rule01_nodelist.append(addlist_rule0)            
        
        # # rule3： 兄弟节点逾期或未通过系统审核
        # thenoder1n = graph1.run(f"MATCH (n)<-[r1:Contactp]-(n1) WHERE n.mobile={themobile} RETURN n1").data()
        # if thenoder1n == None:
        # #        print(f"##### Error in Mobile [themobile]")
        #     continue
        # countrule3 = 0
        # for j in range(len(thenoder1n)):
        #     fathermobile =  thenoder1n[j]['n1']['mobile']       
        #     thenodebro = graph1.run(f"MATCH (n2)<-[r:Contactp]-(n1) WHERE n1.mobile={fathermobile} RETURN n2").data()
        #     if thenodebro == None:
        #         continue
        #     else:
        #         for k in range(len(thenodebro)):
        #             # if thenodebro[k]['n2']['label'] == None:
        #             #     continue
                    
        #             # if ((thenodebro[k]['n2']['label'] >= 1) | (thenodebro[k]['n2']['auditstatus'] == -1)) & (thenodebro[k]['n2']['mobile'] != themobile):
        #             if (thenodebro[k]['n2']['label'] == 1)  & (thenodebro[k]['n2']['mobile'] != themobile):
    
        #                 countrule3 +=1
                    
        # if countrule3 >= 1:  # >= 2
        #     addlist_rule3 = [thenode['mobile'],thenode['createtime'],countrule3]
        #     rule3_nodelist4yuqi.append(addlist_rule3)  
        
        
        # # rule1： 和thenode节点有双向关系的节点 逾期或未通过审核
        # thenoder1n = graph1.run(f"MATCH (n1)-[r1:Contactp]->(n2) WHERE n1.mobile={themobile} RETURN n2").data()
        # if thenoder1n == None:
        #     continue
        # countrule1 = 0
        # for j in range(len(thenoder1n)):
        #     # if thenoder1n[j]['n2']['label'] == None:
        #     #     continue
        #     # if ((thenoder1n[j]['n2']['label'] >= 1) | (thenoder1n[j]['n2']['auditstatus'] == -1)) &  (thenoder1n[j]['n2']['mobile'] != themobile):
        #     if (thenoder1n[j]['n2']['label'] == 1)  &  (thenoder1n[j]['n2']['mobile'] != themobile):
    
        #         rule1mobilelist = thenoder1n[j]['n2']['relationmobilelist']
        #         if themobile in rule1mobilelist:
        #             countrule1 += 1
    
        #     if j == (len(thenoder1n)-1):
        #         if countrule1 >=1:
        #             addlistrule1 = [thenode['mobile'],thenode['createtime'],countrule1]
        #             rule1_nodelist4yuqi.append(addlistrule1)
    
    
        # # 对 auditstatus == -1 之外的 采用 rule based on 逾期&审核拒绝
        # # rule2: 一级或两级联系人中有逾期的
    
        # thenoder1n = graph1.run(f"MATCH (n1)<-[r1:Contactp]-(n2) WHERE n1.mobile={themobile} RETURN n2").data()
        # count1_rule2 = 0
        # count2_rule2 = 0
        # if thenoder1n == None:
        #     continue
        # else:
        #     for j in range(len(thenoder1n)):
        #         if (thenoder1n[j]['n2']['label'] == 1): # | (thenoder1n[j]['n2']['auditstatus'] == -1):
        #             count1_rule2 += 1
        #     thenoder2n = graph1.run(f"MATCH (n1)<-[r1:Contactp]-(n2)<-[r2:Contactp]-(n3) WHERE n1.mobile={themobile} RETURN n3").data()
        #     if thenoder2n == None:
        #         continue
        #     for k in range(len(thenoder2n)):
        #         # if ((thenoder2n[k]['n3']['label'] >= 1) | (thenoder2n[k]['n3']['auditstatus'] == -1))  & (thenoder2n[k]['n3']['mobile'] != themobile):
        #         if (thenoder2n[k]['n3']['label'] == 1)  & (thenoder2n[k]['n3']['mobile'] != themobile):
    
        #             count2_rule2 += 1
        #     if (count1_rule2 + count2_rule2) >= 2:
        #         addlist_rule2 = [thenode['mobile'],thenode['createtime'],count1_rule2,count2_rule2]
        #         rule2_nodelist4yuqi.append(addlist_rule2)
    
        
        
        # tx.commit()
        e_time = time.time()    
        # print("The {}th  Node done in {}  seconds".format(i,(e_time-s_time)))
        ck_time.append((e_time-s_time))

    e_timeall = time.time()   
    print("The {} Nodes done in {}  seconds ___ {} mins".format(i,e_timeall-s_timeall,(e_timeall-s_timeall)/60))

    return rule00_nodelist,rule01_nodelist
               

def pushnodes(node_list4push):
    ck_time = []
    s_timeall = time.time() 
    for i in tqdm(range(len(node_list4push))):
        
        s_time = time.time() # 使用tqdm之后  不会打印每次的time    
        # if i == 1:
        #     graph1.run('CREATE CONSTRAINT FOR  (n:Borrower) REQUIRE n.mobile IS UNIQUE') # V5
        
        matcher1 = NodeMatcher(graph1)
        thenode = node_list4push[i]
        themobile = thenode['mobile']
        
    
        if (themobile in thenode['relationmobilelist']):
            thenode['relationmobilelist'].remove(themobile)
        premoblist = thenode['relationmobilelist']
        premoborderno = thenode['orderno']
        # preauditstatuslist = thenode['auditstatuslist']
        # prestatuslist = thenode['statuslist']
        prestatus56 = thenode['status56']    
        preliuliangclass = thenode['liuliangclass']
        precreatetime = thenode['createtime']
        
        if (str(thenode['label']) == 'nan'):
            thenode['label'] = 0
        elif (thenode['label'] == 1.0):
            thenode['label'] = int(thenode['label'])
        prelabel = thenode['label']
    
    
        node_test = matcher1.match("Borrower", mobile = thenode['mobile']).all() #node1 can be a list of Node
        # try:
        if len(node_test) ==0:
            graph1.create(thenode)
            node_test1 = matcher1.match("Borrower", mobile = thenode['mobile']).all() #node1 can be a list of Node
            if len(node_test1) ==0:
                print("\n")
                print(f"create {i} th node error>>>>>>>>>>",end="\n")           
            e_time = time.time()
            ck_time.append((e_time-s_time))
    
        elif len(node_test) ==1:
            previousmoblist = node_test[0]['relationmobilelist']
            
            previousorderno = node_test[0]['orderno']
            # previousauditstatuslist = node_test[0]['auditstatuslist']
            # previousstatuslist = node_test[0]['statuslist']
            previousliuliangclass = node_test[0]['liuliangclass']
            previousstatus56 = node_test[0]['status56']
            previouscreatetime = node_test[0]['createtime']
            previouslabel = node_test[0]['label']
            newmoblist = list(set(premoblist) | set(previousmoblist))
            
            if previousorderno== None:
                neworderno =  ['nan']+premoborderno
            else:
                neworderno =  previousorderno+premoborderno
            
            if previousstatus56 == 1:
                newstatus56 = 1
            else:
                newstatus56 = prestatus56
            
            if previouscreatetime <= precreatetime:
                newcreatetime = previouscreatetime
            else:
                newcreatetime = precreatetime
            
            if previouslabel == 0:
                newlabel = prelabel
            
            
            if previousliuliangclass== [None]:
                if preliuliangclass ==[None]:
                    newliuliangclass= ['nan']
                else:
                    newliuliangclass =  ['nan'] + preliuliangclass
            else:
                if preliuliangclass ==[None]:
                    newliuliangclass = previousliuliangclass
                else:
                    newliuliangclass =  previousliuliangclass + preliuliangclass
            
            # node_test[0]['statuslist'] = newstatuslist 
            node_test[0]['status56'] = newstatus56  
            # node_test[0]['auditstatuslist'] = newauditstatuslist
            node_test[0]['relationmobilelist'] = newmoblist
            node_test[0]['orderno'] = neworderno # xiugai
            node_test[0]['liuliangclass'] = newliuliangclass # xiugai
            node_test[0]['createtime'] = newcreatetime
            node_test[0]['label'] = newlabel
            
            graph1.push(node_test[0])
            # matcher2?? need?
            node_testpush = matcher1.match("Borrower", mobile = thenode['mobile']).all() #node1 can be a list of Node
            if not (node_testpush[0]['relationmobilelist'] == newmoblist) & (node_testpush[0]['orderno'] == neworderno):
                print(f"push {i} th node error",end="\n")
        elif len(node_test) >= 2:
            print("\n")
            print(f"#####Error {i} th node repeate {len(node_test)} times>>>>>>>>>>",end="\n")
    
        
        node_test1 = matcher1.match("Borrower", mobile = thenode['mobile']).first() #node1 can be a list of Node
    
        # ???
        # matcher1 = NodeMatcher(graph1)        
        # thenode 的 联系人 曾经出现过 thenode['relationmobilelist']=[-999]/[23345]/[12234,3435] 
        for k in range(len(thenode['relationmobilelist'])):
            relamobile = thenode['relationmobilelist'][k]
            node1 = matcher1.match("Borrower", mobile = relamobile).all() #node1 can be a list of Node
            if len(node1) ==0: # node1 is a list (can be null)
                continue
            elif len(node1) ==1:
                # print("\n")
                # print(f"The {i}th  Node has contact in mobilelist person {k} before",end='\n')
                # if node_test1['orderno'] != node1[0]['orderno']: #防止自己连接自己 在数据库加constrain实现
                graph1.create(Relationship(node_test1, "Contactp", node1[0])) #thenode
                if (node_test1['mobile'] in node_test1['relationmobilelist']):
                    node_test1['relationmobilelist'].remove(node_test1['mobile'])
                
                # 验证 创建成功！！！！！》》》》》
            elif len(node1) ==2:
                print("\n")
                print(f"#####ERROR__ The {i}th  Node contactperson {k} appears 2 before>>>>>>>>>",end='\n')
                # 比较一下 orderno 和 relationmobilelist 然后删除 少的那个
            else:
                print(f"#####ERROR__ The {i}th  Node contactperson {k} appears {len(node1)} before>>>>>>>>>",end='\n')
        # thenode 作为 其他人  的联系人
        # matcher1 = NodeMatcher(graph1) !!!
        # data1 = matcher1.match("Borrower", relationmobilelist=CONTAINS(themobile)).all() # V4
        data1 = graph1.run('MATCH(n) WHERE '+str(themobile)+' IN n.relationmobilelist RETURN n').data() #V5
        
        if (type(data1) == list) & (len(data1) >=1):
            # print("\n")
            # print(f"The {i}th  Node is others' contact",end="\n")
            for m in range(len(data1)):
                # ofrelanode = data1[m] # V4
                ofrelanode = data1[m]['n'] # V5
                # print(ofrelanode.identity)  #   待修改
                # if node_test1['orderno'] != ofrelanode[0]['orderno']: #防止自己连接自己
                R_create = Relationship(ofrelanode, "Contactp", node_test1) # thenode
                graph1.create(R_create)
                if (node_test1['mobile'] in node_test1['relationmobilelist']):
                    node_test1['relationmobilelist'].remove(node_test1['mobile'])
                
                    # graph1.push(R_create) # ???
                # graph.exists(R_create)
                # 验证  》》》》》》》》》》》》》》》
        elif type(data1) == float:            
            if str(data1) == 'nan':
                continue
            else:
                print("\n")
                print(f"#####Error 2 in {i}th mobilelist {data1} >>>>>>>")
        
        # tx.commit()
        e_time = time.time()    
        # print("The {}th  Node done in {}  seconds".format(i,(e_time-s_time)))
        ck_time.append((e_time-s_time))

    e_timeall = time.time()   
    print("The {} Nodes done in {}  seconds ___ {} mins".format(i,e_timeall-s_timeall,(e_timeall-s_timeall)/60))

    return node_list4push
 
#for i in range(len(querydate)):  
    
def updatewholedb(querydate,i):
    
    thequerydate = querydate[i] 
    print(f"模拟 {thequerydate[1]} 凌晨0点更新")
    # thequerydate = ['2023-06-11', '2023-06-12']
    sql = f"SELECT\
        	tu.mobile_account,tb.bid_no,tb.create_time,\
            IFNULL(if(tbs.`status` in(3,4) AND tbs.repayment_time>= '{thequerydate[0]}' AND tbs.repayment_time < '{thequerydate[1]}',1,0),0) as label,\
            IFNULL(if(tb.`status` in(5,6),1,0),0) as is_loan\
            FROM\
            	indonesia_loan.t_loan_bid tb\
                INNER JOIN indonesia_loan.t_loan_user tu ON tu.id = tb.user_id\
                LEFT JOIN indonesia_loan.t_loan_bill tbs on tbs.bid_no = tb.bid_no AND tbs.period =1\
            WHERE \
                tb.create_time < '{thequerydate[1]}'\
                HAVING label = 1\
                ORDER BY tu.mobile_account\
            "
    s_timeall = time.time()
    result = RunSql(sql)
    e_timeall = time.time()          
    print("done in {}  seconds ___ {} mins".format(e_timeall-s_timeall,(e_timeall-s_timeall)/60))
    
    ck_resultdf = pd.DataFrame(result)
    resultdf1 = ck_resultdf.rename({0:'mobile',3:'label',4:'status56'},axis='columns')
    resultdf1 = resultdf1[['mobile','label','status56']]
    resultdf1['mobile'] = resultdf1['mobile'].apply(lambda x: int(x) )
    dfdict = resultdf1.to_dict('records')
    node_list4push = [Node("Borrower",**dfdict[i]) for i in range(len(dfdict))]
    print(len(node_list4push))

    countupdate = propertyupdate(node_list4push,'label')
    print(f"countupdate {countupdate}  label")
    countupdate = propertyupdate(node_list4push,'status56')
    print(f"countupdate {countupdate}  status56")
    return node_list4push

# for i in range(len(querydate2)):
    
def updatedailynodes(querydate2,i):
    thequerydate = querydate2[i] 
    print(f" 模拟获取  {thequerydate[0]} 这一天的 待入库节点")
    # thequerydate = ['2023-06-11', '2023-06-12']
    sql2 = f"SELECT *from ( \
    SELECT \
    	tu.mobile_account, tb.bid_no,tb.create_time, \
        IFNULL(if(tb.`status` in(5,6),1,0),0) as '是否放款', \
        IFNULL(if(tbs.`status` in(3,4) AND tbs.repayment_time < '{thequerydate[1]}',1,0),0) as '是否逾期', \
        CASE    WHEN trs.tracker_name not LIKE '%Google Ads ACI%' and trs.tracker_name not LIKE 'Unattributed'   then '自然流量' \
				WHEN trs.tracker_name LIKE '%Google Ads ACI%' then '谷歌广告' \
				WHEN trs.tracker_name LIKE 'Unattributed' then 'facebook' \
				else trs.tracker_name end as '流量类型', \
        td.relation_mobile as 'relationmobilelist' \
    FROM \
    	indonesia_loan.t_loan_bid tb \
    INNER JOIN indonesia_risk.t_risk_audit_result tar ON CONVERT(tb.bid_no USING utf8)  = tar.order_no \
    INNER JOIN indonesia_risk.t_risk_data_record trd ON trd.order_no = tar.order_no \
    INNER JOIN indonesia_risk.t_risk_user_detail td ON td.user_id = tar.user_id \
    INNER JOIN indonesia_loan.t_loan_user tu ON tu.id = tb.user_id \
    LEFT JOIN indonesia_loan.t_loan_bill tbs ON tbs.bid_no = tb.bid_no AND tbs.period =1 \
    LEFT join indonesia_risk.t_risk_audit_result_support trs ON trs.order_no = tar.order_no and trs.serial_number = tar.last_serial_number AND trs.user_id = tar.user_id \
    WHERE \
    tb.create_time >= '{thequerydate[0]}' \
    AND tb.create_time < '{thequerydate[1]}' \
    ORDER BY td.id DESC LIMIT 10000000000000 \
    ) a GROUP BY a.bid_no\
    "
    s_timeall = time.time()
    result2 = RunSql(sql2)
    e_timeall = time.time()          
    print("done in {}  seconds ___ {} mins".format(e_timeall-s_timeall,(e_timeall-s_timeall)/60))

    resultdf = pd.DataFrame(result2)
    resultdf2 = resultdf.rename({0:'mobile',1:'orderno',2:'createtime',3:'status56',4:'label',5:'liuliangclass',6:'relationmobile'},axis='columns')
    resultdf2['mobile'] = resultdf2['mobile'].apply(lambda x: int(x) )
    resultdf2['relationmobilelist'] = resultdf2['relationmobile'].apply(changecol)
    resultdf2= resultdf2[resultdf2['relationmobilelist'] != 'nan']
    del resultdf2['relationmobile']
    gc.collect()
    resultdf2['orderno'] = resultdf2['orderno'].apply(lambda x: int(x))
    resultdf2['orderno'] = resultdf2['orderno'].apply(changeintcol)
    resultdf2['liuliangclass'] = resultdf2['liuliangclass'].apply(lambda x: 'nan' if str(x) == 'nan' else x)
    resultdf2['liuliangclass'] = resultdf2['liuliangclass'].apply(changeintcol)
    
    
    dfdict = resultdf2.to_dict('records')
    node_list4push = [Node("Borrower",**dfdict[i]) for i in range(len(dfdict))]
    return node_list4push,resultdf2

    
'''
def fscore(baselist, testlist, totalnum= len(passedmobile)):
    TP = len(set(baselist) & set(testlist))    
    TPFP = len(testlist)
    TPFN = len(baselist)
    if TPFP == 0:
        return 0
    FNTN = totalnum - TPFP
    FP = TPFP - TP
    FN = TPFN - TP
    TN = FNTN - FN
    
    FPR = FP/(FP+ TN)
    recall = TP/TPFN
    precision = TP/TPFP    # 判断与事实均为真/ 事实为真
    #fs = 2/(1/recall + 1/precision)
    # print("For "+str(baselist)+" and  "+str(testlist), end='\n' )
    MCC = (TP*TN-FP*TN)/np.sqrt((TP+FP)*(TP+FN)*(TN+FP)*(TN+FN)) # thershold 0.2 for covariate shift

    print(f"总样本数 {(totalnum)}  总逾期数 {len(baselist)}  选中样本数 {len(testlist)}:", end='\n')
    # print(f"For {fscore.__code__.co_varnames}", end = '\n')
    # print(f"precision(选中样本逾期率): {format(precision, '.3f')}  FPR: {format(FPR, '.3f')}   recall(TPR): {format(recall, '.3f')}    fscore: {format(fs, '.3f')} ", end = '\n')
    # print(f"") 选中比例
    print(f"选中样本中的逾期数: {TP}", end='\n')
    print(f"precision: {format(precision, '.3f')}", end = '\n')
    print("",end= '\n')
    return set(baselist) & set(testlist)
'''

# %%

# rule00_nodelist4yuqi = pd.read_excel("rule_results.xlsx",sheet_name = "rule00",)
# rule00_nodelist4yuqi = rule00_nodelist4yuqi.iloc[:,0].tolist()

# rule01_nodelist4yuqi = pd.read_excel("rule_results.xlsx",sheet_name = "rule01",)
# rule01_nodelist4yuqi = rule01_nodelist4yuqi.iloc[:,0].tolist()

# rule1_nodelist4yuqi = pd.read_excel("rule_results.xlsx",sheet_name = "rule1",)
# rule1_nodelist4yuqi = rule1_nodelist4yuqi.iloc[:,0].tolist()

# rule2_nodelist4yuqi = pd.read_excel("rule_results.xlsx",sheet_name = "rule2",)
# rule2_nodelist4yuqi = rule2_nodelist4yuqi.iloc[:,0].tolist()

# rule3_nodelist4yuqi = pd.read_excel("rule_results.xlsx",sheet_name = "rule3",)
# rule3_nodelist4yuqi = rule3_nodelist4yuqi.iloc[:,0].tolist()


# rule3_nodelist4yuqi = []
# rule2_nodelist4yuqi = []
# rule1_nodelist4yuqi = []
rule00_nodelistmy = []
rule01_nodelistmy = []

date_list = get_every_day('2023-06-10', '2023-07-11')  # 从 6-11 开始读入 直到 7-10

# for update whole db daily
querydate = [[date_list[i],date_list[i+1]] for i in range(len(date_list)) if i+1< len(date_list)]

# del node_list4push
querydate2 = [[date_list[i+1],date_list[i+2]] for i in range(len(date_list)) if i+2< len(date_list)]



# j = 4 It time for j = 5 跑下面的
# print(j)
# 假设今天是 2023-06-11 凌晨0点，先更新数据库 # 每天先更新 库里面存在的节点的可能逾期 情况
# 

resultdfpast = pd.DataFrame()

# (0,20)
for j in range(10,20):

# 更新 节点
    print(f"以下进行 {querydate2[j][0]} 的所有更新 >>>>>>>>>>>>>>>>>>>")
    
    node_list4pushdb = updatewholedb(querydate,j)
    
    node_list4pushdailynode,resultdfdaily = updatedailynodes(querydate2,j)
    
    resultdfpast = resultdfpast.append(resultdfdaily)
    
    rule00_nodelistmy,rule01_nodelistmy = pushnodes_testrule(node_list4pushdailynode,rule00_nodelistmy,rule01_nodelistmy)
    
    print(j, len(rule00_nodelistmy),len(rule01_nodelistmy))



'''
j= 0(6.11)
j  rule0  rule1  rule2  rule3
0 8 1 0 0
9 72 12 4 11


'''


rule00_out =[x[0] for x in  rule00_nodelistmy]

rule01_df = pd.DataFrame(rule01_nodelistmy,columns=['mobile','createtime','label','groupsize','relations'])
rule01_df['ratio'] = rule01_df['relations']/rule01_df['groupsize'] # relations per node



rule01_g3_out =[x[0] for x in  rule01_nodelistmy if x[3] >= 3] # group size >= 3
rule01_r3_out =[x[0] for x in  rule01_nodelistmy if x[4] >= 3]  # relationship num >= 3
rule01_r4_out =[x[0] for x in  rule01_nodelistmy if x[4] >= 4]  # relationship num >= 3



# rule1_out = [x[0] for x in rule1_nodelist4yuqi]
# rule2_out = [x[0] for x in rule2_nodelist4yuqi]
# rule3_out = [x[0] for x in rule3_nodelist4yuqi]


resultdfpast['rule00'] = resultdfpast['mobile'].apply(lambda x: 1 if (x in rule00_out) else 0) 
resultdfpast['rule01_g3'] = resultdfpast['mobile'].apply(lambda x: 1 if (x in rule01_g3_out) else 0)
resultdfpast['rule01_r3'] = resultdfpast['mobile'].apply(lambda x: 1 if (x in rule01_r3_out) else 0)
resultdfpast['rule01_r4'] = resultdfpast['mobile'].apply(lambda x: 1 if (x in rule01_r4_out) else 0)

resultdfpast1 = resultdfpast.drop(['label','status56'],axis='columns')
resultdfpast1['orderno'] = resultdfpast1['orderno'].apply(lambda x: int(x[0]))
resultdfpast1['liuliangclass'] = resultdfpast1['liuliangclass'].apply(lambda x: x[0])


resultdfpast1.to_csv("nodewithrule1_20230611_20230630.csv",index=False)


ck_sql = "SELECT\
    	tu.mobile_account,tb.bid_no,tb.create_time,\
        IFNULL(if(tbs.`status` in(3,4) AND tbs.repayment_time>= '2023-06-11' AND tbs.repayment_time < '{0}',1,0),0) as label,\
        IFNULL(if(tb.`status` in(5,6),1,0),0) as is_loan\
        FROM\
        	indonesia_loan.t_loan_bid tb\
            INNER JOIN indonesia_loan.t_loan_user tu ON tu.id = tb.user_id\
            LEFT JOIN indonesia_loan.t_loan_bill tbs on tbs.bid_no = tb.bid_no AND tbs.period =1\
        WHERE \
            tb.create_time >= '2023-06-11'\
            AND tu.mobile_account in {1}    \
            ORDER BY tu.mobile_account \
        ".format('2023-07-24',tuple(rule01_r4_out))
s_timeall = time.time()
ck_result = RunSql(ck_sql)
e_timeall = time.time()          
print("done in {}  seconds ___ {} mins".format(e_timeall-s_timeall,(e_timeall-s_timeall)/60))
    
ck_resultdf = pd.DataFrame(ck_result)
ck_resultdf  = ck_resultdf.rename({0:'mobile',3:'label',4:'status56'},axis='columns')
ck_resultdf['label'].sum()
ck_resultdf['status56'].sum()




# %%


# 更新 06-11 库里面之前所有的
sql2 = '''SELECT
        	tu.mobile_account,tb.bid_no,tb.create_time,
            IFNULL(if(tbs.`status` in(3,4) AND  tbs.repayment_time < '2023-06-11',1,0),0) as label,
            IFNULL(if(tb.`status` in(5,6),1,0),0) as is_loan
            FROM
            	indonesia_loan.t_loan_bid tb
                INNER JOIN indonesia_loan.t_loan_user tu ON tu.id = tb.user_id
                LEFT JOIN indonesia_loan.t_loan_bill tbs on tbs.bid_no = tb.bid_no AND tbs.period =1
            WHERE 
                tb.create_time < '2023-06-11'
                HAVING label = 1
                ORDER BY tu.mobile_account
            '''



s_timeall = time.time()
result2 = RunSql(sql2)
e_timeall = time.time()          
print("done in {}  seconds ___ {} mins".format(e_timeall-s_timeall,(e_timeall-s_timeall)/60))

ck_resultdf = pd.DataFrame(result2)
resultdf1 = ck_resultdf.rename({0:'mobile',3:'label',4:'status56'},axis='columns')
resultdf1 = resultdf1[['mobile','label','status56']]
resultdf1['mobile'] = resultdf1['mobile'].apply(lambda x: int(x) )
dfdict = resultdf1.to_dict('records')
node_list4push = [Node("Borrower",**dfdict[i]) for i in range(len(dfdict))]
print(len(node_list4push))
 
countupdate = propertyupdate(node_list4push,'label')
countupdate = propertyupdate(node_list4push,'status56')



# 输出 j

sql3 = '''SELECT *from ( 
SELECT 
	tu.mobile_account, tb.bid_no,tb.create_time, 
    IFNULL(if(tb.`status` in(5,6),1,0),0) as '是否放款', 
    IFNULL(if(tbs.`status` in(3,4) AND tbs.repayment_time < '2023-06-11',1,0),0) as '是否逾期', 
    CASE    WHEN trs.tracker_name not LIKE '%Google Ads ACI%' and trs.tracker_name not LIKE 'Unattributed'   then '自然流量' 
				WHEN trs.tracker_name LIKE '%Google Ads ACI%' then '谷歌广告' 
				WHEN trs.tracker_name LIKE 'Unattributed' then 'facebook' 
				else trs.tracker_name end as '流量类型', 
    td.relation_mobile as 'relationmobilelist' 
FROM 
	indonesia_loan.t_loan_bid tb 
INNER JOIN indonesia_risk.t_risk_audit_result tar ON CONVERT(tb.bid_no USING utf8)  = tar.order_no 
INNER JOIN indonesia_risk.t_risk_data_record trd ON trd.order_no = tar.order_no 
INNER JOIN indonesia_risk.t_risk_user_detail td ON td.user_id = tar.user_id 
INNER JOIN indonesia_loan.t_loan_user tu ON tu.id = tb.user_id 
LEFT JOIN indonesia_loan.t_loan_bill tbs ON tbs.bid_no = tb.bid_no AND tbs.period =1 
LEFT join indonesia_risk.t_risk_audit_result_support trs ON trs.order_no = tar.order_no and trs.serial_number = tar.last_serial_number AND trs.user_id = tar.user_id 
WHERE 
tb.create_time < '2023-06-11' 
AND tu.mobile_account in (82325307596, 89912345633)    
ORDER BY td.id DESC LIMIT 10000000000000 
) a GROUP BY a.bid_no
'''


s_timeall = time.time()
result3 = RunSql(sql3)
e_timeall = time.time()          
print("done in {}  seconds ___ {} mins".format(e_timeall-s_timeall,(e_timeall-s_timeall)/60))

ck_resultdf = pd.DataFrame(result3)
resultdf2 = ck_resultdf.rename({0:'mobile',1:'orderno',2:'createtime',3:'status56',4:'label',5:'liuliangclass',6:'relationmobile'},axis='columns')
resultdf2['mobile'] = resultdf2['mobile'].apply(lambda x: int(x) )
resultdf2['relationmobilelist'] = resultdf2['relationmobile'].apply(changecol)
resultdf2= resultdf2[resultdf2['relationmobilelist'] != 'nan']
del resultdf2['relationmobile']
gc.collect()
resultdf2['orderno'] = resultdf2['orderno'].apply(lambda x: int(x))
resultdf2['orderno'] = resultdf2['orderno'].apply(changeintcol)
resultdf2['liuliangclass'] = resultdf2['liuliangclass'].apply(lambda x: 'nan' if str(x) == 'nan' else x)
resultdf2['liuliangclass'] = resultdf2['liuliangclass'].apply(changeintcol)


dfdict = resultdf2.to_dict('records')
node_list4push = [Node("Borrower",**dfdict[i]) for i in range(len(dfdict))]
pushnodes2 = pushnodes(node_list4push)









