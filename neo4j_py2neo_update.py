# -*- coding: utf-8 -*-
"""
Created on Fri Jun 16 09:18:15 2023

@author: Administrator
"""

from py2neo import Graph, Node, Relationship,Subgraph
from py2neo import NodeMatcher
from py2neo.matching import *
from tqdm import tqdm
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
import time
import gc


df = pd.read_csv("E:/GraphIDA/IDA_20230525_20230612.csv") # 0525<= date < 0612
df =  df.rename(columns={'order_no': 'orderno', 'create_time': 'createtime','relation_mobile':'relationmobile','create_date':'createdate','create_datetime':'createdatetime','relation_mobilelist':'relationmobilelist'})
df = df[['mobile','relationmobile','orderno']]
df=df.dropna(subset=['mobile'])
df['mobile'] = df['mobile'].apply(lambda x: int(x))


def changecol(x):
    if str(x) != 'nan':
        x = x.split(',')
        x = [int(i) for i in x]
    elif str(x) == 'nan':
        x = 'nan'
        print(x)
    return x
df['relationmobilelist'] = df['relationmobile'].apply(changecol)
df = df[df['relationmobilelist'] != 'nan']
del df['relationmobile']
gc.collect()

def changeintcol(x):
    y = []
    y.append(x)
    return y
df['orderno'] = df['orderno'].apply(changeintcol)


dfdict = df.to_dict('records')

# orderno 需要非空
# del node_list4pushall,node_list4push  # 删除数据库之后 需要删除nodelist 再创建一遍
gc.collect()
node_list4pushall = [Node("Borrower",**dfdict[i]) for i in range(len(dfdict))]
node_list4push = node_list4pushall[:10000]
# node_list4push = node_list4pushall[::-1] # reverse to check your update




graph1 = Graph('neo4j://localhost:7687', auth = ('neo4j', '12345678'))
# newNode = Node('Borrower',**{'mobile':666666})
# graph1.create(newNode)
graph1.run('CREATE CONSTRAINT ON (n:Borrower) ASSERT n.mobile IS UNIQUE') # V4
ck = graph1.run('MATCH (n:Borrower) RETURN count(n)').data() # V4
graph1.run('MATCH(n1)-[r1:Contactp]->(n1) DELETE r1')
# 加上 constrain  // 需要sleep 吗

ck_time = []
s_timeall = time.time() 
for i in tqdm(range(len(node_list4push))):
    
    s_time = time.time()   
    # if i == 1:
    #     graph1.run('CREATE CONSTRAINT FOR  (n:Borrower) REQUIRE n.mobile IS UNIQUE') # V5    
    matcher1 = NodeMatcher(graph1)
    thenode = node_list4push[i]
    themobile = thenode['mobile']
    if (themobile in thenode['relationmobilelist']):
        thenode['relationmobilelist'].remove(themobile)
    premoblist = thenode['relationmobilelist']
    premoborderno = thenode['orderno']
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
        newmoblist = list(set(premoblist) | set(previousmoblist))
        neworderno =  list(set(premoborderno) | set(previousorderno)) # xiugai       
        node_test[0]['relationmobilelist'] = newmoblist
        node_test[0]['orderno'] = neworderno # xiugai
        
        # node_test[0].update({'relationmobilelist':newmoblist,'orderno':neworderno}) 
        graph1.push(node_test[0])
        node_testpush = matcher1.match("Borrower", mobile = thenode['mobile']).all() #node1 can be a list of Node
        if not (node_testpush[0]['relationmobilelist'] == newmoblist) & (node_testpush[0]['orderno'] == neworderno):
            print(f"push {i} th node error",end="\n")
    elif len(node_test) >= 2:
        print("\n")
        print(f"#####Error {i} th node repeate {len(node_test)} times>>>>>>>>>>",end="\n")
    # except ClientError:
    #     print(len(node_test))
    #     previousmoblist = node_test[0]['relationmobilelist']
    #     previousorderno = node_test[0]['orderno']
    #     newmoblist = list(set(premoblist) | set(previousmoblist))
    #     neworderno =  list(set(premoborderno) | set(previousorderno)) # xiugai       
    #     node_test[0]['relationmobilelist'] = newmoblist
    #     node_test[0]['orderno'] = neworderno # xiugai
        
    #     # node_test[0].update({'relationmobilelist':newmoblist,'orderno':neworderno}) 
    #     graph1.push(node_test[0])
    #     node_testpush = matcher1.match("Borrower", mobile = thenode['mobile']).all() #node1 can be a list of Node
    #     if not (node_testpush[0]['relationmobilelist'] == newmoblist) & (node_testpush[0]['orderno'] == neworderno):
    #         print(f"ClientError  push {i} th node error",end="\n")
    
    node_test1 = matcher1.match("Borrower", mobile = thenode['mobile']).first() #node1 can be a list of Node
  
    # thenode 的 联系人 曾经出现过 thenode['relationmobilelist']=[-999]/[23345]/[12234,3435] 
    for k in range(len(thenode['relationmobilelist'])):
        relamobile = thenode['relationmobilelist'][k]
        node1 = matcher1.match("Borrower", mobile = relamobile).all() #node1 can be a list of Node
        if len(node1) ==0: # node1 is a list (can be null)
            continue
        elif len(node1) ==1:
            # print("\n")
            # print(f"The {i}th  Node has contact in mobilelist person {k} before",end='\n')
            # if node_test1['orderno'] != node1[0]['orderno']: #防止自己连接自己
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

    e_time = time.time()    
    # print("The {}th  Node done in {}  seconds".format(i,(e_time-s_time)))
    ck_time.append((e_time-s_time))
              
e_timeall = time.time()          
print("The {} Nodes done in {}  seconds {} mins".format(i,e_timeall-s_timeall,(e_timeall-s_timeall)/60))


#使用cypher语言里面的merge命令，会查询是否存在已有结点，如果不存在则创建
#使用py2neo里面的merge命令，会直接用新的覆盖旧的
# graph1.delete_all()



ck_timeplot = [ck_time[i] for i in range(len(ck_time)) if i%100==0]
plt.plot(ck_timeplot)
plt.show()