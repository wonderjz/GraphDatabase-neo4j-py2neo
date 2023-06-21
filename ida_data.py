# -*- coding: utf-8 -*-
"""
Created on Mon May 29 18:08:19 2023

@author: Administrator
"""

import pandas as pd
import numpy as np
import time


df = pd.read_csv("E:/IDA1.csv")

df['create_datetemp'] = pd.to_datetime(df['create_time'])
df['create_date'] = df['create_datetemp'].apply(lambda x: x.date())
df['create_datetime'] = df['create_datetemp'].apply(lambda x: x.time())
# df['mobile'] = df['mobile'].astype('int') #  have nan
df = df.drop('create_datetemp',axis=1)
df = df.rename(columns={'流量类型': "liuliangclass"})

# df['mobile1'] = pd.to_numeric(df['mobile'])
# df['mobile1'] = df['mobile'].map(lambda x: int(x) if str(x) != 'nan')

# df['relation_mobile'].apply(lambda x: x.split(',') if str(x) != 'nan')
def changecol(x):
    if str(x) != 'nan':
        x = x.split(',')
        x = [int(i) for i in x]
    return x
df['relation_mobilelist'] = df['relation_mobile'].apply(changecol)

def changemobcol(x):
    if str(x) != 'nan' and x != np.inf:
        x = int(x)
    return x   


# mobilelist = df['mobile'].tolist()
# mobilelist = [int(x) for x in mobilelist if str(x) != 'nan']
# df['mobile1'] = pd.Series(mobilelist)

from datetime import datetime as dt

df['date'] = pd.to_datetime(df['create_date'])
df1 = df[df['date'] < dt.strptime('2023-04-01', "%Y-%m-%d")]
df2 = df[df['date'] >= dt.strptime('2023-04-01', "%Y-%m-%d")]
df1 = df1.drop('date',axis=1)
df2 = df2.drop('date',axis=1)


df1.to_csv("IDA_m2_3.csv",index=False)
df2.to_csv("IDA_m4_5.csv",index=False)


# df = pd.read_csv("IDA2.csv")
# df_dict = (df.T).to_dict()

# for i in range(len(df_dict)):
#     if df_dict[i].get('relation_mobile') != 'nan':
#         try:
#             checkitemlist = (df_dict[i].get('relation_mobile')).split(',')
#             # checkitemlist = [x.split(',') for x in df_dict[i].get('relation_mobile') if str(x) != 'nan']
#             df_dict[i]["relation_mobile"] = checkitemlist
#         except:
#             print("index__{}__{}____".format(i,"error"))
#     else:
#         print("index__{}__{}____".format(i,df_dict[i]["relation_mobile"]))


# given a df_dict return the positions of the repeated elements

relate1 = [x for x in df['relation_mobile'].tolist() if str(x) != 'nan']


# 自动消失 首位的 0
relatedoublelist = []
for x in range(len(relate1)):
    relatedoublelist.append(relate1[x].split(','))

relateflat = []

#  写成函数
for sublist in relatedoublelist:
    for item in sublist:
        relateflat.append(item)

relateflat = [int(x) for x in relateflat]
mobileflat = [int(x) for x in df['mobile'].tolist() if str(x) != 'nan']
repeatlist =  list(set(mobileflat) & set(relateflat))



def get_key (dicta, value):
    return [k for k, v in dicta.items() if v == value]

# df_dict1 = df_dict.copy()

def get_connection(df_dict,repeatlist):
    interlistofdict = []
    for i in range(len(df_dict)):
        if str(df_dict[i]['relation_mobile'])=='nan':
            continue
        else:
            check_relationlist = [int(x) for x in df_dict[i]['relation_mobile1'] ]
            check_interset = set(repeatlist) & set(check_relationlist)
            if len(check_interset) > 0:
                check_interlist = list(check_interset)
                check_themobile = df_dict[i]['mobile']
                tempdict = {'mobile':int(check_themobile), 'relation_mobile':check_interlist}
                interlistofdict.append(tempdict)                
                # interdict.update({str(i): check_interlist})
                
# the info of person who has been recorded as others' contact//'relation_mobile' is the person who did it                 
    return interlistofdict # the info of person who has been recorded as others' contact              
    
check_interdict = get_connection(df_dict,repeatlist)

# 81927374970 # df.iloc[14,:]  	order_no 1628048911349600256

listofdictforgraph = []
check_count = 0
for i in range(len(check_interdict)):
    for k in range(len(check_interdict[i]['relation_mobile'])):
        originmobile = check_interdict[i]['relation_mobile'][k]
        relatedmobile = check_interdict[i]['mobile']
        tempdict1 = {'originmobile':originmobile,'relatedmobile':relatedmobile}
        listofdictforgraph.append(tempdict1)
        if relatedmobile == originmobile:
            check_count += 1
            print(str(originmobile)+"__",end="")

print(check_count) #把自己手机号码 也 填成了 联系人之一
connectionforgraph = pd.DataFrame(listofdictforgraph)


connectionforgraph.to_csv("IDA2_connection.csv",index=False)


# .drop_duplicates 函数 有问题？ 不能删除所有的
connforgraph_uni = connectionforgraph.drop_duplicates(['originmobile','relatedmobile'],keep=False)

def droprowbysamecol(df,collist):
    lenin = len(df)
    fordroprows = []
    for i in range(len(df)):
        checklist = []
        for j in range(len(collist)):
            checklist.append(df.iloc[i,df.columns.get_loc(collist[j])])
        if len(set(checklist)) == 1:
            fordroprows.append(i)
    
    lenout = len(df) - len(fordroprows)
    print("dfin_rows__{}__dfout_rows__{}".format(lenin,lenout))
    outrows = sorted(list(set(list(range(lenin))) - set(fordroprows)))
    
    dfout = df.iloc[outrows,:]           
    return dfout

connforgraph_uniq = droprowbysamecol(connforgraph_uni,['originmobile','relatedmobile'])

connforgraph_uniq.to_csv("IDA2_connection_uniq.csv",index=False)



'''
LOAD CSV  FROM "file:///IDA2.csv" AS row  
CREATE (:Borrower{orderno: row[0], createtime: row[1],mobile: toInteger(row[2]),relationmobile: row[3], liuliangclass: row[4], createdate: row[5], createdatetime: row[6], relationmobilelist: row[7]} )

10w 数据 写入 ，1w关系写入 20min around
'''



from py2neo import Graph, Node, Relationship,Subgraph
from py2neo import NodeMatcher
# Graph()中第一个为local host链接，auth为认证，包含 username 和 password
graph = Graph('neo4j://localhost:7474', auth = ('neo4j', '12345678'))
graph = Graph('http://127.0.0.1:7687', auth = ('neo4j', '12345678'))
graph = Graph('neo4j://localhost:7474', username="neo4j", password='12345678')



# node_list4push = [Node("Borrower", orderno = f"{df2['order_no'][i:i+1]}",createtime = f"{df2['create_time'][i:i+1]}",  mobile = f"{int(df2['mobile'][i:i+1])}", relationmobile = f"{df2['relation_mobile'][i:i+1]}", liulinagclass = f"{df2['liuliangclass'][i:i+1]}", createdate = f"{df2['create_date'][i:i+1]}",  createdatetime = f"{df2['create_datetime'][i:i+1]}",relationmobilelist = f"{df2['relation_mobilelist'][i:i+1]}") for i in range(len(df2)) ]
# node_list4push = [Node("Borrower", orderno = f"{df_try['order_no'][i:i+1]}",createtime = f"{df_try['create_time'][i:i+1]}",  mobile = f"{int(df_try['mobile'][i:i+1])}", relationmobile = f"{df_try['relation_mobile'][i:i+1]}", liulinagclass = f"{df_try['liuliangclass'][i:i+1]}", createdate = f"{df_try['create_date'][i:i+1]}",  createdatetime = f"{df_try['create_datetime'][i:i+1]}",relationmobilelist = f"{df_try['relation_mobilelist'][i:i+1]}") for i in range(len(df_try)) ]


df2 =  df2.rename(columns={'order_no': 'orderno', 'create_time': 'createtime','relation_mobile':'relationmobile','create_date':'createdate','create_datetime':'createdatetime','relation_mobilelist':'relationmobilelist'})
df2=df2.dropna(subset=['mobile'])
# df2['mobile'] = df2['mobile'].astype(int)
df2['mobile'] = df2['mobile'].apply(lambda x: int(x))
df2['createdate'] = df2['createdate'].apply(lambda x:str(x))
df2['createdatetime'] = df2['createdatetime'].apply(lambda x:str(x))
df2dict = df2.to_dict('records')
node_list4push = [Node("Borrower",**df2dict[i]) for i in range(len(df2dict))]

node_list4pushp2 = node_list4push
node_list4push = node_list4push[:1000]
node_list4push = node_list4push[::-1] # 列表翻转

macher1 = NodeMatcher(graph)
# macher2 = RelationshipMatcher(graph)
# node2 = macher2.match(r_type="KNOWS").limit(25)  # 找出关系类型为KNOWS的前25个关系


#////////////////////////////////////////////////////////////////

# graph = Graph('http://localhost:7474', auth = ('neo4j', '12345678'))

s_time = time.time() 
for i in range(len(node_list4push)):
    # s_time = time.time() 
    
    macher1 = NodeMatcher(graph)
    thenode = node_list4push[i]
    graph.create(thenode)
    
    # thenode 的 联系人 曾经出现过 
    for k in range(len(thenode['relationmobilelist'])):
        relamobile = thenode['relationmobilelist'][k]
        node1 = macher1.match("Borrower", mobile = relamobile).all() #node1 can be a list of Node
        if len(node1) ==0:
            continue
        elif len(node1) >=1:
            print(f"The {i}th  Node has contact before")
            for j in range(len(node1)):
                graph.create(Relationship(thenode, "Contactp", node1[j]))
    # thenode 作为 其他人  的联系人
    themobile = thenode['mobile']
    cypher1 = 'MATCH (n:Borrower) WHERE  n.relationmobilelist CONTAINS '+str(themobile)+' RETURN  n'
    data1 = graph.run(cypher1).data()
    if len(data1) >=1:
        print(f"The {i}th  Node is others' contact")
        for m in range(len(data1)):
            ofrelanode = data1[m]['n']
            graph.create(Relationship(ofrelanode, "Contactp", thenode))
        
    # e_time = time.time()
    # print("The {}th  Node done in {}  seconds".format(i,(e_time-s_time)))
                
e_time = time.time()          
print("The  Node done in {}  seconds".format(e_time-s_time))
    
# 待做 constrin on 'orderno'
# 避免重复create
    
checkset = set()
checkdup = [x for x in df1['mobile'].tolist() if x in checkset or (checkset.add(x) or False)]
print(checkdup[0])  # 81267305359





cypher1 = 'MATCH (n:Borrower)  WHERE date("2023-03-01") <= date(n.createdate) <= date("2023-03-02") RETURN n.orderno'
data1 = graph.run(cypher1).data()

cypher1_all = 'MATCH (n:Borrower)  WHERE date("2023-03-01") <= date(n.createdate) <= date("2023-03-02") RETURN n'
data1_all = graph.run(cypher1_all).data()

# print(data1, type(data1))
# print(data1[0]['n']['mobile'])

orderlist = [ data1[i]['n.orderno'] for i in range(len(data1))]




# 查询节点
node_list = []
for i in range(10000):
    node_matcher = NodeMatcher(graph)
    node = node_matcher.match('Test', name=f"上海_{i}").first()
    node_list.append(node)
print("find nodes.")
print(len(node_list))





# 建立两个节点之间的关系
def create_relationship(graph, label1, attrs1, label2, attrs2, r_name):
    value1 = match_node(graph, label1, attrs1)
    value2 = match_node(graph, label2, attrs2)
    if value1 is None or value2 is None:
        return False
    r = Relationship(value1, r_name, value2)
    graph.create(r)

# r = "证券交易所"
# create_relationship(graph, label1, attrs1, label2, attrs2, r)

# 查询节点
def match_node(graph, label, attrs):
    n = "_.name=" + "\"" + attrs["name"] + "\""
    matcher = NodeMatcher(graph)
    return matcher.match(label).where(n).first()

def batch_create(graph, nodes_list, relations_list):
    """
        批量创建节点/关系,nodes_list和relations_list不同时为空即可
        特别的：当利用关系创建节点时，可使得nodes_list=[]
    :param graph: Graph()
    :param nodes_list: Node()集合
    :param relations_list: Relationship集合
    :return:
    """

    subgraph = Subgraph(nodes_list, relations_list)
    tx_ = graph.begin()
    tx_.create(subgraph)
    graph.commit(tx_)

"""
    # 批量创建节点
    nodes_list = []  # 一批节点数据
    relations_list = []  # 一批关系数据
    # 如：实例化一个节点
    node_1 = Node("中药名", name="白术")
    nodes_list.append(node_1)
    node_2 = Node("功能", name="健脾")
    nodes_list.append(node_2)

    # 创建两个节点之间的关系
    relation = Relationship(node_1, "功能", node_2)
    relations_list.append(relation)

    node_3 = Node("功能", name="益气")
    nodes_list.append(node_3)
    relation2 = Relationship(node_1, "功能", node_3)
    relations_list.append(relation2)

    # 批量创建节点/关系
    batch_create(graph, nodes_list, relations_list)
"""


# 更新一个 属性
aa = graph.nodes.match("Borrower", orderno="1630614580056182784").first()
# 查询当前所有属性？？
aa["paybacktime"] = "2023-04-01"
graph.push(aa)
print(graph.nodes.match("Borrower", orderno="1630614580056182784").first()["paybacktime"])




# # 批量更新property

# 'n' is the n for node in the cypher query 
# nodeV = 'n'
# cols = [x for x in data1_all[0][nodeV]]
# returnString = ', '.join([f'{nodeV}.{col} as {col}' for col in data1_all[0][nodeV]]) #Generating a return statement with aliases 


node_list = [data1_all[i]['n'] for i in range(len(data1_all))]

df = []
for i in range(len(node_list)):
    df.append(dict(node_list[i]))
df = pd.DataFrame(df)   

# df_renew = pd.read_csv("df_try4push.csv")
# df_renew['orderno'] = ['%.2f' % x for x in df_renew['orderno'].values]
# df_renew['orderno'] = df_renew['orderno'].apply(lambda x: int(x))
# df_renew['orderno'] = df_renew['orderno'].astype(str)

dfmerged = pd.merge(df,df1,on="orderno")
dfmerged['paybacktime'] = dfmerged.apply(lambda x: x['paybacktime_y'] if len(x['paybacktime_y'])!=0  else x['paybacktime_x'], axis=1)

# node_list4push = [Node("Borrower", orderno = f"{node_list[i]['orderno']}",paybacktime = f"{node_list[i]['paybacktime']}") for i in range(len(node_list)) ]
node_list4push = [Node("Borrower", orderno = f"{dfmerged['orderno'][i]}",paybacktime = f"{dfmerged['paybacktime'][i]}") for i in range(len(dfmerged)) ]


s_time = time.time() 
# for i  in  range(len(node_list)):
#     node_list[i].update(node_list[i]) # node_list4push
#     graph.push(node_list[i])
    
for i  in  range(len(node_list)):
    node_list[i].update(node_list4push[i]) # node_list4push
    graph.push(node_list[i])
    
e_time = time.time()
print(f"cost time: {(e_time - s_time)}") #47.49s 

    
cypher2 = 'MATCH (n:Borrower)  WHERE date("2023-03-01") <= date(n.createdate) <= date("2023-03-02") RETURN n.mobile'
data2 = graph.run(cypher2).data()

# from multiprocessing.pool import ThreadPool

# def pushnewproperty(node_listitem):
#     node_listitem.update(node_listitem)
#     graph.push(node_listitem)

# # pool_size = 10
# # pool = ThreadPool(pool_size)  

# s_time = time.time()  
# pool = ThreadPool()                  
# pool.map(pushnewproperty, node_list)  # 修改 node_list4push
# pool.close()  
# pool.join()  # 等待线程全部执行完
    
# e_time = time.time()
# print(f"cost time: {(e_time - s_time)}") #47.49s 


