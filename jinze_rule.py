# -*- coding: utf-8 -*-
"""
Created on Mon Jun 19 16:48:01 2023

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


# 用 python 获取并 检验每个节点 保存节点的 不同规则的结果

# rule 1
graph1 = Graph('neo4j://localhost:7687', auth = ('neo4j', '12345678'))

# 逾期的
badmobile = graph1.run("MATCH (n1) WHERE n1.label >= 1 RETURN n1.mobile").data()
badmobile = [x['n1.mobile'] for x in badmobile]

# 审核拒绝和逾期的
# 有交叉！！！
badmobileall = graph1.run("MATCH (n1) WHERE n1.label >= 1 OR n1.auditstatus =-1 RETURN n1.mobile").data()
badmobileall = [x['n1.mobile'] for x in badmobileall]

totalnum = graph1.run("MATCH (n)  RETURN  count(*)").data()['count(*)']
totalnum = totalnum[0]['count(*)']


'''
# 对 所有 采用 rule
rule1_nodelist = []
for i in tqdm(range(116562)):
    # thenode = graph1.nodes.match("Borrower", ID= i).first()
    thenode = graph1.run((f"match (n) where id(n)={i} return (n)")).data()[0]['n']

    themobile = thenode['mobile']
    thenoder1n = graph1.run(f"MATCH (n1)-[r1:Contactp]->(n2) WHERE id(n1)={i} RETURN n2").data()
    if thenoder1n == None:
        continue
    count = 0
    for j in range(len(thenoder1n)):
        if thenoder1n[j]['n2']['label'] >= 1:
            mobilelist = thenoder1n[j]['n2']['relationmobilelist']
            if themobile in mobilelist:
                count += 1
                rule1_nodelist.append(thenode)
        if count ==1:
            break
        else:
            continue

rule1_mobile = [x['mobile'] for x in rule1_nodelist]



rule2_nodelist = []
for i in tqdm(range(116562)):
    # thenode = graph1.nodes.match("Borrower", ID= i).first()
    thenode = graph1.run((f"match (n) where id(n)={i} return (n)")).data()[0]['n']
    themobile = thenode['mobile']
    thenoder1n = graph1.run(f"MATCH (n1)<-[r1:Contactp]-(n2) WHERE id(n1)={i} RETURN n2").data()

    count1 = 0
    count2 = 0
    if thenoder1n == None:
        continue
    for j in range(len(thenoder1n)):
        if thenoder1n[j]['n2']['label'] >= 1:
            count1 += 1
    thenoder2n = graph1.run(f"MATCH (n1)<-[r1:Contactp]-(n2)<-[r2:Contactp]-(n3) WHERE id(n1)={i} RETURN n3").data()
    if thenoder2n == None:
        continue
    for k in range(len(thenoder2n)):
        if (thenoder2n[k]['n3']['label'] >= 1) & (thenoder2n[k]['n3']['mobile'] != themobile):
            count2 += 1
    if (count1 + count2) >= 2:
        addlist = [thenode,count1,count2]
        rule2_nodelist.append(addlist)


rule2_mobile = [x[0]['mobile'] for x in rule2_nodelist]
rule2df = pd.DataFrame(rule2_nodelist)
rule2df['countsum'] = rule2df[1] + rule2df[2]
rule2df['mobile'] = rule2df[0].apply(lambda x: x['mobile'])

rule2df_ck = rule2df.iloc[:,1:]

rule2_sum3mobile= list(rule2df[rule2df['countsum']>= 3]['mobile'])


rule3_nodelist = []
for i in tqdm(range(116562)):
    # thenode = graph1.nodes.match("Borrower", ID= i).first()
    thenode = graph1.run((f"match (n) where id(n)={i} return (n)")).data()[0]['n']
    themobile = thenode['mobile']
    thenoder1n = graph1.run(f"MATCH (n)<-[r1:Contactp]-(n1) WHERE id(n)={i} RETURN n1").data()
    if thenoder1n == None:
        continue
    count1 = 0
    for j in range(len(thenoder1n)):
        fathermobile =  thenoder1n[j]['n1']['mobile']       
        thenodebro = graph1.run(f"MATCH (n2)<-[r:Contactp]-(n1) WHERE n1.mobile={fathermobile} RETURN n2").data()
        if thenodebro == None:
            continue
        else:
            for k in range(len(thenodebro)):
                if (thenodebro[k]['n2']['mobile'] != themobile) & (thenodebro[k]['n2']['label'] >= 1):
                    count1 +=1
                
    if count1 >= 2:
        addlist = [thenode,count1]
        rule3_nodelist.append(addlist)
      

rule3_mobile = [x[0]['mobile'] for x in rule3_nodelist]

rule3df = pd.DataFrame(rule3_nodelist)
rule3dfcol2 = rule3df[1]
rule3dfcol2 = list(rule3dfcol2)

rule3_3mobile = [x[0]['mobile'] for x in rule3_nodelist  if x[1]>= 3]
87760126988 in rule3_mobile



rule4_nodelist = []
for i in tqdm(range(116562)):
    # thenode = graph1.nodes.match("Borrower", ID= i).first()
    thenode = graph1.run((f"match (n) where id(n)={i} return (n)")).data()[0]['n']
    themobile = thenode['mobile']
    thenoder1n = graph1.run(f"MATCH (n1)-[r1:Contactp]->(n2) WHERE id(n1)={i} RETURN n2").data()

    count1 = 0
    count2 = 0
    if thenoder1n == None:
        continue
    for j in range(len(thenoder1n)):
        if thenoder1n[j]['n2']['label'] >= 1:
            count1 += 1
    thenoder2n = graph1.run(f"MATCH (n1)-[r1:Contactp]->(n2)-[r2:Contactp]->(n3) WHERE id(n1)={i} RETURN n3").data()
    if thenoder2n == None:
        continue
    for k in range(len(thenoder2n)):
        if (thenoder2n[k]['n3']['label'] >= 1) & (thenoder2n[k]['n3']['mobile'] != themobile):
            count2 += 1
    if (count1 + count2) >= 2:
        addlist = [thenode,count1,count2]
        rule4_nodelist.append(addlist)


rule4_mobile = [x[0]['mobile'] for x in rule4_nodelist]
rule4df = pd.DataFrame(rule4_nodelist)
rule4df['countsum'] = rule4df[1] + rule4df[2]
rule4df['mobile'] = rule4df[0].apply(lambda x: x['mobile'])

rule4df_ck = rule4df.iloc[:,1:]
rule4_sum3mobile= list(rule4df[rule4df['countsum']>= 3]['mobile'])
'''    



# 对 auditstatus == -1 之外的 采用 rule based on 逾期
rule1_nodelist4pass = []
for i in tqdm(range(116562)):
    # thenode = graph1.nodes.match("Borrower", ID= i).first()
    thenode = graph1.run((f"match (n) where id(n)={i} return (n)")).data()[0]['n']
    if thenode['auditstatus'] == -1:
        continue
    themobile = thenode['mobile']
    thenoder1n = graph1.run(f"MATCH (n1)-[r1:Contactp]->(n2) WHERE id(n1)={i} RETURN n2").data()
    if thenoder1n == None:
        continue
    count = 0
    for j in range(len(thenoder1n)):
        if thenoder1n[j]['n2']['label'] >= 1:
            mobilelist = thenoder1n[j]['n2']['relationmobilelist']
            if themobile in mobilelist:
                count += 1
                rule1_nodelist4pass.append(thenode)
        if count ==1:
            break
        else:
            continue

rule1_mobile4pass = [x['mobile'] for x in rule1_nodelist4pass]



# 对 auditstatus == -1 之外的 采用 rule based on 逾期(修改nodelist名称 & 判断thenode auditstatus！= -1)
rule2_nodelist4pass = []
for i in tqdm(range(116562)):
    # thenode = graph1.nodes.match("Borrower", ID= i).first()
    thenode = graph1.run((f"match (n) where id(n)={i} return (n)")).data()[0]['n']
    if thenode['auditstatus'] == -1:
        continue    
    themobile = thenode['mobile']
    thenoder1n = graph1.run(f"MATCH (n1)<-[r1:Contactp]-(n2) WHERE id(n1)={i} RETURN n2").data()

    count1 = 0
    count2 = 0
    if thenoder1n == None:
        continue
    for j in range(len(thenoder1n)):
        if thenoder1n[j]['n2']['label'] >= 1:
            count1 += 1
    thenoder2n = graph1.run(f"MATCH (n1)<-[r1:Contactp]-(n2)<-[r2:Contactp]-(n3) WHERE id(n1)={i} RETURN n3").data()
    if thenoder2n == None:
        continue
    for k in range(len(thenoder2n)):
        if (thenoder2n[k]['n3']['label'] >= 1) & (thenoder2n[k]['n3']['mobile'] != themobile):
            count2 += 1
    if (count1 + count2) >= 2:
        addlist = [thenode,count1,count2]
        rule2_nodelist4pass.append(addlist)


rule2_mobile4pass = [x[0]['mobile'] for x in rule2_nodelist4pass]
rule2df4pass = pd.DataFrame(rule2_nodelist4pass)
rule2df4pass['countsum'] = rule2df4pass[1] + rule2df4pass[2]
rule2df4pass['mobile'] = rule2df4pass[0].apply(lambda x: x['mobile'])
rule2df4pass_ck = rule2df4pass.iloc[:,1:]

rule2_sum3mobile4pass= list(rule2df4pass[rule2df4pass['countsum']>= 3]['mobile'])




# 对 auditstatus == -1 之外的 采用 rule based on 逾期
rule3_nodelist4pass = []
for i in tqdm(range(116562)):
    # thenode = graph1.nodes.match("Borrower", ID= i).first()
    thenode = graph1.run((f"match (n) where id(n)={i} return (n)")).data()[0]['n']
    if thenode['auditstatus'] == -1:
        continue    
    themobile = thenode['mobile']
    thenoder1n = graph1.run(f"MATCH (n)<-[r1:Contactp]-(n1) WHERE id(n)={i} RETURN n1").data()
    if thenoder1n == None:
        continue
    count1 = 0
    for j in range(len(thenoder1n)):
        fathermobile =  thenoder1n[j]['n1']['mobile']       
        thenodebro = graph1.run(f"MATCH (n2)<-[r:Contactp]-(n1) WHERE n1.mobile={fathermobile} RETURN n2").data()
        if thenodebro == None:
            continue
        else:
            for k in range(len(thenodebro)):
                if (thenodebro[k]['n2']['mobile'] != themobile) & (thenodebro[k]['n2']['label'] >= 1):
                    count1 +=1
                
    if count1 >= 2:
        addlist = [thenode,count1]
        rule3_nodelist4pass.append(addlist)
      

rule3_mobile4pass = [x[0]['mobile'] for x in rule3_nodelist4pass]

rule3df4pass = pd.DataFrame(rule3_nodelist4pass)
rule3df4passcol2 = rule3df4pass[1]
rule3df4passcol2 = list(rule3df4passcol2)

rule3_3mobile4pass = [x[0]['mobile'] for x in rule3_nodelist4pass  if x[1]>= 3]



# 对 auditstatus == -1 之外的 采用 rule based on 逾期
rule4_nodelist4pass = []
for i in tqdm(range(116562)):
    # thenode = graph1.nodes.match("Borrower", ID= i).first()
    thenode = graph1.run((f"match (n) where id(n)={i} return (n)")).data()[0]['n']
    if thenode['auditstatus'] == -1:
        continue    
    themobile = thenode['mobile']
    thenoder1n = graph1.run(f"MATCH (n1)-[r1:Contactp]->(n2) WHERE id(n1)={i} RETURN n2").data()

    count1 = 0
    count2 = 0
    if thenoder1n == None:
        continue
    for j in range(len(thenoder1n)):
        if thenoder1n[j]['n2']['label'] >= 1:
            count1 += 1
    thenoder2n = graph1.run(f"MATCH (n1)-[r1:Contactp]->(n2)-[r2:Contactp]->(n3) WHERE id(n1)={i} RETURN n3").data()
    if thenoder2n == None:
        continue
    for k in range(len(thenoder2n)):
        if (thenoder2n[k]['n3']['label'] >= 1) & (thenoder2n[k]['n3']['mobile'] != themobile):
            count2 += 1
    if (count1 + count2) >= 2:
        addlist = [thenode,count1,count2]
        rule4_nodelist4pass.append(addlist)


rule4_mobile4pass = [x[0]['mobile'] for x in rule4_nodelist4pass]
rule4df4pass = pd.DataFrame(rule4_nodelist4pass)
rule4df4pass['countsum'] = rule4df4pass[1] + rule4df4pass[2]
rule4df4pass['mobile'] = rule4df4pass[0].apply(lambda x: x['mobile'])

rule4df4pass_ck = rule4df4pass.iloc[:,1:]
rule4_sum3mobile4pass= list(rule4df4pass[rule4df4pass['countsum']>= 3]['mobile'])



# 对 auditstatus == -1 之外的 采用 rule based on 逾期&审核拒绝
# rule1p: rule1 plus
rule1p_nodelist4pass = []
for i in tqdm(range(116562)):
    # thenode = graph1.nodes.match("Borrower", ID= i).first()
    thenode = graph1.run((f"match (n) where id(n)={i} return (n)")).data()[0]['n']
    if thenode['auditstatus'] == -1:
        continue
    themobile = thenode['mobile']
    thenoder1n = graph1.run(f"MATCH (n1)-[r1:Contactp]->(n2) WHERE id(n1)={i} RETURN n2").data()
    if thenoder1n == None:
        continue
    count = 0
    for j in range(len(thenoder1n)):
        if (thenoder1n[j]['n2']['label'] >= 1) | (thenoder1n[j]['n2']['auditstatus'] == -1):
            mobilelist = thenoder1n[j]['n2']['relationmobilelist']
            if themobile in mobilelist:
                count += 1
                rule1p_nodelist4pass.append(thenode)
        if count ==1:
            break
        else:
            continue

rule1p_mobile4pass = [x['mobile'] for x in rule1p_nodelist4pass]



# 对 auditstatus == -1 之外的 采用 rule based on 逾期&审核拒绝
rule2p_nodelist4pass = []
for i in tqdm(range(116562)):
    # thenode = graph1.nodes.match("Borrower", ID= i).first()
    thenode = graph1.run((f"match (n) where id(n)={i} return (n)")).data()[0]['n']
    if thenode['auditstatus'] == -1:
        continue    
    themobile = thenode['mobile']
    thenoder1n = graph1.run(f"MATCH (n1)<-[r1:Contactp]-(n2) WHERE id(n1)={i} RETURN n2").data()

    count1 = 0
    count2 = 0
    if thenoder1n == None:
        continue
    for j in range(len(thenoder1n)):
        if (thenoder1n[j]['n2']['label'] >= 1) | (thenoder1n[j]['n2']['auditstatus'] == -1):
            count1 += 1
    thenoder2n = graph1.run(f"MATCH (n1)<-[r1:Contactp]-(n2)<-[r2:Contactp]-(n3) WHERE id(n1)={i} RETURN n3").data()
    if thenoder2n == None:
        continue
    for k in range(len(thenoder2n)):
        if ((thenoder2n[k]['n3']['label'] >= 1) | (thenoder2n[k]['n3']['auditstatus'] == -1))  & (thenoder2n[k]['n3']['mobile'] != themobile):
            count2 += 1
    if (count1 + count2) >= 2:
        addlist = [thenode,count1,count2]
        rule2p_nodelist4pass.append(addlist)


rule2p_mobile4pass = [x[0]['mobile'] for x in rule2p_nodelist4pass]
rule2pdf4pass = pd.DataFrame(rule2p_nodelist4pass)
rule2pdf4pass['countsum'] = rule2pdf4pass[1] + rule2pdf4pass[2]
rule2pdf4pass['mobile'] = rule2pdf4pass[0].apply(lambda x: x['mobile'])
rule2pdf4pass_ck = rule2pdf4pass.iloc[:,1:]

rule2p_sum3mobile4pass= list(rule2pdf4pass[rule2pdf4pass['countsum']>= 3]['mobile'])


# 对 auditstatus == -1 之外的 采用 rule based on 逾期&审核拒绝
rule3p_nodelist4pass = []
rule3m_nodelist4pass = []
for i in tqdm(range(116562)):
    # thenode = graph1.nodes.match("Borrower", ID= i).first()
    thenode = graph1.run((f"match (n) where id(n)={i} return (n)")).data()[0]['n']
    # if thenode['auditstatus'] == -1:
    #     continue    
    themobile = thenode['mobile']
    thenoder1n = graph1.run(f"MATCH (n)<-[r1:Contactp]-(n1) WHERE id(n)={i} RETURN n1").data()
    if thenoder1n == None:
        continue
    count1 = 0
    for j in range(len(thenoder1n)):
        fathermobile =  thenoder1n[j]['n1']['mobile']       
        thenodebro = graph1.run(f"MATCH (n2)<-[r:Contactp]-(n1) WHERE n1.mobile={fathermobile} RETURN n2").data()
        if thenodebro == None:
            continue
        else:
            for k in range(len(thenodebro)):
                if ((thenodebro[k]['n2']['label'] >= 1) | (thenodebro[k]['n2']['auditstatus'] == -1)) & (thenodebro[k]['n2']['label'] >= 1):
                    count1 +=1
                
    if count1 >= 1:  # >= 2
        addlist = [thenode,count1]
        # rule3p_nodelist4pass.append(addlist)
        rule3m_nodelist4pass.append(addlist)      

# rule3p_mobile4pass = [x[0]['mobile'] for x in rule3p_nodelist4pass]
rule3m_mobile4pass = [x[0]['mobile'] for x in rule3m_nodelist4pass]

# rule3pdf4pass = pd.DataFrame(rule3p_nodelist4pass)
# rule3pdf4passcol2 = rule3pdf4pass[1]
# rule3pdf4passcol2 = list(rule3pdf4passcol2)

# rule3p_3mobile4pass = [x[0]['mobile'] for x in rule3p_nodelist4pass  if x[1]>= 3]
rule3m_3mobile4pass = [x[0]['mobile'] for x in rule3m_nodelist4pass  if x[1]>= 3]


# 对 auditstatus == -1 之外的 采用 rule based on 逾期&审核拒绝
rule4p_nodelist4pass = []
for i in tqdm(range(116562)):
    # thenode = graph1.nodes.match("Borrower", ID= i).first()
    thenode = graph1.run((f"match (n) where id(n)={i} return (n)")).data()[0]['n']
    if thenode['auditstatus'] == -1:
        continue    
    themobile = thenode['mobile']
    thenoder1n = graph1.run(f"MATCH (n1)-[r1:Contactp]->(n2) WHERE id(n1)={i} RETURN n2").data()

    count1 = 0
    count2 = 0
    if thenoder1n == None:
        continue
    for j in range(len(thenoder1n)):
        if (thenoder1n[j]['n2']['label'] >= 1) | (thenoder1n[j]['n2']['auditstatus'] == -1):
            count1 += 1
    thenoder2n = graph1.run(f"MATCH (n1)-[r1:Contactp]->(n2)-[r2:Contactp]->(n3) WHERE id(n1)={i} RETURN n3").data()
    if thenoder2n == None:
        continue
    for k in range(len(thenoder2n)):
        if ((thenoder2n[k]['n3']['label'] >= 1) | (thenoder2n[k]['n3']['auditstatus'] == -1) ) & (thenoder2n[k]['n3']['mobile'] != themobile):
            count2 += 1
    if (count1 + count2) >= 2:
        addlist = [thenode,count1,count2]
        rule4p_nodelist4pass.append(addlist)


rule4p_mobile4pass = [x[0]['mobile'] for x in rule4p_nodelist4pass]
rule4pdf4pass = pd.DataFrame(rule4p_nodelist4pass)
rule4pdf4pass['countsum'] = rule4pdf4pass[1] + rule4pdf4pass[2]
rule4pdf4pass['mobile'] = rule4pdf4pass[0].apply(lambda x: x['mobile'])

rule4pdf4pass_ck = rule4pdf4pass.iloc[:,1:]
rule4p_sum3mobile4pass= list(rule4pdf4pass[rule4pdf4pass['countsum']>= 3]['mobile'])





def fscore(baselist, testlist, totalnum=116562):
    TP = len(set(baselist) & set(testlist))    
    TPFP = len(testlist)
    TPFN = len(baselist)
    FNTN = totalnum - TPFP
    FP = TPFP - TP
    FN = TPFN - TP
    TN = FNTN - FN
    
    FPR = FP/(FP+ TN)
    recall = TP/TPFN
    precision = TP/TPFP    # 判断与事实均为真/ 事实为真
    fs = 2/(1/recall + 1/precision)
    # print("For "+str(baselist)+" and  "+str(testlist), end='\n' )
    print(f"总样本数 {(totalnum)}  总逾期样本数 {len(baselist)}  选中样本数 {len(testlist)}:", end='\n')
    # print(f"For {fscore.__code__.co_varnames}", end = '\n')
    # print(f"precision(选中样本逾期率): {format(precision, '.3f')}  FPR: {format(FPR, '.3f')}   recall(TPR): {format(recall, '.3f')}    fscore: {format(fs, '.3f')} ", end = '\n')
    # print(f"") 选中比例
    print(f"TP选中样本中的逾期数: {TP}", end='\n')
    print(f"precision: {format(precision, '.3f')}", end = '\n')
    print("",end= '\n')
    return precision

# fscore.__code__.co_varnames
# 主要参考 precision
fs_rule1 = fscore(badmobile,rule1_mobile) # precision: 0.256 选中样本数410

fs_rule2 = fscore(badmobile,rule2_mobile) #precision: 0.161 选中样本数155
fs_rule2_sum3 = fscore(badmobile,rule2_sum3mobile) # precision: 0.239   选中样本数46 

fs_rule3 = fscore(badmobile,rule3_mobile) # precision: 0.371  选中样本数70
fs_rule3_3 = fscore(badmobile,rule3_3mobile) #  precision: 0.548  选中样本数31

fs_rule4 = fscore(badmobile,rule4_mobile) #precision(选中样本逾期率): 0.137 选中样本数204
fs_rule4_sum3 = fscore(badmobile,rule4_sum3mobile) #precision(选中样本逾期率): 0.188 选中样本数69




fs_rule1 = fscore(badmobile,rule1_mobile4pass) # precision: 0.628  选中样本数164

fs_rule2 = fscore(badmobile,rule2_mobile4pass) #precision: 0.581  选中样本数 43
fs_rule2_sum3 = fscore(badmobile,rule2_sum3mobile4pass) # precision: 0.611 选中样本数 18

fs_rule3 = fscore(badmobile,rule3_mobile4pass) # precision: 0.765 选中样本数 34
fs_rule3_3 = fscore(badmobile,rule3_3mobile4pass) # precision: 0.850  选中样本数 20

fs_rule4 = fscore(badmobile,rule4_mobile4pass) # precision: 0.491  选中样本数 55
fs_rule4_sum3 = fscore(badmobile,rule4_sum3mobile4pass) # precision: 0.565  选中样本数 23



fs_rule1p = fscore(badmobile,rule1p_mobile4pass) # precision: 0.360 选中样本数 859

fs_rule2p = fscore(badmobile,rule2p_mobile4pass) # 
fs_rule2p_sum4 = fscore(badmobile,rule2p_sum4mobile4pass) # precision: 0.433  选中样本数 134

fs_rule3m = fscore(badmobile,rule3m_mobile4pass) # precision: 0.941  选中样本数 714
fs_rule3p = fscore(badmobile,rule3p_mobile4pass) # precision: 0.947 选中样本数 152   // 2
fs_rule3m_3 = fscore(badmobile,rule3m_3mobile4pass) # precision: 0.955 选中样本数 67 // 3

fs_rule4p = fscore(badmobile,rule4p_mobile4pass) # 
fs_rule4p_sum3 = fscore(badmobile,rule4p_sum3mobile4pass) # 




rule1234_mobile4pass = list((set(rule1_mobile4pass) & set(rule2_mobile4pass)  & set(rule4_mobile4pass)) | set(rule3_mobile4pass))
fs_rule12344pass = fscore(badmobile, rule1234_mobile4pass) #precision: 0.604  选中样本数 225  

# rule12_mobile = list(set(rule1_mobile) & set(rule2_mobile))
# rule23_mobile = list(set(rule2_mobile) & set(rule3_mobile))
# rule13_mobile = list(set(rule1_mobile) & set(rule3_mobile))
# fs_rule12 = fscore(badmobile, rule12_mobile)
# fs_rule23 = fscore(badmobile, rule23_mobile)
# fs_rule13 = fscore(badmobile, rule13_mobile)
# fs_rule123 = fscore(badmobile, rule123_mobile)

