返回所有n 个节点形成圈子的节点和他们之间的关系，例子n=3,4,5
get all nodes that circle in 3,4,5 links and get count the bidirection edges 


```python
closedr3 = graph1.run('MATCH (n1)-[r1:Contactp]->(n2)-[r2:Contactp]->(n3)-[r3:Contactp]->(n1) RETURN n1.mobile,n2.mobile,n3.mobile').to_data_frame()
closedr_try = graph1.run('MATCH (n1)-[r1:Contactp]->(n2)-[r2:Contactp]->(n3)-[r3:Contactp]->(n1) WHERE n1.mobile=895622295430 RETURN n1.mobile,n2.mobile,n3.mobile').to_data_frame()

def fullcols(dfa,num):
    fullcols = []
    for i in range(len(dfa)):
        if len(set(dfa.iloc[i,:].tolist())) < num:
            continue
        else:
            fullcols.append(i)
    return fullcols

closedr3_fullcols = fullcols(closedr3,3)
closedr3full = closedr3.iloc[closedr3_fullcols,:]

matcher1 = NodeMatcher(graph1)
relation_matcher = RelationshipMatcher(graph1)
# node1 = matcher1.match("Borrower").where(mobile=85703212509).first()
# node2 = matcher1.match("Borrower").where(mobile=895622295430).first()
# relationship1 = list(relation_matcher.match((node1,node2), r_type=None))
# relationship2 = list(relation_matcher.match((node2,node1), r_type=None))


del closedr3['cluster']
closedr3['cluster'] = closedr3.values.tolist()
closedr3['cluster'] = closedr3['cluster'].apply(lambda x: sorted(x))
closedr3['cluster'] = closedr3['cluster'].apply(lambda x: "".join(str(x)))
# pda = pd.DataFrame(np.array([a]), dtype='int64')
closedr3name = []
closedr3group = []
for name, group in closedr3.groupby(['cluster']):
    closedr3name.append(name)
    closedr3group.append(group)
    
df_closedr3 = pd.DataFrame(columns=['n1.mobile','n2.mobile','n3.mobile','cluster','count'])

for i in range(len(closedr3group)):
    ck_group = closedr3group[i].head(1)
    count = 0
    for j in range(3):
        if j == 2:
            node1mob = int(ck_group.iloc[0,j])
            node2mob = int(ck_group.iloc[0,0])
            node1 = matcher1.match("Borrower").where(mobile= node1mob).first()
            # print(node1)
            node2 = matcher1.match("Borrower").where(mobile= node2mob).first()
            # print(node2)
            relationship1 = list(relation_matcher.match((node1,node2), r_type=None))
            relationship2 = list(relation_matcher.match((node2,node1), r_type=None))
            print(len(relationship1))
            print(len(relationship2))
            if (len(relationship1) >0) & (len(relationship2)) > 0:
                count = count+1
            else:
                continue
        else:
            node1mob = int(ck_group.iloc[0,j])
            node2mob = int(ck_group.iloc[0,j+1])
            node1 = matcher1.match("Borrower").where(mobile= node1mob).first()
            # print(node1)
            node2 = matcher1.match("Borrower").where(mobile= node2mob).first()
            # print(node2)
            relationship1 = list(relation_matcher.match((node1,node2), r_type=None))
            relationship2 = list(relation_matcher.match((node2,node1), r_type=None))
            print(len(relationship1))
            print(len(relationship2))
            if (len(relationship1) >0) & (len(relationship2)) > 0:
                count = count+1
            else:
                continue
    print(f"count is {count}")
    ck_group['count'] = count
    df_closedr3 = df_closedr3.append(ck_group)
        

def neo4jfulldf2pydf(dfa, num):
    closedr = dfa.copy()
    closedr['cluster'] = closedr.values.tolist()
    closedr['cluster'] = closedr['cluster'].apply(lambda x: sorted(x))
    closedr['cluster'] = closedr['cluster'].apply(lambda x: "".join(str(x)))
# pda = pd.DataFrame(np.array([a]), dtype='int64')
    closedrname = []
    closedrgroup = []
    for name, group in closedr.groupby(['cluster']):
        closedrname.append(name)
        closedrgroup.append(group)
    mobilecolname = [f"n{i+1}.mobile" for i in range(num)]
    df_closedr = pd.DataFrame(columns = mobilecolname+ ['cluster','count'])

    for i in range(len(closedrgroup)):
        ck_group = closedrgroup[i].head(1)
        count = 0
        for j in range(num): # num = 4 (0-3)
            for k in range(num-j-1):
                node1mob = int(ck_group.iloc[0,j])
                node2mob = int(ck_group.iloc[0,k+j+1])
                node1 = matcher1.match("Borrower").where(mobile= node1mob).first()
                # print(node1)
                node2 = matcher1.match("Borrower").where(mobile= node2mob).first()
                # print(node2)
                relationship1 = list(relation_matcher.match((node1,node2), r_type=None))
                relationship2 = list(relation_matcher.match((node2,node1), r_type=None))
                print(len(relationship1))
                print(len(relationship2))
                if (len(relationship1) >0) & (len(relationship2)) > 0:
                    count = count+1
                else:
                    continue
        print(f"count is {count}")
        ck_group['count'] = count
        df_closedr = df_closedr.append(ck_group)
    return df_closedr




closedr4 = graph1.run('MATCH (n1)-[r1:Contactp]->(n2)-[r2:Contactp]->(n3)-[r3:Contactp]->(n4)-[r4:Contactp]->(n1) RETURN n1.mobile,n2.mobile,n3.mobile,n4.mobile').to_data_frame()
closedr4_fullcols = fullcols(closedr4,4)
closedr4full = closedr4.iloc[closedr4_fullcols,:]
df_closedr4 = neo4jfulldf2pydf(closedr4full, 4)


closedr5 = graph1.run('MATCH (n1)-[r1:Contactp]->(n2)-[r2:Contactp]->(n3)-[r3:Contactp]->(n4)-[r4:Contactp]->(n5)-[r5:Contactp]->(n1) RETURN n1.mobile,n2.mobile,n3.mobile,n4.mobile,n5.mobile').to_data_frame()
closedr5_fullcols = fullcols(closedr5,5)
closedr5full = closedr5.iloc[closedr5_fullcols,:]
df_closedr5 = neo4jfulldf2pydf(closedr5full, 5)


df_closedr3.to_csv("IDA_closedr3_20230611.csv",index=False)
df_closedr4.to_csv("IDA_closedr4_20230611.csv",index=False)
df_closedr5.to_csv("IDA_closedr5_20230611.csv",index=False)
```