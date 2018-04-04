from process import Con_Neo4j, Con_MySQL
from py2neo import Relationship
import time
import create_graph
import search_graph
import weighted_path

option_neo4j = {'http': "http://opsrv.mapout.lan:7474", 'user': "neo4j", 'password': "Neo4j"}  # Neo4j图数据库连接配置
option_mysql = {'database': "demo", 'user': "root", 'password': "root", 'host': "opsrv.mapout.lan"}  # MySQL数据库连接配置

if __name__ == '__main__':
    time1 = time.clock()
    graph = Con_Neo4j(**option_neo4j)
    mysql = Con_MySQL(**option_mysql)

    # MySQL语句
    # select_sql = "SELECT id_index_new FROM demo.work_resume_copy2"
    # my_cur = mysql.cursor()  # 获取关系型数据库游标
    # my_cur.execute(select_sql)
    # count1 = count2 = count3 = 0
    # data = graph.data(f"MATCH ()-[r:work_at]->() RETURN r;")
    # temp = []
    # err = []
    # for r in data:
    #     count1 += 1
    #     print('Neo4j:', count1, r['r']['work_id'])
    #     temp.append(r['r']['work_id'])
    # for line in my_cur.fetchall():
    #     count2 += 1
    #     print('SQL:', count2, line[0])
    #     if line[0] not in temp:
    #         err.append(line[0])
    #         count3 += 1
    #         print(count3, line[0])
    # print(count1, count2, count3)
    # for e in err:
    #     print(e)
    # my_cur.close()

    # count = 0
    # for person in graph.find(label='Person'):
    #     count += 1
    #     print(f":************************ {count} Person:{person['name']} ************************")
    #     graph.data(f"MATCH {person}-[r:countrymen_with]-(:Person) DELETE r")

    # node1 = graph.find_one(label='Person', property_key='id', property_value='00200000000000013000')  # 查找节点1
    # print(node1)
    # if not graph.data(f"MATCH {node1}-[r:workmate_with]->(n) RETURN r"):
    #     print('666')
    # print(graph.data(f"MATCH ()-[r:workmate_with|schoolfellow_with|countrymen_with]->() DELETE r"))
    # for rel in graph.data("MATCH (n)-[r:workmate_with]->(m) RETURN n,r,m"):
    #     print(rel['n'], rel['r'], rel['m'])
    # for node in graph.data("MATCH (n:Person) WHERE n.id='00200000000000013000'RETURN n"):
    #     print(node['n'])

    # create_graph.add_init(['00261000000000308000', '00262000000036643000'], graph, mysql)

    # 初始化人物关系
    # create_graph.init_countrymen(graph, mysql)
    # create_graph.init_schoolfellow(graph, mysql)
    # create_graph.init_workmate(1, graph, mysql)

    # for data in search_graph.select_schoolfellow(person_id='00262030000030106000', graph=graph):
    #     print(data)
    # count = 0
    # for data in search_graph.select_countrymen(person_id='00261000000000308000', graph=graph):
    #     count += 1
    #     print(count, data)

    # weighted_path.init_countrymen_cost(graph)
    # weighted_path.init_schoolfellow_cost(graph, mysql)
    # weighted_path.init_workmate_cost(graph, mysql)


    # for person in search_graph.select_schoolfellow_multi('00215030000011596000', '00333333300000009000', '00333333300000009000', graph):
    #     print(person)
    # print(graph.data("MATCH ()-[r]->() WHERE r.work_id='00215000000020780029' RETURN id(r)"))
    # print(graph.data("MATCH ()-[r]->() WHERE r.work_id='00211000000041483016' RETURN id(r)"))
    # i = 1
    # for location in graph.find(label='Academy'):
    #     print(i, location)
    #     i += 1


    # node1 = graph.find_one(label='Location', property_key='name', property_value='中国')  # 查找节点
    # node2 = graph.find_one(label='Location', property_key='name', property_value='广东省')  # 查找节点2
    # node3 = graph.find_one(label='Location', property_key='location_name', property_value='柳州')  # 查找节点3
    # node4 = graph.find_one(label='Location', property_key='location_name', property_value='广州')  # 查找节点1
    # paths = search_graph.searchAndSave_allShortestPaths(graph, mysql, '00215042200007310000', '00261000000000308000', 10, 5)

    # person1 = graph.data("MATCH (n:Person) WHERE n.id='00261000000000308000' RETURN n")[0]['n']
    # person2 = graph.data("MATCH (n:Person) WHERE n.id='00200000000000255000' RETURN n")[0]['n']
    # if not graph.data(f"MATCH {person1}-[r:countrymen_with]-{person2} RETURN r"):  # 如果不存在关系，则创建关系
    #     print('666')

    # test1 = search_graph.allShortestPaths(graph, '谢商华', '周峰越', 10, 5,  'Person', 'name', 'Person', 'name', None)
    # test1 = search_graph.dijkstraWithDefaultWeight(graph, '00215042200007310000', '00261000000000308000')
    # print('Paths:', test1[0], test1[1], test1[2])
    # test2 = graph.match(end_node=node1, rel_type='include_location', bidirectional=False)
    # for nodes in test2:
    #     print(nodes)
    # new_node = Node('Location')  # 定义新节点
    # new_node['location_name'] = '广州'  # 属性赋值
    # print(graph.create(new_node))  # 创建新节点
    # for nodes in graph.data(f"MATCH ()-[r]-() WHERE exists(r.end_time) RETURN r LIMIT 100;"):
    #     print(nodes['r']['end_time'])
    #
    # my_cur = mysql.cursor()  # 获取关系数据库游标
    # insert_sql_countrymen = "INSERT INTO demo.countrymen(id_x, id_y, name_x, name_y, place_of_birth_id, " \
    #                         "place_of_birth, type_int) VALUES ('{0}', '{1}', '{2}', '{3}', '{4}', '{5}', '{6}');"
    # finish_insert_sql = insert_sql_countrymen.format('123', '321', '张三', '李四', '666', '广州市', 1, )
    # try:
    #     my_cur.execute(finish_insert_sql)
    #     mysql.commit()
    #     print('Countrymen-MySQL Insert Successful:', '123', '321')
    # except:
    #     mysql.rollback()  # 插入失败，执行回滚操作
    #     print('Countrymen-MySQL Insert Error:', '123', '321')
    # my_cur.close()
    # mysql.close()

    mysql.close()
    time2 = time.clock()
    print('Execution Time:', time2 - time1, 's')