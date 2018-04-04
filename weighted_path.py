from process import period_cmp, time_now
import create_graph
import math

def init_countrymen_cost(graph=None):
    """
    函数功能：连接在同一'Location'节点上的两条'is_from'关系相当于一个同乡关系，在所有'is_from'关系中设置代价属性'cost'，其值为5/(3*type_int*2)。
    type_int同乡关系类型（1-10）：中国节点上的同乡关系类型为1，地区节点每往下一级关系类型加1，最大为10。
    :param graph: 图数据库连接配置
    :return: True
    """
    # 从'中国'节点开始
    china = graph.data("MATCH (location:Location) WHERE location.name='中国' RETURN location")[0]['location']
    type_int = 1  # 地区节点层级
    cost = float(5 / type_int / 6)
    graph.data(f"MATCH {china}<-[r:is_from]-(:Person) SET r.cost={cost};")
    print(time_now(1), f":************************ {type_int} Level Location ************************")
    while type_int < 10:  # 按地区层级设置代价属性'cost'
        type_int += 1
        print(time_now(1), f":************************ {type_int} Level Location ************************")
        cost = float(5 / type_int / 6)  # 设置代价属性'cost'
        location = graph.data(f"MATCH {china}-[:include_location*{type_int-1}]->(location:Location)"
                              f"<-[r:is_from]-(:Person) SET r.cost={cost} RETURN location;")
        if not location:
            break
    return True

def init_schoolfellow_cost(graph=None, mysql=None):
    """
    函数功能：从MySQL获取数据，在所有'schoolfellow_with'关系中设置代价属性'cost'，其值为5/(4*(1+ln(n))*(6-type_int))，其中n为同校年数。
    type_int同学关系类型(1,2,3,4,5)，1：表示同学院且同级，2：表示同学院不同级但时间有重叠，3：表示不同学院但同级，4：表示不同学院不同级但时间有重叠，5：表示同校的其他情况。
    :param graph: 图数据库连接配置
    :param mysql: 关系型数据库连接配置
    :return: True
    """
    print(time_now(1), f":************************ Schoolfellow ************************")
    select_sql = "SELECT id_x, id_y, start_time, end_time, type_int FROM demo.schoolfellow;"
    my_cur = mysql.cursor()
    my_cur.execute(select_sql)
    count = 0
    for line in my_cur.fetchall():  # 对MySQL中的每条同学记录进行处理
        count += 1
        print(time_now(1), count, line)
        node1 = graph.data(f"MATCH (person:Person) WHERE person.id='{line[0]}' RETURN person")
        node2 = graph.data(f"MATCH (person:Person) WHERE person.id='{line[1]}' RETURN person")
        if node1 and node2:
            node1 = node1[0]['person']
            node2 = node2[0]['person']
            overlap = int(line[3][0:4]) - int(line[2][0:4])
            years = 1 if overlap <= 1 else overlap  # 同校年数
            cost = 5/(4*(1+math.log(years, math.e))*(6-float(line[4])))  # 设置代价属性'cost'
            create_graph.create_relationship(graph, node1, node2, 'schoolfellow_with', 'cost', cost)  # 创建同学关系
    return True

def init_workmate_cost(graph=None, mysql=None):
    """
    函数功能：从MySQL获取数据，在所有'workmate_with'关系中设置代价属性'cost'，其值为(type_int+1)/(5*(1+ln(n)))，其中n为同校年数。
    type_int同事关系类型[0,10]，n：表示x是y的第n层上级，0：表示x与y是同一级别。
    :param graph: 图数据库连接配置
    :param mysql: 关系型数据库连接配置
    :return: True
    """
    print(time_now(1), f":************************ Workmate ************************")
    select_sql = "SELECT id_x, id_y, start_time, end_time, type_int FROM demo.workmate;"
    my_cur = mysql.cursor()
    my_cur.execute(select_sql)
    count = 0
    for line in my_cur.fetchall():  # 对MySQL中的每条同事记录进行处理
        count += 1
        print(time_now(1), count, line)
        node1 = graph.data(f"MATCH (person:Person) WHERE person.id='{line[0]}' RETURN person")
        node2 = graph.data(f"MATCH (person:Person) WHERE person.id='{line[1]}' RETURN person")
        if node1 and node2:
            node1 = node1[0]['person']
            node2 = node2[0]['person']
            overlap = int(line[3][0:4]) - int(line[2][0:4])
            years = 1 if overlap <= 1 else overlap  # 同校年数
            cost = (float(line[4])+1)/(5*(1+math.log(years, math.e)))  # 设置代价属性'cost'
            create_graph.create_relationship(graph, node1, node2, 'workmate_with', 'cost', cost)  # 创建同事关系
    return True
