import math

from py2neo import Relationship

from process import Con_Neo4j, Con_MySQL, period_cmp, time_now

"""这是一个功能包，包含关系初始化创建模块、关系增量创建模块。"""


# --------------------------------------------------------------------
# 连接Neo4j和MySQL
graph = Con_Neo4j(http="http://opsrv.mapout.lan:7474", username="neo4j", password="Neo4j")  # 连接图数据库
mysql = Con_MySQL(database="demo", user="root", password="root", host="opsrv.mapout.lan", port=3306,
                  charset="utf8")  # 连接关系型数据库


# --------------------------------------------------------------------
# 初始化创建关系
def create_relationship(node1=None, node2=None, rel_type=None, property=None, value=None):
    """
    函数功能：在图中创建关系，并可设置属性。
    :param node1: 节点1
    :param node2: 节点2
    :param rel_type: 关系类型
    :param property: 关系属性
    :param value: 关系属性值
    :return: True或False表示创建关系成功或失败
    """
    if not node1 == node2:
        r = graph.data(f"MATCH {node1}-[r:{rel_type}]-{node2} RETURN r")
    else:
        return False
    if not r:  # 如果不存在关系，则创建关系
        rel = Relationship(node1, rel_type, node2)
        if property == 'cost':  # 添加'cost'属性
            rel[property] = float(value)
        graph.create(rel)
        print(time_now(1), ':Create relationship:', node1['name'], rel_type, node2['name'], property, value)
        return True
    elif property == 'cost':  # 如果存在关系，且需要设置'cost'属性
        # 若关系中已存在'cost'属性，则降低，否则设置
        value = 1 / (1 / float(r[0]['r']['cost']) + 1 / value) if 'cost' in r[0]['r'].keys() else float(value)
        graph.data(f"MATCH {node1}-[r:{rel_type}]-{node2} SET r.cost={value}")
        print(time_now(1), ':Update relationship:', node1['name'], rel_type, node2['name'], property, value)
        return True
    else:
        return False


def init_countrymen():
    """此方法暂时停用
    函数功能：在图中查找所有地点，将每个地点上的所有人物视为同乡，最后结果保存在关系型数据库中。
    在对应人物节点之间添加同乡关系并设置代价属性'cost'，其值为5/(3*type_int*2)。
    type_int同乡关系类型(1-10)：中国节点上的同乡关系类型为1，地区节点每往下一级关系类型加1，最大为10。
    :return: True
    """
    my_cur = mysql.cursor()  # 获取关系型数据库游标
    select_sql = "SELECT * FROM demo.countrymen WHERE id_x='{0}' AND id_y='{1}' OR id_x='{1}' AND id_y='{0}';"
    # MySQL插入语句，保存同乡关系
    insert_sql = "INSERT INTO demo.countrymen(id_x, id_y, name_x, name_y, place_of_birth_id, place_of_birth, type_int) VALUES"
    insert_value = ''  # 初始化SQL语句拼接字段
    count = 0  # 地点数量计数器
    rel_num = 0  # 创建的关系计数器
    trigger = 0  # MySQL提交数量累加器
    for location in graph.find(label='Location'):
        count += 1
        print(time_now(1), f":************************ {count} Location:{location['name']} ************************")
        # if count < 1488:
        #     continue
        type_int = 1  # 同乡关系类型
        while type_int < 10:
            if graph.data(f"MATCH {location}<-[:include_location*{type_int-1}]-(location:Location) "
                          f"RETURN location;")[0]['location']['name'] == '中国':
                break
            type_int += 1
        person_group = graph.data(f"MATCH {location}<-[:is_from]-(person:Person) RETURN person")  # 用来保存人物的列表
        for i in range(len(person_group) - 1):  # 给人物两两创建同乡关系
            for j in range(i + 1, len(person_group)):
                if not person_group[i]['person']['id'] == person_group[j]['person']['id'] and not \
                        my_cur.execute(
                            select_sql.format(person_group[i]['person']['id'], person_group[j]['person']['id'])):
                    create_relationship(graph, person_group[i]['person'], person_group[j]['person'],
                                        'countrymen_with', 'cost', 5 / (3 * type_int))  # 创建同乡关系
                    # 插入关系型数据库保存结果
                    insert_value = insert_value + \
                                   f" ('{person_group[i]['person']['id']}','{person_group[j]['person']['id']}'," \
                                   f"'{person_group[i]['person']['name']}','{person_group[j]['person']['name']}'," \
                                   f"'{location['id']}','{location['name']}','{type_int}'),"
                    rel_num += 1
                    trigger += 1
                    if trigger >= 100:
                        finish_insert_sql = insert_sql + insert_value
                        finish_insert_sql = finish_insert_sql[:-1] + ';'
                        try:
                            my_cur.execute(finish_insert_sql)
                            mysql.commit()
                            print(time_now(1), ':MySQL Insert Successful:', trigger, '\tCounts:', rel_num)
                            insert_value = ''
                            trigger = 0
                        except:
                            mysql.rollback()  # 插入失败，执行回滚操作
                            print(time_now(1), ':\t\tMySQL Insert Error At Counts:', trigger, 'in', rel_num)
    if trigger > 0:
        finish_insert_sql = insert_sql + insert_value
        finish_insert_sql = finish_insert_sql[:-1] + ';'
        try:
            my_cur.execute(finish_insert_sql)
            mysql.commit()
            print(time_now(1), ':MySQL Insert Successful:', trigger, '\tCounts:', rel_num)
        except:
            mysql.rollback()  # 插入失败，执行回滚操作
            print(time_now(1), ':\t\tMySQL Insert Error:', trigger, 'in', rel_num)
    my_cur.close()
    return True


def init_schoolfellow():
    """
    函数功能：在图中查找所有学校节点（School），匹配连接到每个学校及其学院的学生教育经历，如果就读时间段存在重叠的就视为同学关系，学校可能包含多个学院（Academy），但只有在同一School中的学生才能算同学，最后结果保存在关系型数据库中。
    在对应人物节点之间添加同学关系并设置代价属性'cost'，其值为5/(4*(1+ln(n))*(6-type_int))，其中n为同校年数。
    type_int同学关系类型(1,2,3,4,5)，1：表示同学院且同级，2：表示同学院不同级但时间有重叠，3：表示不同学院但同级，4：表示不同学院不同级但时间有重叠，5：表示同校的其他情况。
    :return: True
    """
    my_cur = mysql.cursor()  # 获取关系型数据库游标
    select_sql = "SELECT * FROM demo.schoolfellow WHERE id_x_index_n='{0}' AND id_y_index_n='{1}' OR id_x_index_n='{1}' AND id_y_index_n='{0}';"
    # MySQL插入语句，保存同学关系
    insert_sql = "INSERT INTO demo.schoolfellow(id_x_index_n, id_y_index_n, id_x, id_y, name_x, name_y, " \
                 "school_x_id, school_x, academy_x_id, academy_x, school_y_id, school_y, academy_y_id, academy_y, " \
                 "start_time, end_time, type_int) VALUES"
    insert_value = ''  # 初始化SQL语句拼接字段
    count = 0  # 学校数量计数器
    rel_num = 0  # 创建的关系计数器
    trigger = 0  # MySQL提交数量累加器
    for node_school in graph.find(label='School'):  # 查找学校节点
        count += 1
        print(time_now(1), f":************************ {count} School:{node_school['name']} ************************")
        # 查找就读于当前学校的所有人物
        person_group = graph.data(f"MATCH (person:Person)-[r:study_at]->{node_school} RETURN person, r;") \
                       + graph.data(
            f"MATCH (person:Person)-[r:study_at]->(academy:Academy)<-[:include_academy]-{node_school} "
            f"RETURN person, r, academy;")
        for i in range(len(person_group) - 1):  # 匹配教育经历时间，并创建同学关系
            if person_group[i]['r']['start_time'][0] == '0':  # 若开始时间年份为‘0’(数据不完整)，则忽略该条记录
                continue
            if person_group[i]['r']['end_time'][0] == '0':  # 结束时间年份为‘0’的表示至今，按格式更改为当前时间
                person_group[i]['r']['end_time'] = time_now(0)  # 当前日期
            for j in range(i + 1, len(person_group)):
                if person_group[i]['person']['id'] == person_group[j]['person']['id'] or \
                        person_group[i]['r']['study_id'] == person_group[j]['r']['study_id'] or \
                        person_group[j]['r']['start_time'][0] == '0':  # 若ID重复或开始时间年份为‘0’(数据不完整)，则忽略该条记录
                    continue
                if person_group[j]['r']['end_time'][0] == '0':  # 结束时间年份为‘0’的表示至今，按格式更改为当前时间
                    person_group[j]['r']['end_time'] = time_now(0)  # 当前日期
                # 匹配时间段重叠情况
                overlap = period_cmp(person_group[i]['r']['start_time'], person_group[i]['r']['end_time'],
                                     person_group[j]['r']['start_time'], person_group[j]['r']['end_time'])
                if overlap:
                    if person_group[i]['r']['start_time'][0:4] == person_group[j]['r']['start_time'][0:4]:
                        if len(person_group[i]) == 3 and len(person_group[j]) == 3 \
                                and person_group[i]['academy']['id'] == person_group[j]['academy']['id']:
                            type_int = 1  # 同学关系类型0：同学院且同级
                        else:
                            type_int = 3  # 同学关系类型2：不同学院但同级
                    elif len(person_group[i]) == 3 and len(person_group[j]) == 3 \
                            and person_group[i]['academy']['id'] == person_group[j]['academy']['id']:
                        type_int = 2  # 同学关系类型1：同学院不同级但时间有重叠
                    else:
                        type_int = 4  # 同学关系类型3：不同学院不同级但时间有重叠
                else:
                    type_int = 5  # 同学关系类型4：同校的其他情况
                if overlap:
                    if not my_cur.execute(
                            select_sql.format(person_group[i]['r']['study_id'], person_group[j]['r']['study_id'])):
                        create_relationship(graph, person_group[i]['person'], person_group[j]['person'],
                                            'schoolfellow_with', 'cost', 5 / (4 * (1 + math.log(overlap[2], math.e)) * (
                                        6 - float(type_int))))  # 创建同学关系
                        # 插入关系型数据库保存结果
                        insert_value = insert_value + \
                                       f" ('{person_group[i]['r']['study_id']}','{person_group[j]['r']['study_id']}'," \
                                       f"'{person_group[i]['person']['id']}','{person_group[j]['person']['id']}'," \
                                       f"'{person_group[i]['person']['name']}','{person_group[j]['person']['name']}'," \
                                       f"'{node_school['id']}','{node_school['name']}'," + \
                                       'NULL,' if len(
                            person_group[i]) == 2 else f"'{person_group[i]['academy']['id']}'," + \
                                                       'NULL,' if len(
                            person_group[i]) == 2 else f"'{person_group[i]['academy']['name']}'," + \
                                                       f"'{node_school['id']}','{node_school['name']}'," + \
                                                       'NULL,' if len(
                            person_group[j]) == 2 else f"'{person_group[j]['academy']['id']}'," + \
                                                       'NULL,' if len(
                            person_group[j]) == 2 else f"'{person_group[j]['academy']['name']}'," + \
                                                       f"'{overlap[0]}','{overlap[1]}','{type_int}'),"
                        rel_num += 1
                        trigger += 1
                        if trigger >= 100:
                            finish_insert_sql = insert_sql + insert_value
                            finish_insert_sql = finish_insert_sql[:-1] + ';'
                            try:
                                my_cur.execute(finish_insert_sql)
                                mysql.commit()
                                print(time_now(1), ':MySQL Insert Successful:', trigger, '\tCounts:', rel_num)
                                insert_value = ''
                                trigger = 0
                            except:
                                mysql.rollback()  # 插入失败，执行回滚操作
                                print(time_now(1), ':\t\tMySQL Insert Error At Counts:', trigger, 'in', rel_num)
    if trigger > 0:
        finish_insert_sql = insert_sql + insert_value
        finish_insert_sql = finish_insert_sql[:-1] + ';'
        try:
            my_cur.execute(finish_insert_sql)
            mysql.commit()
            print(time_now(1), ':MySQL Insert Successful:', trigger, '\tCounts:', rel_num)
        except:
            mysql.rollback()  # 插入失败，执行回滚操作
            print(time_now(1), ':\t\tMySQL Insert Error:', trigger, 'in', rel_num)
    my_cur.close()
    return True


def init_workmate(max_level=1):
    """
    函数功能：在图中查找所有机构节点（Institution），匹配连接到这一机构所有职位的工作经历，机构节点下的顶级职位视为同一级，最后结果保存在关系型数据库中。
    在图中查找所有职位节点（Position），匹配连接到这一机构所有职位的工作经历，机构节点下的顶级职位视为同一级，最后结果保存在关系型数据库中。
    在对应人物节点之间添加同事关系并设置代价属性'cost'，其值为(type_int+1)/(5*(1+ln(n)))，其中n为同事年数。
    type_int同事关系类型(0-10)，n：表示x是y的第n层上级，0：表示x与y是同一级别。双向关系只保存一条记录。
    :param max_level: 最大允许查找上下级的层数，缺省值为1
    :return: True
    """
    my_cur = mysql.cursor()  # 获取关系型数据库游标
    select_sql = "SELECT * FROM demo.workmate WHERE id_x_index_n='{0}' AND id_y_index_n='{1}' OR id_x_index_n='{1}' AND id_y_index_n='{0}';"
    # MySQL插入语句，保存同事关系
    insert_sql = "INSERT INTO demo.workmate(id_x_index_n, id_y_index_n, id_x, id_y, name_x, name_y, " \
                 "institution_x_id, institution_x, position_x_id, position_x, institution_y_id, institution_y, " \
                 "position_y_id, position_y, start_time, end_time, type_int) VALUES"
    insert_value = ''  # 初始化SQL语句拼接字段
    count = 0  # 机构数量计数器
    rel_num = 0  # 创建的关系计数器
    trigger = 0  # MySQL提交数量累加器
    for node_institution in graph.find(label='Institution'):  # 查找所有机构节点
        count += 1
        print(time_now(1),
              f":************************ {count} Institution:{node_institution['name']} ************************")
        # if count < 23059:
        #     continue
        # 查找工作于当前机构顶层职位的所有人物
        worker_group = graph.data(
            f"MATCH (person:Person)-[r:work_at]->(position:Position)<-[:include_position]-{node_institution} "
            f"RETURN person, r, position;")
        for i in range(len(worker_group) - 1):  # 匹配工作经历时间，并创建同事关系
            if worker_group[i]['r']['start_time'][0] == '0':  # 若开始时间年份为‘0’(数据不完整)，则忽略该条记录
                continue
            if worker_group[i]['r']['end_time'][0] == '0':  # 结束时间年份为‘0’的表示至今，按格式更改为当前时间
                worker_group[i]['r']['end_time'] = time_now(0)  # 当前日期
            for j in range(i + 1, len(worker_group)):
                if worker_group[i]['r']['work_id'] == worker_group[j]['r']['work_id'] or \
                        worker_group[i]['person']['id'] == worker_group[j]['person']['id'] or \
                        worker_group[j]['r']['start_time'][0] == '0':  # 若ID重复或开始时间年份为‘0’(数据不完整)，则忽略该条记录
                    continue
                if worker_group[j]['r']['end_time'][0] == '0':  # 结束时间年份为‘0’的表示至今，按格式更改为当前时间
                    worker_group[j]['r']['end_time'] = time_now(0)  # 当前日期
                    # 匹配时间段重叠情况
                    overlap = period_cmp(worker_group[i]['r']['start_time'], worker_group[i]['r']['end_time'],
                                         worker_group[j]['r']['start_time'], worker_group[j]['r']['end_time'])
                    if overlap:
                        if not my_cur.execute(
                                select_sql.format(worker_group[i]['r']['work_id'], worker_group[j]['r']['work_id'])):
                            create_relationship(graph, worker_group[i]['person'], worker_group[j]['person'],
                                                'workmate_with', 'cost',
                                                1 / (5 * (1 + math.log(overlap[2], math.e))))  # 创建同事关系
                            # 插入关系型数据库保存结果
                            insert_value = insert_value + \
                                           f" ('{worker_group[i]['r']['work_id']}','{worker_group[j]['r']['work_id']}'," \
                                           f"'{worker_group[i]['person']['id']}','{worker_group[j]['person']['id']}'," \
                                           f"'{worker_group[i]['person']['name']}','{worker_group[j]['person']['name']}'," \
                                           f"'{node_institution['id']}','{node_institution['name']}'," \
                                           f"'{worker_group[i]['position']['id']}','{worker_group[i]['position']['name']}'," \
                                           f"'{node_institution['id']}','{node_institution['name']}'," \
                                           f"'{worker_group[j]['position']['id']}','{worker_group[j]['position']['name']}'," \
                                           f"'{overlap[0]}','{overlap[1]}','0'),"
                            rel_num += 1
                            trigger += 1
                            if trigger >= 100:
                                finish_insert_sql = insert_sql + insert_value
                                finish_insert_sql = finish_insert_sql[:-1] + ';'
                                try:
                                    my_cur.execute(finish_insert_sql)
                                    mysql.commit()
                                    print(time_now(1), ':MySQL Insert Successful:', trigger, '\tCounts:', rel_num)
                                    insert_value = ''
                                    trigger = 0
                                except:
                                    mysql.rollback()  # 插入失败，执行回滚操作
                                    print(time_now(1), ':\t\tMySQL Insert Error At Counts:', trigger, 'in', rel_num)
        # 查找当前机构节点下的所有职位
        position_group = graph.data(
            f"MATCH (position:Position)<-[:include_position*..]-{node_institution} RETURN position;")
        for position_up in position_group:  # 查找每个职位节点的多层下级关系
            position_up = position_up['position']
            workers = graph.data(f"MATCH (person:Person)-[r:work_at]->{position_up} RETURN person, r;")
            if not \
                    graph.data(
                        f"MATCH (institution:Institution)-[:include_position]->{position_up} RETURN institution;")[0]:
                for i in range(len(workers) - 1):  # 匹配当前职位中的工作经历时间，并创建同事关系
                    if workers[i]['r']['start_time'][0] == '0':  # 若开始时间年份为‘0’(数据不完整)，则忽略该条记录
                        continue
                    if workers[i]['r']['end_time'][0] == '0':  # 结束时间年份为‘0’的表示至今，按格式更改为当前时间
                        workers[i]['r']['end_time'] = time_now(0)  # 当前日期
                    for j in range(i + 1, len(workers)):
                        if workers[i]['r']['work_id'] == workers[j]['r']['work_id'] or \
                                workers[i]['person']['id'] == workers[j]['person']['id'] or \
                                workers[j]['r']['start_time'][0] == '0':  # 若ID重复或开始时间年份为‘0’(数据不完整)，则忽略该条记录
                            continue
                        if workers[j]['r']['end_time'][0] == '0':  # 结束时间年份为‘0’的表示至今，按格式更改为当前时间
                            workers[j]['r']['end_time'] = time_now(0)  # 当前日期
                            # 匹配时间段重叠情况
                            overlap = period_cmp(workers[i]['r']['start_time'], workers[i]['r']['end_time'],
                                                 workers[j]['r']['start_time'], workers[j]['r']['end_time'])
                            if overlap:
                                if not my_cur.execute(
                                        select_sql.format(workers[i]['r']['work_id'], workers[j]['r']['work_id'])):
                                    create_relationship(graph, workers[i]['person'], workers[j]['person'],
                                                        'workmate_with', 'cost',
                                                        1 / (5 * (1 + math.log(overlap[2], math.e))))  # 创建同事关系
                                    # 插入关系型数据库保存结果
                                    insert_value = insert_value + \
                                                   f" ('{workers[i]['r']['work_id']}','{workers[j]['r']['work_id']}'," \
                                                   f"'{workers[i]['person']['id']}','{workers[j]['person']['id']}'," \
                                                   f"'{workers[i]['person']['name']}','{workers[j]['person']['name']}'," \
                                                   f"'{node_institution['id']}','{node_institution['name']}'," \
                                                   f"'{position_up['id']}','{position_up['name']}'," \
                                                   f"'{node_institution['id']}','{node_institution['name']}'," \
                                                   f"'{position_up['id']}','{position_up['name']}'," \
                                                   f"'{overlap[0]}','{overlap[1]}','0'),"
                                    rel_num += 1
                                    trigger += 1
                                    if trigger >= 100:
                                        finish_insert_sql = insert_sql + insert_value
                                        finish_insert_sql = finish_insert_sql[:-1] + ';'
                                        try:
                                            my_cur.execute(finish_insert_sql)
                                            mysql.commit()
                                            print(time_now(1), ':MySQL Insert Successful:', trigger, '\tCounts:',
                                                  rel_num)
                                            insert_value = ''
                                            trigger = 0
                                        except:
                                            mysql.rollback()  # 插入失败，执行回滚操作
                                            print(time_now(1), ':\t\tMySQL Insert Error At Counts:', trigger, 'in',
                                                  rel_num)
            type_int = 0  # 控制往下查找的层级数
            while type_int < max_level:  # 查找当前节点的多层下级关系
                type_int += 1
                positions_down = graph.data(f"MATCH (position:Position)<-[:include_position*{type_int}]-{position_up} "
                                            f"RETURN position;")
                if not positions_down:  # 当没有更低级职位时退出
                    break
                for position in positions_down:
                    position = position['position']
                    worker_group = graph.data(f"MATCH (person:Person)-[r:work_at]->{position} RETURN person, r;")
                    for i in range(len(workers) - 1):  # 匹配上级职位与该下级职位上的工作经历时间，并创建同事关系
                        if workers[i]['r']['start_time'][0] == '0':  # 若开始时间年份为‘0’(数据不完整)，则忽略该条记录
                            continue
                        if workers[i]['r']['end_time'][0] == '0':  # 结束时间年份为‘0’的表示至今，按格式更改为当前时间
                            workers[i]['r']['end_time'] = time_now(0)  # 当前日期
                        for j in range(len(worker_group) - 1):
                            if workers[i]['r']['work_id'] == workers[j]['r']['work_id'] or \
                                    workers[i]['person']['id'] == workers[j]['person']['id'] or \
                                    workers[j]['r']['start_time'][0] == '0':  # 若ID重复或开始时间年份为‘0’(数据不完整)，则忽略该条记录
                                continue
                            if worker_group[j]['r']['end_time'][0] == '0':  # 结束时间年份为‘0’的表示至今，按格式更改为当前时间
                                worker_group[j]['r']['end_time'] = time_now(0)  # 当前日期
                                # 匹配时间段重叠情况
                                overlap = period_cmp(workers[i]['r']['start_time'], workers[i]['r']['end_time'],
                                                     worker_group[j]['r']['start_time'],
                                                     worker_group[j]['r']['end_time'])
                                if overlap:
                                    if not my_cur.execute(select_sql.format(workers[i]['r']['work_id'],
                                                                            worker_group[j]['r']['work_id'])):
                                        create_relationship(graph, workers[i]['person'], worker_group[j]['person'],
                                                            'workmate_with', 'cost', (type_int + 1) / (5 * (
                                                    1 + math.log(overlap[2], math.e))))  # 创建同事关系
                                        # 插入关系型数据库保存结果
                                        insert_value = insert_value + \
                                                       f" ('{workers[i]['r']['work_id']}','{worker_group[j]['r']['work_id']}'," \
                                                       f"'{workers[i]['person']['id']}','{worker_group[j]['person']['id']}'," \
                                                       f"'{workers[i]['person']['name']}','{worker_group[j]['person']['name']}'," \
                                                       f"'{node_institution['id']}','{node_institution['name']}'," \
                                                       f"'{position_up['id']}','{position_up['name']}'," \
                                                       f"'{node_institution['id']}','{node_institution['name']}'," \
                                                       f"'{position['id']}','{position['name']}'," \
                                                       f"'{overlap[0]}','{overlap[1]}','{type_int}'),"
                                        rel_num += 1
                                        trigger += 1
                                        if trigger >= 100:
                                            finish_insert_sql = insert_sql + insert_value
                                            finish_insert_sql = finish_insert_sql[:-1] + ';'
                                            try:
                                                my_cur.execute(finish_insert_sql)
                                                mysql.commit()
                                                print(time_now(1), ':MySQL Insert Successful:', trigger, '\tCounts:',
                                                      rel_num)
                                                insert_value = ''
                                                trigger = 0
                                            except:
                                                mysql.rollback()  # 插入失败，执行回滚操作
                                                print(time_now(1), ':\t\tMySQL Insert Error At Counts:', trigger, 'in',
                                                      rel_num)
    if trigger > 0:
        finish_insert_sql = insert_sql + insert_value
        finish_insert_sql = finish_insert_sql[:-1] + ';'
        try:
            my_cur.execute(finish_insert_sql)
            mysql.commit()
            print(time_now(1), ':MySQL Insert Successful:', trigger, '\tCounts:', rel_num)
        except:
            mysql.rollback()  # 插入失败，执行回滚操作
            print(time_now(1), ':\t\tMySQL Insert Error:', trigger, 'in', rel_num)
    my_cur.close()
    return True


# --------------------------------------------------------------------
# 增量创建关系
def add_init(add_id_group=None):
    """
    函数功能：根据新增加的人物ID列表，对这些人的三种关系及其代价值进行增量计算（同乡关系已停用）。
    :param add_id_group:新增人物ID列表
    :return: True
    """
    my_cur = mysql.cursor()  # 获取关系型数据库游标
    # selece_sql_countrymen = "SELECT * FROM demo.countrymen WHERE id_x='{0}' AND id_y='{1}' OR id_x='{1}' AND id_y='{0}'"
    selece_sql_schoolfellow = "SELECT * FROM demo.schoolfellow WHERE id_x_index_n='{0}' AND id_y_index_n='{1}' OR id_x_index_n='{1}' AND id_y_index_n='{0}'"
    selece_sql_workmate = "SELECT * FROM demo.workmate WHERE id_x_index_n='{0}' AND id_y_index_n='{1}' OR id_x_index_n='{1}' AND id_y_index_n='{0}'"
    # MySQL插入语句，保存同乡关系
    # insert_sql_countrymen = "INSERT INTO demo.countrymen(id_x, id_y, name_x, name_y, place_of_birth_id, " \
    #                         "place_of_birth, type_int) VALUES"
    # MySQL插入语句，保存同学关系
    insert_sql_schoolfellow = "INSERT INTO demo.schoolfellow(id_x_index_n, id_y_index_n, id_x, id_y, name_x, name_y, " \
                              "school_x_id, school_x, academy_x_id, academy_x, school_y_id, school_y, academy_y_id, academy_y, " \
                              "start_time, end_time, type_int) VALUES"
    # MySQL插入语句，保存同事关系
    insert_sql_workmate = "INSERT INTO demo.workmate(id_x_index_n, id_y_index_n, id_x, id_y, name_x, name_y, " \
                          "institution_x_id, institution_x, position_x_id, position_x, institution_y_id, institution_y, " \
                          "position_y_id, position_y, start_time, end_time, type_int) VALUES"
    count = 0  # 新增人物数量计数器
    node_group = []
    for add_id in add_id_group:  # 保存增加的人物节点
        node_group.append(graph.find_one(label='Person', property_key='id', property_value=add_id))
    for node in node_group:  # 对每个增加的人物节点进行关系查找
        location_level = 1  # 地区节点层级
        while location_level < 10:  # 确定人物所属地区层级
            locations = graph.data(f"MATCH {location}<-[:include_location*{location_level-1}]-(location:Location)"
                                   f"RETURN location;")
            if locations[0]['location']['name'] == '中国':
                break
            location_level += 1
        # 创建'if_from'关系中的代价属性'cost'，用来代替同乡关系
        graph.data(
            f"MATCH {locations[0]['location']}<-[r:is_from]-{node} SET r.cost={float(5/location_level/6)} RETURN r;")

        insert_value = ''  # 初始化SQL语句拼接字段
        trigger = 0  # MySQL提交数量累加器
        count += 1
        print(time_now(1), f":************************ {count} Person:{node} ************************")

        # 此步骤暂时停用
        # ******建立该人物的同乡关系
        # rel_num_countrymen = 0  # 创建的同乡关系计数器
        # for countrymen in create_one_countrymen(node, graph):
        #     if not countrymen[0] == countrymen[1] and \
        #             not my_cur.execute(selece_sql_countrymen.format(countrymen[0], countrymen[1])):
        #         # 插入关系型数据库保存结果
        #         insert_value = insert_value + f" ('{countrymen[0]}','{countrymen[1]}','{countrymen[2]}'," \
        #                                 f"'{countrymen[3]}','{countrymen[4]}','{countrymen[5]}','{countrymen[6]}'),"
        #         rel_num_countrymen += 1
        #         trigger += 1
        #         if trigger >= 100:
        #             finish_insert_sql = insert_sql_countrymen + insert_value
        #             finish_insert_sql = finish_insert_sql[:-1] + ';'
        #             try:
        #                 my_cur.execute(finish_insert_sql)
        #                 mysql.commit()
        #                 print(time_now(1), ':Countrymen-MySQL Insert Successful:', trigger, '\tCounts:', rel_num_countrymen)
        #                 insert_value = ''
        #                 trigger = 0
        #             except:
        #                 mysql.rollback()  # 插入失败，执行回滚操作
        #                 print(time_now(1), ':\t\tCountrymen-MySQL Insert Error At Counts:', trigger, 'in', rel_num_countrymen)
        # if trigger > 0:
        #     finish_insert_sql = insert_sql_countrymen + insert_value
        #     finish_insert_sql = finish_insert_sql[:-1] + ';'
        #     try:
        #         my_cur.execute(finish_insert_sql)
        #         mysql.commit()
        #         print(time_now(1), ':Countrymen-MySQL Insert Successful:', trigger, '\tCounts:', rel_num_countrymen)
        #         insert_value = ''
        #         trigger = 0
        #     except:
        #         mysql.rollback()  # 插入失败，执行回滚操作
        #         print(time_now(1), ':\t\tCountrymen-MySQL Insert Error At Counts:', trigger, 'in', rel_num_countrymen)
        #         insert_value = ''
        #         trigger = 0

        # ******建立该人物的同学关系
        rel_num_schoolfellow = 0  # 创建的同学关系计数器
        for schoolfellow in create_one_schoolfellow(node, graph):
            if not schoolfellow[0] == schoolfellow[1] and \
                    not my_cur.execute(selece_sql_schoolfellow.format(schoolfellow[0], schoolfellow[1])):
                # 插入关系型数据库保存结果
                insert_value = insert_value + f" ('{schoolfellow[0]}','{schoolfellow[1]}','{schoolfellow[2]}'," \
                                              f"'{schoolfellow[3]}','{schoolfellow[4]}','{schoolfellow[5]}'," \
                                              f"'{schoolfellow[6]}','{schoolfellow[7]}'," + \
                               f"{schoolfellow[8]}," if schoolfellow[8] == 'NULL' else f"'{schoolfellow[8]}'," + \
                                                                                       f"{schoolfellow[9]}," if \
                    schoolfellow[9] == 'NULL' else f"'{schoolfellow[9]}'," + \
                                                   f"'{schoolfellow[10]}','{schoolfellow[11]}'," + \
                                                   f"{schoolfellow[12]}," if schoolfellow[
                                                                                 12] == 'NULL' else f"'{schoolfellow[12]}'," + \
                                                                                                    f"{schoolfellow[13]}," if \
                    schoolfellow[13] == 'NULL' else f"'{schoolfellow[13]}'," + \
                                                    f"'{schoolfellow[14]}','{schoolfellow[15]}','{schoolfellow[16]}'),"
                rel_num_schoolfellow += 1
                trigger += 1
                if trigger >= 100:
                    finish_insert_sql = insert_sql_schoolfellow + insert_value
                    finish_insert_sql = finish_insert_sql[:-1] + ';'
                    try:
                        my_cur.execute(finish_insert_sql)
                        mysql.commit()
                        print(time_now(1), ':Schoolfellow-MySQL Insert Successful:', trigger, '\tCounts:',
                              rel_num_schoolfellow)
                        insert_value = ''
                        trigger = 0
                    except:
                        mysql.rollback()  # 插入失败，执行回滚操作
                        print(time_now(1), ':\t\tSchoolfellow-MySQL Insert Error At Counts:', trigger, 'in',
                              rel_num_schoolfellow)
        if trigger > 0:
            finish_insert_sql = insert_sql_schoolfellow + insert_value
            finish_insert_sql = finish_insert_sql[:-1] + ';'
            try:
                my_cur.execute(finish_insert_sql)
                mysql.commit()
                print(time_now(1), ':Schoolfellow-MySQL Insert Successful:', trigger, '\tCounts:', rel_num_schoolfellow)
                insert_value = ''
                trigger = 0
            except:
                mysql.rollback()  # 插入失败，执行回滚操作
                print(time_now(1), ':\t\tSchoolfellow-MySQL Insert Error At Counts:', trigger, 'in',
                      rel_num_schoolfellow)
                insert_value = ''
                trigger = 0

        # ******建立该人物的同事关系
        rel_num_workmate = 0  # 创建的同事关系计数器
        for workmate in create_one_workmate(node, 10, graph):
            if not workmate[0] == workmate[1] and \
                    not my_cur.execute(selece_sql_workmate.format(workmate[0], workmate[1])):
                # 插入关系型数据库保存结果
                insert_value = insert_value + f" ('{workmate[0]}','{workmate[1]}','{workmate[2]}','{workmate[3]}'," \
                                              f"'{workmate[4]}','{workmate[5]}','{workmate[6]}','{workmate[7]}'," \
                                              f"'{workmate[8]}','{workmate[9]}','{workmate[10]}','{workmate[11]}'," \
                                              f"'{workmate[12]}','{workmate[13]}','{workmate[14]}','{workmate[15]}'," \
                                              f"'{workmate[16]}'),"
                rel_num_workmate += 1
                trigger += 1
                if trigger >= 100:
                    finish_insert_sql = insert_sql_workmate + insert_value
                    finish_insert_sql = finish_insert_sql[:-1] + ';'
                    try:
                        my_cur.execute(finish_insert_sql)
                        mysql.commit()
                        print(time_now(1), ':Workmate-MySQL Insert Successful:', trigger, '\tCounts:', rel_num_workmate)
                        insert_value = ''
                        trigger = 0
                    except:
                        mysql.rollback()  # 插入失败，执行回滚操作
                        print(time_now(1), ':\t\tWorkmate-MySQL Insert Error At Counts:', trigger, 'in',
                              rel_num_workmate)
        if trigger > 0:
            finish_insert_sql = insert_sql_workmate + insert_value
            finish_insert_sql = finish_insert_sql[:-1] + ';'
            try:
                my_cur.execute(finish_insert_sql)
                mysql.commit()
                print(time_now(1), ':Workmate-MySQL Insert Successful:', trigger, '\tCounts:', rel_num_workmate)
            except:
                mysql.rollback()  # 插入失败，执行回滚操作
                print(time_now(1), ':\t\tWorkmate-MySQL Insert Error At Counts:', trigger, 'in', rel_num_workmate)
    my_cur.close()
    return True


def create_one_countrymen(node=None):
    """此方法暂时停用
    函数功能：根据给定的人物节点node，在图中查找并建立他的同乡关系及其代价属性。
    同乡关系类型(1-10)：中国节点上的同乡关系类型为1，地区节点每往下一级关系类型加1，最大为10。
    :param node:给定的人物节点
    :return: 记录同乡关系的列表 countrymen_pair
    """
    countrymen_pair = []  # 用来保存同乡关系信息的列表
    location = graph.data(f"MATCH {node}-[is_from]->(location:Location) RETURN location")[0]['location']
    type_int = 1  # 同乡关系类型
    while type_int < 10:
        if graph.data(f"MATCH {location}<-[:include_location*{type_int-1}]-(location:Location) RETURN location;")[0][
            'location']['name'] == '中国':
            break
        type_int += 1
    for countrymen in graph.data(f"MATCH {node}-[:is_from]->(:Location)<-[:is_from]-(person:Person) "
                                 f"RETURN DISTINCT person;"):
        countrymen_pair.append([node['id'], countrymen['person']['id'], node['name'], countrymen['person']['name'],
                                location['id'], location['neme'], type_int])
        create_relationship(graph, node, countrymen['person'], 'countrymen_with', 'cost', 5 / (3 * type_int))  # 创建同乡关系
    return countrymen_pair  # 返回同乡关系信息列表


def create_one_schoolfellow(node=None):
    """
    函数功能：根据给定的人物节点node，在图中查找并建立他的同学关系。
    同学关系类型(1,2,3,4,5)，1：表示同学院且同级，2：表示同学院不同级但时间有重叠，3：表示不同学院但同级，4：表示不同学院不同级但时间有重叠，5：表示同校的其他情况。
    :param node:给定的人物节点
    :return: 记录同学关系信息的列表 schoolfellow_pair
    """
    schoolfellow_pair = []  # 用来保存同学关系信息的列表
    school_group = graph.data(f"MATCH {node}-[r:study_at]->(school:School) RETURN school, r;")  # 该人连接的学校节点
    academy_group = graph.data(f"MATCH {node}-[r:study_at]->(academy:Academy)<-[:include_academy]-(school:School) "
                               f"RETURN school, academy, r;")  # 该人连接的学院节点
    for study in school_group:  # 匹配该人与连接的学校节点下所有人的同学关系
        if study['r']['start_time'][0] == '0':  # 若开始时间年份为‘0’(数据不完整)，则忽略该条记录
            continue
        if study['r']['end_time'][0] == '0':  # 结束时间年份为‘0’的表示至今，按格式更改为当前时间
            study['r']['end_time'] = time_now(0)  # 当前日期
        # 保存学校里所有人物教育经历
        person_group = graph.data(f"MATCH {node}-[:study_at]->{study['school']}<-[r:study_at]-(person:Person) "
                                  f"RETURN person, r;") \
                       + graph.data(f"MATCH {node}-[:study_at]->{study['school']}-[:include_academy]->(academy:Academy)"
                                    f"<-[r:study_at]-(person:Person) RETURN person, r, academy;")
        for person in person_group:
            if study['r']['study_id'] == person['r']['study_id'] or node['id'] == person['person']['id'] or \
                    person['r']['start_time'][0] == '0':  # 若ID重复或开始时间年份为‘0’(数据不完整)，则忽略该条记录
                continue
            if person['r']['end_time'][0] == '0':  # 结束时间年份为‘0’的表示至今，按格式更改为当前时间
                person['r']['end_time'] = time_now(0)  # 当前日期
            # 匹配时间段重叠情况
            overlap = period_cmp(study['r']['start_time'], study['r']['end_time'], person['r']['start_time'],
                                 person['r']['end_time'])
            if overlap:
                if study['r']['start_time'][0:4] == person['r']['start_time'][0:4]:
                    type_int = 3  # 同学关系类型2：不同学院但同级
                else:
                    type_int = 4  # 同学关系类型3：不同学院不同级但时间有重叠
            else:
                type_int = 5  # 同学关系类型4：同校的其他情况
            if overlap:  # 创建同学关系
                # 保存同学关系信息
                schoolfellow_pair.append([study['r']['study_id'], person['r']['study_id'],
                                          node['id'], person['person']['id'],
                                          node['name'], person['person']['name'],
                                          study['school']['id'], study['school']['name'], 'NULL', 'NULL',
                                          study['school']['id'], study['school']['name'],
                                          'NULL' if len(person) == 2 else person['academy']['id'],
                                          'NULL' if len(person) == 2 else person['academy']['name'],
                                          overlap[0], overlap[1], type_int])
                create_relationship(graph, node, person['person'], 'schoolfellow_with', 'cost',
                                    5 / (4 * (1 + math.log(overlap[2], math.e)) * (6 - float(type_int))))  # 创建同学关系
    for study in academy_group:  # 匹配该人与连接的学院节点所在学校下所有人的同学关系
        if study['r']['start_time'][0] == '0':  # 若开始时间年份为‘0’(数据不完整)，则忽略该条记录
            continue
        if study['r']['end_time'][0] == '0':  # 结束时间年份为‘0’的表示至今，按格式更改为当前时间
            study['r']['end_time'] = time_now(0)  # 当前日期
        # 保存学校里所有人物教育经历
        person_group = graph.data(f"MATCH {node}-[:study_at]->{study['academy']}<-[r:study_at]-(person:Person) "
                                  f"RETURN person, r;") \
                       + graph.data(f"MATCH {node}-[:study_at]->{study['academy']}<-[:include_academy]-(:School)"
                                    f"<-[r:study_at]-(person:Person) RETURN person, r;") \
                       + graph.data(f"MATCH {node}-[:study_at]->{study['academy']}<-[:include_academy]-(:School)-"
                                    f"[:include_academy]->(academy:Academy)<-[r:study_at]-(person:Person) "
                                    f"RETURN person, r, academy;")
        for person in person_group:
            if study['r']['study_id'] == person['r']['study_id'] or node['id'] == person['person']['id'] or \
                    person['r']['start_time'][0] == '0':  # 若ID重复或开始时间年份为‘0’(数据不完整)，则忽略该条记录
                continue
            if person['r']['end_time'][0] == '0':  # 结束时间年份为‘0’的表示至今，按格式更改为当前时间
                person['r']['end_time'] = time_now(0)  # 当前日期
            # 匹配时间段重叠情况
            overlap = period_cmp(study['r']['start_time'], study['r']['end_time'], person['r']['start_time'],
                                 person['r']['end_time'])
            if overlap:
                if len(person) == 2:
                    if study['r']['start_time'][0:4] == person['r']['start_time'][0:4]:
                        type_int = 1  # 同学关系类型0：同学院且同级
                    else:
                        type_int = 2  # 同学关系类型1：同学院不同级但时间有重叠
                elif study['r']['start_time'][0:4] == person['r']['start_time'][0:4]:
                    type_int = 3  # 同学关系类型2：不同学院但同级
                else:
                    type_int = 4  # 同学关系类型3：不同学院不同级但时间有重叠
            else:
                type_int = 5  # 同学关系类型4：同校的其他情况
            if overlap:  # 创建同学关系
                # 保存同学关系信息
                schoolfellow_pair.append([study['r']['study_id'], person['r']['study_id'],
                                          node['id'], person['person']['id'],
                                          node['name'], person['person']['name'],
                                          study['school']['id'], study['school']['name'],
                                          study['academy']['id'], study['academy']['name'],
                                          study['school']['id'], study['school']['name'],
                                          'NULL' if len(person) == 2 else person['academy']['id'],
                                          'NULL' if len(person) == 2 else person['academy']['name'],
                                          overlap[0], overlap[1], type_int])
                create_relationship(graph, node, person['person'], 'schoolfellow_with', 'cost',
                                    5 / (4 * (1 + math.log(overlap[2], math.e)) * (6 - float(type_int))))  # 创建同学关系
    return schoolfellow_pair  # 返回同学关系信息列表


def create_one_workmate(node=None, max_level=1):
    """
    函数功能：根据给定的人物节点node，在图中查找并建立他的复杂同事关系。
    同事关系类型(0-10)，n：表示x是y的第n层上级，0：表示x与y是同一级别。双向关系只保存一条记录。
    :param node:给定的人物节点
    :param max_level: 最大允许查找下级的层数，缺省值为1
    :return: 记录同学关系信息的列表 workmate_pair
    """
    workmate_pair = []  # 用来保存同学关系信息的列表
    works = graph.data(f"MATCH {node}-[r:work_at]->(position:Position) RETURN position, r;")
    for work_at in works:
        if work_at['r']['start_time'][0] == '0':  # 若开始时间年份为‘0’(数据不完整)，则忽略该条记录
            continue
        if work_at['r']['end_time'][0] == '0':  # 结束时间年份为‘0’的表示至今，按格式更改为当前时间
            work_at['r']['end_time'] = time_now(0)  # 当前日期
        position_up = work_at['position']
        level = 0
        while level < 10:
            level += 1
            institution = graph.data(f"MATCH {position_up}<-[:include_position*{level}]-(institution:Institution) "
                                     f"RETURN institution;")[0]
            if institution:
                institution = institution['institution']
                break
        # 如果改职位为顶级职位，则选择该机构下所有顶级职位下的工作经历，否则仅选择该职位下的所有工作经历
        person_group = graph.data(f"MATCH (person:Person)-[r:work_at]->(position:Position)<-[:include_position]-"
                                  f"{institution} RETURN position, r, person;") \
            if level == 1 else graph.data(f"MATCH {node}-[:work_at]->(position:Position)<-[r:work_at]-(person:Person) "
                                          f"RETURN position, r, person;")
        for person in person_group:
            if work_at['r']['work_id'] == person['r']['work_id'] or node['id'] == person['person']['id'] or \
                    person['r']['start_time'][0] == '0':  # 若ID重复或开始时间年份为‘0’(数据不完整)，则忽略该条记录
                continue
            if person['r']['end_time'][0] == '0':  # 结束时间年份为‘0’的表示至今，按格式更改为当前时间
                person['r']['end_time'] = time_now(0)  # 当前日期
            # 匹配时间段重叠情况
            overlap = period_cmp(work_at['r']['start_time'], work_at['r']['end_time'], person['r']['start_time'],
                                 person['r']['end_time'])
            if overlap:  # 创建同事关系
                # 保存同事关系信息
                workmate_pair.append([work_at['r']['work_id'], person['r']['work_id'],
                                      node['id'], person['person']['id'],
                                      node['name'], person['person']['name'],
                                      institution['id'], institution['name'],
                                      work_at['position']['id'], work_at['position']['name'],
                                      institution['id'], institution['name'],
                                      person['position']['id'], person['position']['name'], overlap[0], overlap[1],
                                      '0'])
                create_relationship(graph, node, person['person'], 'workmate_with', 'cost',
                                    1 / (5 * (1 + math.log(overlap[2], math.e))))  # 创建同事关系
        type_int = 0  # 控制往下查找的层级数
        while type_int < max_level:  # 查找当前节点的多层下级关系
            type_int += 1
            positions_down = graph.data(f"MATCH (position:Position)<-[:include_position*{type_int}]-{position_up} "
                                        f"RETURN position;")
            if not positions_down:  # 当没有更低级职位时退出
                break
            for position in positions_down:
                position = position['position']
                person_group = graph.data(f"MATCH (person:Person)-[r:work_at]->{position} RETURN person, r;")
                for person in person_group:
                    if work_at['r']['work_id'] == person['r']['work_id'] or node['id'] == person['person']['id'] or \
                            person['r']['start_time'][0] == '0':  # 若ID重复或开始时间年份为‘0’(数据不完整)，则忽略该条记录
                        continue
                    if person['r']['end_time'][0] == '0':  # 结束时间年份为‘0’的表示至今，按格式更改为当前时间
                        person['r']['end_time'] = time_now(0)  # 当前日期
                    # 匹配时间段重叠情况
                    overlap = period_cmp(work_at['r']['start_time'], work_at['r']['end_time'],
                                         person['r']['start_time'],
                                         person['r']['end_time'])
                    if overlap:  # 创建同事关系
                        # 保存同事关系信息
                        workmate_pair.append([work_at['r']['work_id'], person['r']['work_id'],
                                              node['id'], person['person']['id'],
                                              node['name'], person['person']['name'],
                                              institution['id'], institution['name'],
                                              work_at['position']['id'], work_at['position']['name'],
                                              institution['id'], institution['name'],
                                              person['position']['id'], person['position']['name'], overlap[0],
                                              overlap[1], str(type_int)])
                        create_relationship(graph, node, person['person'], 'workmate_with', 'cost',
                                            (type_int + 1) / (5 * (1 + math.log(overlap[2], math.e))))  # 创建同事关系
        if level > 1:  # 如果该职位不是顶级职位
            type_int = 0  # 控制往上查找的层级数
            while type_int < max_level:  # 查找当前节点的多层上级关系
                type_int += 1
                positions_up = graph.data(f"MATCH (position:Position)-[:include_position*{type_int}]->{position_up} "
                                          f"RETURN position;")
                if not positions_up:  # 当没有更高级职位时退出
                    break
                for position in positions_up:
                    position = position['position']
                    person_group = graph.data(f"MATCH (person:Person)-[r:work_at]->{position} RETURN person, r;")
                    for person in person_group:
                        if work_at['r']['work_id'] == person['r']['work_id'] or node['id'] == person['person']['id'] or \
                                person['r']['start_time'][0] == '0':  # 若ID重复或开始时间年份为‘0’(数据不完整)，则忽略该条记录
                            continue
                        if person['r']['end_time'][0] == '0':  # 结束时间年份为‘0’的表示至今，按格式更改为当前时间
                            person['r']['end_time'] = time_now(0)  # 当前日期
                        # 匹配时间段重叠情况
                        overlap = period_cmp(work_at['r']['start_time'], work_at['r']['end_time'],
                                             person['r']['start_time'],
                                             person['r']['end_time'])
                        if overlap:  # 创建同事关系
                            # 保存同事关系信息
                            workmate_pair.append([person['r']['work_id'], work_at['r']['work_id'],
                                                  person['person']['id'], node['id'],
                                                  person['person']['name'], node['name'],
                                                  institution['id'], institution['name'],
                                                  person['position']['id'], person['position']['name'],
                                                  institution['id'], institution['name'],
                                                  work_at['position']['id'], work_at['position']['name'], overlap[0],
                                                  overlap[1], str(type_int)])
                            create_relationship(graph, person['person'], node, 'workmate_with', 'cost',
                                                (type_int + 1) / (5 * (1 + math.log(overlap[2], math.e))))  # 创建同事关系
    return workmate_pair  # 返回同事关系信息列表


if __name__ == '__main__':
    print('Hello World!')
