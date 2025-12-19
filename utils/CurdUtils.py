from neo4j.exceptions import Neo4jError, ServiceUnavailable
import logging

# 新增：配置logger（与main.py保持一致，也可简化）
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)  # 新增：创建logger实例

class CurdUtils:
    """
    Neo4j 图操作工具类，用于封装基本的 CRUD 操作。
    需要传入 Neo4jConnectionManager 实例来获取会话。
    已更新为使用 elementId() 替换弃用的 id()，兼容 Neo4j 5.x+。
    """

    def __init__(self, connection_manager):
        """
        初始化图操作工具。
        :param connection_manager: Neo4jConnectionManager 实例
        """
        self.connection_manager = connection_manager

    def create_node(self, label, properties):
        """
        创建一个节点(可重复)
        :param label: 节点的标签（字符串）
        :param properties: 节点的属性（字典）
        :return: 创建的节点 elementId (字符串)
        """
        try:
            with self.connection_manager.get_session() as session:
                cypher = f"CREATE (n:{label} $props) RETURN elementId(n) AS node_id"
                result = session.run(cypher, props=properties)
                node_id = result.single()["node_id"]
                print(f"成功创建节点，ID: {node_id}")
                return node_id
        except KeyError:  # 无返回结果（极少见，如Cypher写错）
            raise Exception(f"创建{label}节点失败：无返回ID，属性{properties}")
        except (Neo4jError, ServiceUnavailable) as e:
            raise Exception(f"创建{label}节点失败：{str(e)}")

    def merge_node(self, label, properties):
        """
        使用 MERGE 创建或匹配节点（避免重复）
        :param label: 节点的标签（字符串）
        :param properties: 节点的属性（字典），用于匹配和设置
        :return: 节点 elementId (字符串)
        """
        # 校验核心参数
        if not label:
            raise ValueError("节点标签不能为空！")
        if not properties or "name" not in properties:
            raise ValueError("merge_node需传入包含'name'键的属性字典（中医药实体唯一标识）！")

        try:
            with self.connection_manager.get_session() as session:
                cypher = f"MERGE (n:{label} {{name: $name}}) SET n += $props RETURN elementId(n) AS node_id"
                params = {"name": properties.get("name"), "props": properties}
                result = session.run(cypher, params)
                node_id = result.single()["node_id"]
                print(f"成功合并节点，ID: {node_id}")
                return node_id
        except (Neo4jError, ServiceUnavailable) as e:
            raise Exception(f"合并{label}节点失败：{str(e)}")

    def create_relationship(self, start_node_id, end_node_id, rel_type, properties=None):
        """
        创建两个节点之间的关系
        :param start_node_id: 起始节点 elementId (字符串)
        :param end_node_id: 结束节点 elementId (字符串)
        :param rel_type: 关系类型（字符串）
        :param properties: 关系的属性（字典，可选）
        :return: 关系 elementId (字符串)
        """
        if properties is None:
            properties = {}
        with self.connection_manager.get_session() as session:
            cypher = (
                    "MATCH (a) WHERE elementId(a) = $start_id "
                    "MATCH (b) WHERE elementId(b) = $end_id "
                    "CREATE (a)-[r:" + rel_type + " $props]->(b) "
                                                  "RETURN elementId(r) AS rel_id"
            )
            params = {"start_id": start_node_id, "end_id": end_node_id, "props": properties}
            result = session.run(cypher, params)
            rel_id = result.single()["rel_id"]
            print(f"成功创建关系，ID: {rel_id}")
            return rel_id

    def read_node(self, node_id=None, label=None, properties=None):
        """
        读取节点。
        :param node_id: 节点 elementId (字符串，可选)
        :param label: 标签（可选）
        :param properties: 属性过滤（字典，可选）
        :return: 节点数据列表（每个是字典）
        """
        if properties is None:
            properties = {}
        with self.connection_manager.get_session() as session:
            if node_id:
                cypher = "MATCH (n) WHERE elementId(n) = $node_id RETURN n"
                params = {"node_id": node_id}
            else:
                # 对于属性过滤，简化版；实际可动态构建更复杂的 WHERE
                prop_conditions = " AND ".join([f"n.{k} = ${k}" for k in properties])
                cypher = f"MATCH (n:{label})" + (f" WHERE {prop_conditions}" if prop_conditions else "") + " RETURN n"
                params = properties
            result = session.run(cypher, params)
            nodes = [record["n"] for record in result]
            print(f"成功读取 {len(nodes)} 个节点")
            return nodes

    def update_properties(self, entity_id, properties, is_node=True):
        """
        更新节点或关系的属性。
        :param entity_id: 实体 elementId (字符串)
        :param properties: 要更新的属性（字典）
        :param is_node: True 为节点，False 为关系
        :return: None
        """
        with self.connection_manager.get_session() as session:
            if is_node:
                cypher = "MATCH (n) WHERE elementId(n) = $id SET n += $props"
            else:
                cypher = "MATCH ()-[r]-() WHERE elementId(r) = $id SET r += $props"
            params = {"id": entity_id, "props": properties}
            session.run(cypher, params)
            print("成功更新属性")

    def delete_entity(self, entity_id, is_node=True, detach=True):
        """
        删除节点或关系。
        :param entity_id: 实体 elementId (字符串)
        :param is_node: True 为节点，False 为关系
        :param detach: 对于节点，是否 DETACH DELETE（删除关联关系）
        :return: None
        """
        with self.connection_manager.get_session() as session:
            if is_node:
                cypher = "MATCH (n) WHERE elementId(n) = $id " + ("DETACH DELETE n" if detach else "DELETE n")
            else:
                cypher = "MATCH ()-[r]-() WHERE elementId(r) = $id DELETE r"
            params = {"id": entity_id}
            session.run(cypher, params)
            print("成功删除实体")

    def clear_all_data(self):
        """
        高效清空Neo4j中所有节点和关系（最优方式，单条Cypher执行）
        原理：DETACH DELETE会删除所有节点及关联关系，比循环删除快100倍+
        """
        if self.connection_manager.driver is None:
            raise ValueError("未建立数据库连接，请先调用connect()")

        try:
            with self.connection_manager.get_session() as session:
                # 核心Cypher：一次性删除所有节点和关系（Neo4j官方推荐的清空方式）
                cypher = "MATCH (n) DETACH DELETE n"
                result = session.run(cypher)
                # 获取删除的记录数（可选）
                summary = result.consume()
                deleted_count = summary.counters.nodes_deleted
                logger.info(f"成功清空所有节点和关系，共删除 {deleted_count} 个节点（所有关系已同步删除）")
                return deleted_count
        except (Neo4jError, ServiceUnavailable) as e:
            raise Exception(f"清空数据库失败：{str(e)}")

    # 【备选】如果想基于原有delete_entity循环删除（仅小数据量测试用，效率低）
    def delete_all_nodes_cycle(self):
        """
        循环删除所有节点（依赖原有delete_entity方法，效率低，不推荐）
        """
        # 先查询所有节点ID
        all_nodes = self.read_node()  # 调用原有read_node方法（无参数查所有节点）
        deleted_count = 0
        for node in all_nodes:
            node_id = node.element_id  # Neo4j 5.x+ 获取elementId的方式
            self.delete_entity(node_id, is_node=True, detach=True)
            deleted_count += 1
        logger.info(f"循环删除完成，共删除 {deleted_count} 个节点")
        return deleted_count
