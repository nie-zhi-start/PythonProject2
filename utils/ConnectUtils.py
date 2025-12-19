from neo4j import GraphDatabase


class ConnectUtils:
    """
    Neo4j 连接管理类，用于处理数据库连接、会话管理和关闭。
    """

    def __init__(self, uri, user, password):
        """
        初始化连接管理器。
        :param uri: Neo4j 数据库 URI，例如 "bolt://localhost:7687"
        :param user: 用户名
        :param password: 密码
        """
        self.uri = uri
        self.user = user
        self.password = password
        self.driver = None

    def connect(self):
        """
        建立与 Neo4j 数据库的连接。
        :return: Neo4j Driver 对象
        """
        try:
            self.driver = GraphDatabase.driver(self.uri, auth=(self.user, self.password))
            print("成功连接到 Neo4j 数据库。")
            return self.driver
        except Exception as e:
            print(f"连接 Neo4j 数据库失败: {str(e)}")
            raise

    def get_session(self):
        """
        获取一个 Neo4j 会话（默认写模式，直接开放读写操作）。
        :return: Neo4j Session 对象
        """
        if self.driver is None:
            print("请先调用 connect() 方法建立连接。")
            raise ValueError("未建立数据库连接。")

        session = self.driver.session()  # 不指定模式，默认写模式
        print("已获取会话（默认写模式）。")
        return session

    def close(self):
        """
        关闭 Neo4j 连接。
        """
        if self.driver is not None:
            self.driver.close()
            print("已关闭 Neo4j 数据库连接。")
            self.driver = None
        else:
            print("没有打开的连接可关闭。")

