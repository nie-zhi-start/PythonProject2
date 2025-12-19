import logging
import json
from neo4j.exceptions import Neo4jError

# 配置日志
logger = logging.getLogger(__name__)


class KBQAService:
    """
    知识问答服务类：负责将自然语言转换为Cypher查询，并生成回答。
    """

    def __init__(self, connection_manager, llm_client, model_name="gpt-3.5-turbo"):
        """
        :param connection_manager: 你的 Neo4jConnectionManager 实例
        :param llm_client: 初始化好的 LLM 客户端 (如 OpenAI 实例)
        :param model_name: 模型名称
        """
        self.connection_manager = connection_manager
        self.client = llm_client
        self.model_name = model_name

        # 【重要】定义你的图谱 Schema
        # 请根据你实际构建的节点标签和关系类型修改此处
        self.schema_definition = """
        图谱包含以下节点标签(Labels)：
        1. Tea (代茶饮): 属性 [name, efficacy(功效), taste(性味), usage(用法)]
        2. Ingredient (药材/成分): 属性 [name, property(归经)]
        3. Season (季节): 属性 [name] (如: 春季, 夏季)
        4. Symptom (症状/适应症): 属性 [name] (如: 上火, 失眠)
        5. Efficacy (功效类别): 属性 [name] (如: 清热解毒)

        图谱包含以下关系类型(Relationships)：
        1. (:Tea)-[:COMPOSED_OF]->(:Ingredient) : 代茶饮由药材组成
        2. (:Tea)-[:SUITABLE_FOR]->(:Season) : 代茶饮适合的季节
        3. (:Tea)-[:TREATS]->(:Symptom) : 代茶饮治疗的症状
        4. (:Tea)-[:HAS_EFFICACY]->(:Efficacy) : 代茶饮具有的功效
        """

    def _get_cypher_from_llm(self, user_question):
        """
        调用 LLM 将自然语言转换为 Cypher 语句
        """
        system_prompt = f"""
        你是一个 Neo4j Cypher 专家助手。你的任务是将用户的自然语言问题转换为 Neo4j Cypher 查询语句。

        {self.schema_definition}

        规则：
        1. 只返回 Cypher 语句，不要包含 markdown 格式（如 ```cypher），不要包含任何解释。
        2. 使用模糊匹配时请使用 CONTAINS。
        3. 始终限制返回结果数量，例如 LIMIT 5。
        4. 如果问题涉及推荐（如“适合喝什么”），请返回 Tea 节点的 name 和 efficacy 属性。

        示例：
        用户：金银花茶有什么功效？
        Cypher: MATCH (n:Tea {{name: '金银花茶'}}) RETURN n.efficacy

        用户：夏季适合喝什么茶？
        Cypher: MATCH (t:Tea)-[:SUITABLE_FOR]->(s:Season {{name: '夏季'}}) RETURN t.name, t.efficacy
        """

        try:
            response = self.client.chat.completions.create(
                model=self.model_name,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_question}
                ],
                temperature=0  # 设置为0以保证输出稳定性
            )
            cypher = response.choices[0].message.content.strip()
            # 清理可能存在的 markdown 符号
            cypher = cypher.replace("```cypher", "").replace("```", "").strip()
            logger.info(f"生成的 Cypher: {cypher}")
            return cypher
        except Exception as e:
            logger.error(f"LLM 生成 Cypher 失败: {str(e)}")
            raise

    def _execute_cypher(self, cypher):
        """
        执行查询并格式化结果
        """
        try:
            with self.connection_manager.get_session() as session:
                result = session.run(cypher)
                # 将结果转换为简单的列表/字典格式
                data = [record.data() for record in result]
                return data
        except Neo4jError as e:
            logger.error(f"Cypher 执行错误: {str(e)}")
            return []

    def _generate_natural_answer(self, user_question, graph_data):
        """
        根据图谱查询结果生成自然语言回答
        """
        if not graph_data:
            return "抱歉，我在知识图谱中没有找到相关信息，或者该问题超出了我的知识范围。"

        system_prompt = "你是一个中药代茶饮领域的专家助手。请根据用户的问题和提供的数据库查询结果，生成通顺、专业且亲切的回答。"

        user_prompt = f"""
        用户问题：{user_question}
        数据库查询结果：{json.dumps(graph_data, ensure_ascii=False)}

        请生成回答：
        """

        try:
            response = self.client.chat.completions.create(
                model=self.model_name,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt}
                ]
            )
            return response.choices[0].message.content
        except Exception as e:
            logger.error(f"生成回答失败: {str(e)}")
            return "抱歉，生成回答时出现了系统错误。"

    def answer(self, question):
        """
        主入口：处理用户提问
        """
        print(f"用户提问: {question}")

        # 1. 文本转 Cypher
        try:
            cypher_query = self._get_cypher_from_llm(question)
        except Exception:
            return "系统繁忙，无法理解您的问题。"

        # 2. 查询图数据库
        data = self._execute_cypher(cypher_query)
        logger.info(f"查询结果条数: {len(data)}")

        # 3. 生成最终回复
        final_answer = self._generate_natural_answer(question, data)
        return final_answer