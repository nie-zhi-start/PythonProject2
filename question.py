from utils.ConnectUtils import ConnectUtils
from utils.KBQAService import KBQAService
from openai import OpenAI
import os
import logging
import threading


# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# 全局初始化（避免每次请求重复创建连接/客户端）
# 1. 数据库连接管理器（全局单例）
# conn_manager = ConnectUtils("bolt://localhost:7687", "neo4j", "88888888")
# # 2. LLM客户端（全局单例）
# client = OpenAI(
#     api_key="sk-93df5702a17d46678b356d19bad90d30",  # 替换为你的真实API Key
#     base_url="https://dashscope.aliyuncs.com/compatible-mode/v1"  # 通义千问的兼容地址
# )

# 单例锁：避免reload时重复初始化
_init_lock = threading.Lock()
_qa_service_initialized = False  # 标记是否已初始化

# 全局变量
conn_manager = None
client = None
qa_service = None


def init_qa_service():
    """初始化QA服务（单例，避免重复连接）"""
    global conn_manager, client, qa_service, _qa_service_initialized
    with _init_lock:
        if _qa_service_initialized:
            return  # 已初始化，直接返回
        try:
            # 1. 数据库连接管理器
            conn_manager = ConnectUtils("bolt://localhost:7687", "neo4j", "88888888")
            conn_manager.connect()
            # 2. LLM客户端
            client = OpenAI(
                api_key="sk-93df5702a17d46678b356d19bad90d30",  # 替换为你的真实API Key
                base_url="https://dashscope.aliyuncs.com/compatible-mode/v1"
            )
            # 3. QA服务实例
            qa_service = KBQAService(conn_manager, client)
            logger.info("QA服务初始化成功")
            _qa_service_initialized = True  # 标记为已初始化
        except Exception as e:
            logger.error(f"QA服务初始化失败: {str(e)}")
            raise


def get_qa_answer_stream(question: str):
    """
    流式生成回答（核心方法，供接口调用）
    :param question: 用户提出的问题
    :return: 生成器，逐段返回回答内容
    """
    if not qa_service:
        yield "系统错误：QA服务未初始化，请检查数据库和LLM配置！"
        return

    # 过滤不文明用语（可选，提升体验）
    dirty_words = ["傻逼", "智障"]
    for word in dirty_words:
        if word in question:
            yield "你的提问包含不文明用语，请使用文明用语重新提问！"
            return

    try:
        # 1. 生成Cypher查询
        cypher_query = qa_service._get_cypher_from_llm(question)
        logger.info(f"生成Cypher: {cypher_query}")

        # 2. 执行Cypher查询
        graph_data = qa_service._execute_cypher(cypher_query)
        logger.info(f"查询结果条数: {len(graph_data)}")

        # 3. 流式生成自然语言回答（修改LLM调用为流式）
        if not graph_data:
            yield "抱歉，我在知识图谱中没有找到相关信息，或者该问题超出了我的知识范围。"
            return

        # 流式调用LLM生成回答
        stream = client.chat.completions.create(
            model=qa_service.model_name,
            messages=[
                {"role": "system",
                 "content": "你是一个中药代茶饮领域的专家助手。请根据用户的问题和提供的数据库查询结果，生成通顺、专业且亲切的回答。"},
                {"role": "user", "content": f"""
                用户问题：{question}
                数据库查询结果：{graph_data}
                回答要求：
                1. 分点列出多个代茶饮，每个对应说明关联信息；
                2. 属性缺失无需提及；
                3. 语言简洁易懂。
                """}
            ],
            stream=True,  # 开启流式输出
            temperature=0.7
        )

        # 逐段返回流式内容
        for chunk in stream:
            if chunk.choices[0].delta.content:
                yield chunk.choices[0].delta.content
    except Exception as e:
        logger.error(f"生成回答失败: {str(e)}")
        yield f"系统错误：{str(e)}"


# 关闭连接的钩子
import atexit
@atexit.register
def close_conn():
    global conn_manager
    if conn_manager:
        conn_manager.close()
        logger.info("数据库连接已关闭")

# 程序启动时初始化
init_qa_service()