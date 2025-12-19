import logging
import pandas as pd
import math
from typing import List, Dict
from neo4j.exceptions import ServiceUnavailable

# 导入独立工具类
from utils.ConnectUtils import ConnectUtils
from utils.CurdUtils import CurdUtils
from utils.BatchHandler import Neo4jBatchHandler

# -------------------------- 全局配置（集中管理） --------------------------
NEO4J_CONFIG = {
    "uri": "bolt://localhost:7687",
    "user": "neo4j",
    "password": "88888888"
}

BATCH_CONFIG = {
    "batch_size": 15,
    "retry_times": 3,
    "retry_delay": 2
}

BUSINESS_CONFIG = {
    "drink_label": "中药材",                  # 代茶饮节点的标签名称
    "herb_label": "代茶饮",                   # 中药材节点的标签名称（可改如"药材"）
    "rel_type": "可制作",                       # 代茶饮与中药材之间的关系类型（可改如"包含"）
    "unique_key": "名称",                     # 所有节点的唯一标识属性名（CSV第一列需匹配此名称）
    "basic_data_path": "基本数据.csv",         # 代茶饮-中药材映射关系的CSV文件路径
    # 新增：基础数据CSV的列名配置（核心！后续改列名只改这两行）
    "basic_drink_col": "中药材",               # 基础数据中“代茶饮”的列名
    "basic_herb_col": "代茶饮",                # 基础数据中“中药材”的列名
    "prop_data_path": "代茶饮属性.csv",        # 待更新属性的CSV文件路径（当前是代茶饮属性，可改中药材）
    "prop_update_label": "代茶饮",            # 要更新属性的目标节点标签（当前更代茶饮，可改中药材）
    "clear_db": False,                       # 改为False，避免重复清空数据库（测试阶段可改True）
    "encoding": "utf-8"                      # CSV文件读取编码（中文乱码时可改为gbk）
}

# -------------------------- 日志初始化 --------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

# -------------------------- 通用工具函数 --------------------------
def read_csv_for_node_props(csv_path: str, unique_key: str) -> List[Dict]:
    """读取节点属性CSV（通用）"""
    try:
        df = pd.read_csv(csv_path, encoding=BUSINESS_CONFIG["encoding"])
    except UnicodeDecodeError:
        df = pd.read_csv(csv_path, encoding="gbk")

    if unique_key not in df.columns:
        raise ValueError(f"CSV[{csv_path}]缺少唯一标识列[{unique_key}]，请检查列名")
    if df.empty:
        raise ValueError(f"CSV[{csv_path}]为空，无属性数据可更新")

    df = df.fillna("")
    prop_data = df.to_dict("records")
    logger.info(
        f"✅ 成功读取属性CSV[{csv_path}]：共{len(prop_data)}条数据，"
        f"属性列：{[col for col in df.columns if col != unique_key]}"
    )
    return prop_data


def split_chinese_herbs(herb_str):
    """
    拆分中药材字符串（处理空值/NaN）
    :param herb_str: 中药材字符串（可能为NaN/float/None）
    :return: 去重后的中药材列表
    """
    # 第一步：处理空值/NaN，直接返回空列表
    # 1. 检测None
    if herb_str is None:
        return []
    # 2. 检测float类型的NaN
    if isinstance(herb_str, float) and math.isnan(herb_str):
        return []
    # 3. 转为字符串（处理数字/其他类型），并去空格
    herb_str = str(herb_str).strip()
    # 4. 处理空字符串
    if herb_str == "" or herb_str == "nan":  # 处理转为字符串后的"nan"
        return []

    # 第二步：正常拆分（原有逻辑）
    return list(set([h.strip() for h in herb_str.split("、") if h.strip()]))

# -------------------------- 主业务逻辑（模块化解耦，可独立注释） --------------------------
def main():
    conn_manager = None
    try:
        # ===================== 模块1：初始化连接（必选，不可注释） =====================
        logger.info("=== 初始化Neo4j连接 ===")
        conn_manager = ConnectUtils(
            uri=NEO4J_CONFIG["uri"],
            user=NEO4J_CONFIG["user"],
            password=NEO4J_CONFIG["password"]
        )
        conn_manager.connect()
        curd_utils = CurdUtils(conn_manager)
        batch_handler = Neo4jBatchHandler(conn_manager=conn_manager, curd_utils=curd_utils)

        # ===================== 模块2：清空数据库（可选，注释即关闭） =====================
        if BUSINESS_CONFIG["clear_db"]:
            logger.info("\n=== 清空数据库（测试用） ===")
            deleted_count = curd_utils.clear_all_data()
            logger.info(f"✅ 清空完成，删除节点数：{deleted_count}")
        else:
            logger.info("\n=== 跳过清空数据库 ===")

        # ===================== 模块3：读取基础数据（代茶饮-中药材） =====================
        logger.info("\n=== 读取代茶饮-中药材基础数据 ===")
        df_basic = pd.read_csv(BUSINESS_CONFIG["basic_data_path"], encoding=BUSINESS_CONFIG["encoding"])
        drink_herb_data: List[Dict] = df_basic.apply(
            lambda row: {
                # 引用配置项的列名，不再硬编码
                "drink_unique_val": row[BUSINESS_CONFIG["basic_drink_col"]],
                "herbs_str": row[BUSINESS_CONFIG["basic_herb_col"]]
            }, axis=1
        ).tolist()
        logger.info(f"✅ 读取基础数据：共{len(drink_herb_data)}条代茶饮记录")

        # ===================== 模块4：创建代茶饮节点（可选，注释即关闭） =====================
        logger.info("\n=== 批量创建代茶饮节点 ===")
        # 先收集所有拆分后的代茶饮值（去重）
        all_drinks = set()
        for item in drink_herb_data:
            drink_val = item["drink_unique_val"]
            # 复用拆分函数处理代茶饮（和中药材拆分逻辑完全一致）
            split_drinks = split_chinese_herbs(drink_val)
            all_drinks.update(split_drinks)

        # 构建代茶饮节点列表（拆分后的值）
        drink_nodes = [
            {BUSINESS_CONFIG["unique_key"]: drink_name}
            for drink_name in all_drinks
        ]
        drink_unique_to_id = batch_handler.batch_merge_nodes(
            label=BUSINESS_CONFIG["drink_label"],
            node_list=drink_nodes,
            unique_key=BUSINESS_CONFIG["unique_key"],
            batch_size=BATCH_CONFIG["batch_size"],
            retry_times=BATCH_CONFIG["retry_times"],
            retry_delay=BATCH_CONFIG["retry_delay"]
        )
        logger.info(f"✅ 代茶饮节点创建完成：共{len(drink_unique_to_id)}个（拆分去重后）")

        # ===================== 模块5：创建中药材节点（可选，注释即关闭） =====================
        logger.info("\n=== 批量创建中药材节点 ===")
        all_herbs = set()
        for item in drink_herb_data:
            all_herbs.update(split_chinese_herbs(item["herbs_str"]))
        herb_nodes = [
            {BUSINESS_CONFIG["unique_key"]: herb_name}
            for herb_name in all_herbs
        ]
        herb_unique_to_id = batch_handler.batch_merge_nodes(
            label=BUSINESS_CONFIG["herb_label"],
            node_list=herb_nodes,
            unique_key=BUSINESS_CONFIG["unique_key"],
            batch_size=BATCH_CONFIG["batch_size"],
            retry_times=BATCH_CONFIG["retry_times"],
            retry_delay=BATCH_CONFIG["retry_delay"]
        )
        logger.info(f"✅ 中药材节点创建完成：共{len(herb_unique_to_id)}个")

        # ===================== 模块6：创建原料关系（可选，注释即关闭） =====================
        # ===================== 模块6：创建原料关系（可选，注释即关闭） =====================
        logger.info("\n=== 批量创建代茶饮-中药材「原料」关系 ===")
        rel_list = []
        skipped_rel = 0  # 统计跳过的无效关系数
        for item in drink_herb_data:
            # ===== 你原有代码：代茶饮空值/NaN过滤（完全保留，不修改） =====
            drink_unique_val = item["drink_unique_val"]
            # 过滤NaN/None/空字符串（和工具类逻辑一致，无额外依赖）
            if (drink_unique_val is None
                    or (isinstance(drink_unique_val, float) and math.isnan(drink_unique_val))
                    or (isinstance(drink_unique_val, str) and drink_unique_val.strip() == "")):
                skipped_rel += 1
                continue  # 跳过无效代茶饮值，不创建关系

            # ===== 新增：拆分代茶饮值（复用通用函数，不破坏原有逻辑） =====
            split_drinks = split_chinese_herbs(drink_unique_val)
            if not split_drinks:  # 拆分后无有效代茶饮值，跳过
                skipped_rel += 1
                continue

            # ===== 你原有代码：中药材拆分（完全保留） =====
            herb_names = split_chinese_herbs(item["herbs_str"])
            if not herb_names:  # 新增：中药材拆分后无值也跳过（可选，增强鲁棒性）
                skipped_rel += 1
                continue

            # ===== 新增：遍历拆分后的代茶饮 + 保留你原有中药材遍历/检查逻辑 =====
            for drink_name in split_drinks:
                # 复用你原有逻辑：检查代茶饮ID是否存在
                if drink_name not in drink_unique_to_id:
                    skipped_rel += 1
                    continue

                for herb_name in herb_names:
                    # 你原有逻辑：检查中药材ID是否存在
                    if herb_name not in herb_unique_to_id:
                        skipped_rel += 1
                        continue

                    # 你原有逻辑：构建关系
                    drink_id = drink_unique_to_id[drink_name]
                    herb_id = herb_unique_to_id[herb_name]
                    rel_list.append((
                        drink_id,
                        herb_id,
                        BUSINESS_CONFIG["rel_type"],
                        {}
                    ))

        # ===== 你原有代码：批量创建关系（完全保留） =====
        if rel_list:
            rel_ids = batch_handler.batch_create_relationships(
                rel_list=rel_list,
                batch_size=BATCH_CONFIG["batch_size"],
                retry_times=BATCH_CONFIG["retry_times"],
                retry_delay=BATCH_CONFIG["retry_delay"]
            )
            logger.info(f"✅ 原料关系创建完成：共{len(rel_ids)}个（跳过无效关系{skipped_rel}个）")
        else:
            logger.warning(f"⚠️ 无有效原料关系可创建（共跳过{skipped_rel}个无效关系）")

        # # ===================== 模块7：批量更新节点属性（可选，注释即关闭） =====================
        # logger.info(f"\n=== 批量更新[{BUSINESS_CONFIG['prop_update_label']}]节点属性 ===")
        # # 步骤1：读取属性CSV
        # prop_data = read_csv_for_node_props(
        #     csv_path=BUSINESS_CONFIG["prop_data_path"],
        #     unique_key=BUSINESS_CONFIG["unique_key"]
        # )
        # # 步骤2：执行批量更新（自动跳过不存在的节点）
        # updated_count = batch_handler.batch_update_node_props(
        #     label=BUSINESS_CONFIG["prop_update_label"],
        #     prop_data=prop_data,
        #     unique_key=BUSINESS_CONFIG["unique_key"],
        #     batch_size=BATCH_CONFIG["batch_size"],
        #     retry_times=BATCH_CONFIG["retry_times"],
        #     retry_delay=BATCH_CONFIG["retry_delay"]
        # )
        # logger.info(f"✅ 属性更新完成：共处理{len(prop_data)}条，成功更新{updated_count}个节点")
        #
        # # ===================== 模块8：结果汇总（可选，注释即关闭） =====================
        # logger.info("\n=== 所有操作完成（结果汇总） ===")
        # logger.info(f"📊 代茶饮节点数：{len(drink_unique_to_id)}")
        # logger.info(f"📊 中药材节点数：{len(herb_unique_to_id)}")
        # logger.info(f"📊 原料关系数：{len(rel_ids)}")
        # logger.info(f"📊 成功更新属性的节点数：{updated_count}")

    except ServiceUnavailable:
        logger.error("❌ Neo4j服务不可用，请检查连接地址和服务状态")
    except Exception as e:
        logger.error(f"❌ 业务逻辑执行失败：{str(e)}", exc_info=True)
    finally:
        # ===================== 模块9：关闭连接（必选，不可注释） =====================
        if conn_manager and conn_manager.driver:
            logger.info("\n=== 关闭Neo4j连接 ===")
            conn_manager.close()

if __name__ == "__main__":
    main()