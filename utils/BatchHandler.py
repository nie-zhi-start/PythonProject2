# BatchHandler.py
import time
import logging
import math
from typing import List, Dict, Tuple, Optional
from neo4j.exceptions import Neo4jError, TransientError

# 初始化日志（与主逻辑保持一致）
logger = logging.getLogger(__name__)

class Neo4jBatchHandler:
    """
    独立的Neo4j批量操作类（解耦，无全局配置依赖）
    需传入ConnectUtils和CurdUtils实例，方法通过unique_key动态指定唯一标识属性
    """
    def __init__(self, conn_manager, curd_utils):
        """
        初始化批量处理器
        :param conn_manager: ConnectUtils实例（数据库连接管理）
        :param curd_utils: CurdUtils实例（基础CRUD操作）
        """
        self.conn_manager = conn_manager
        self.curd_utils = curd_utils
        # 校验连接实例有效性
        if not hasattr(conn_manager, "get_session"):
            raise TypeError("conn_manager必须是ConnectUtils类实例")
        if not hasattr(curd_utils, "clear_all_data"):
            raise TypeError("curd_utils必须是CurdUtils类实例")

    def batch_merge_nodes(
            self,
            label: str,
            node_list: List[Dict],
            unique_key: str,
            batch_size: int = 15,
            retry_times: int = 3,
            retry_delay: int = 2,
            filter_empty_props: bool = True  # 是否过滤非唯一键的空值属性
    ) -> Dict[str, str]:
        unique_to_id = {}
        skipped_nodes = []  # 记录跳过的空值节点
        valid_node_list = []  # 过滤后的有效节点

        # 第一步：前置过滤空值节点（核心修改：增加NaN检测）
        for idx, node in enumerate(node_list):
            # 1. 检查unique_key是否存在
            if unique_key not in node:
                skipped_nodes.append(f"行{idx + 1}：缺少唯一键[{unique_key}]")
                logger.warning(f"节点过滤-行{idx + 1}：缺少唯一键[{unique_key}]，数据：{node}")
                continue

            # 2. 检查unique_key的值是否为无效空值（None/空字符串/全空格/NaN）
            unique_val = node[unique_key]

            # 新增：检测NaN（包括numpy.nan和float('nan')）
            if isinstance(unique_val, float) and math.isnan(unique_val):
                skipped_nodes.append(f"行{idx + 1}：唯一键[{unique_key}]值为NaN")
                logger.warning(f"节点过滤-行{idx + 1}：唯一键[{unique_key}]值为NaN，数据：{node}")
                continue

            # 处理字符串空值（去空格）
            if isinstance(unique_val, str):
                unique_val = unique_val.strip()
            # 检查空值（None/空字符串）
            if unique_val is None or unique_val == "":
                skipped_nodes.append(f"行{idx + 1}：唯一键[{unique_key}]值为空")
                logger.warning(f"节点过滤-行{idx + 1}：唯一键[{unique_key}]值为空，数据：{node}")
                continue

            # 3. 可选：过滤其他属性的空值（含NaN）
            if filter_empty_props:
                filtered_props = {}
                for k, v in node.items():
                    if k == unique_key:
                        filtered_props[k] = unique_val  # 唯一键已校验，保留
                        continue

                    # 新增：先过滤该属性的NaN
                    if isinstance(v, float) and math.isnan(v):
                        continue
                    # 再过滤字符串空值
                    if isinstance(v, str):
                        v = v.strip()
                    if v is not None and v != "":
                        filtered_props[k] = v
                valid_node_list.append(filtered_props)
            else:
                valid_node_list.append(node)

        # 第二步：批量处理有效节点（无修改）
        for batch_idx in range(0, len(valid_node_list), batch_size):
            batch_nodes = valid_node_list[batch_idx:batch_idx + batch_size]
            retry_count = 0
            while retry_count < retry_times:
                try:
                    with self.conn_manager.get_session() as session:
                        with session.begin_transaction() as tx:
                            for node_props in batch_nodes:
                                cypher = (
                                    f"MERGE (n:{label} {{{unique_key}: ${unique_key}}}) "
                                    "SET n += $other_props "
                                    "RETURN elementId(n) AS node_id"
                                )
                                unique_val = node_props[unique_key]
                                other_props = {k: v for k, v in node_props.items() if k != unique_key}
                                params = {unique_key: unique_val, "other_props": other_props}

                                result = tx.run(cypher, params)
                                node_id = result.single()["node_id"]
                                unique_to_id[unique_val] = node_id

                            tx.commit()
                            logger.info(
                                f"节点合并-批次{batch_idx // batch_size + 1}："
                                f"成功合并{len(batch_nodes)}个[{label}]节点（唯一键：{unique_key}）"
                            )
                        break
                except TransientError as e:
                    retry_count += 1
                    logger.warning(
                        f"节点合并-批次{batch_idx // batch_size + 1}："
                        f"临时错误，重试{retry_count}/{retry_times}，错误：{str(e)}"
                    )
                    time.sleep(retry_delay * retry_count)
                except (Neo4jError, Exception) as e:
                    logger.error(
                        f"节点合并-批次{batch_idx // batch_size + 1}："
                        f"非临时错误，错误：{str(e)}"
                    )
                    raise
            else:
                raise Exception(
                    f"节点合并-批次{batch_idx // batch_size + 1}："
                    f"重试{retry_times}次仍失败"
                )

        # 输出跳过的节点日志
        if skipped_nodes:
            logger.warning(f"节点过滤完成：共跳过{len(skipped_nodes)}个无效节点，原因：{skipped_nodes}")
        return unique_to_id

    def batch_create_relationships(
            self,
            rel_list: List[Tuple[str, str, str, Dict]],
            batch_size: int = 15,
            retry_times: int = 3,
            retry_delay: int = 2,
            filter_empty_props: bool = True  # 是否过滤关系属性的空值
    ) -> List[str]:
        """
        批量创建关系（幂等+空值过滤，新增NaN检测）
        :param rel_list: 关系列表：[(起始节点elementId, 结束节点elementId, 关系类型, 关系属性), ...]
        :param filter_empty_props: 是否过滤关系属性的空值
        :return: 成功创建/复用的关系elementId列表
        """
        all_rel_ids = []
        created_count = 0  # 新增关系数
        reused_count = 0  # 复用已有关系数
        skipped_rels = []  # 记录跳过的空值关系
        valid_rel_list = []  # 过滤后的有效关系

        # 第一步：前置过滤空值关系（新增NaN检测）
        for idx, (start_id, end_id, rel_type, rel_props) in enumerate(rel_list):
            # 1. 检查start_id/end_id是否为NaN
            if isinstance(start_id, float) and math.isnan(start_id):
                skipped_rels.append(f"行{idx + 1}：起始节点ID为NaN")
                logger.warning(f"关系过滤-行{idx + 1}：起始节点ID为NaN，跳过该关系")
                continue
            if isinstance(end_id, float) and math.isnan(end_id):
                skipped_rels.append(f"行{idx + 1}：结束节点ID为NaN")
                logger.warning(f"关系过滤-行{idx + 1}：结束节点ID为NaN，跳过该关系")
                continue

            # 2. 检查start_id/end_id是否为空（None/空字符串/全空格）
            if isinstance(start_id, str):
                start_id = start_id.strip()
            if isinstance(end_id, str):
                end_id = end_id.strip()
            if start_id is None or start_id == "":
                skipped_rels.append(f"行{idx + 1}：起始节点ID为空")
                logger.warning(f"关系过滤-行{idx + 1}：起始节点ID为空，跳过该关系")
                continue
            if end_id is None or end_id == "":
                skipped_rels.append(f"行{idx + 1}：结束节点ID为空")
                logger.warning(f"关系过滤-行{idx + 1}：结束节点ID为空，跳过该关系")
                continue

            # 3. 检查关系类型是否为空/NaN
            if isinstance(rel_type, float) and math.isnan(rel_type):
                skipped_rels.append(f"行{idx + 1}：关系类型为NaN")
                logger.warning(f"关系过滤-行{idx + 1}：关系类型为NaN，跳过该关系")
                continue
            if isinstance(rel_type, str):
                rel_type = rel_type.strip()
            if rel_type is None or rel_type == "":
                skipped_rels.append(f"行{idx + 1}：关系类型为空")
                logger.warning(f"关系过滤-行{idx + 1}：关系类型为空，跳过该关系")
                continue

            # 4. 可选：过滤关系属性的空值（含NaN）
            filtered_props = rel_props
            if filter_empty_props and rel_props is not None:
                filtered_props = {}
                for k, v in rel_props.items():
                    # 先过滤NaN
                    if isinstance(v, float) and math.isnan(v):
                        continue
                    # 再过滤字符串空值
                    if isinstance(v, str):
                        v = v.strip()
                    if v is not None and v != "":
                        filtered_props[k] = v

            # 5. 加入有效关系列表（统一去空格）
            valid_rel_list.append((start_id, end_id, rel_type, filtered_props))

        # 第二步：批量处理有效关系（无修改）
        for batch_idx in range(0, len(valid_rel_list), batch_size):
            batch_rels = valid_rel_list[batch_idx:batch_idx + batch_size]
            batch_created = 0
            batch_reused = 0
            retry_count = 0

            while retry_count < retry_times:
                try:
                    with self.conn_manager.get_session() as session:
                        with session.begin_transaction() as tx:
                            for start_id, end_id, rel_type, rel_props in batch_rels:
                                # 1. 前置检查关系是否存在
                                prop_matches = []
                                prop_params = {}
                                for idx_prop, (k, v) in enumerate(rel_props.items()):
                                    prop_key = f"p{idx_prop}"
                                    prop_matches.append(f"r.{k} = ${prop_key}")
                                    prop_params[prop_key] = v

                                check_cypher = f"""
                                MATCH (a)-[r:{rel_type}]->(b)
                                WHERE elementId(a) = $start_id AND elementId(b) = $end_id
                                {" AND " + " AND ".join(prop_matches) if prop_matches else ""}
                                RETURN count(r) > 0 AS exist
                                """
                                check_params = {
                                    "start_id": start_id,
                                    "end_id": end_id,
                                    **prop_params
                                }
                                check_result = tx.run(check_cypher, **check_params)
                                rel_exist_before_merge = check_result.single()["exist"]

                                # 2. 构建MERGE模板
                                rel_attrs = []
                                for k, v in rel_props.items():
                                    if isinstance(v, str):
                                        rel_attrs.append(f"{k} = '{v}'")
                                    else:
                                        rel_attrs.append(f"{k} = {v}")
                                rel_template = f"[r:{rel_type} {{{', '.join(rel_attrs)}}}]" if rel_attrs else f"[r:{rel_type}]"

                                # 3. 执行MERGE（拆分MATCH消除笛卡尔积警告）
                                merge_cypher = f"""
                                MATCH (a) WHERE elementId(a) = $start_id
                                MATCH (b) WHERE elementId(b) = $end_id
                                MERGE (a)-{rel_template}->(b)
                                RETURN elementId(r) AS rel_id
                                """
                                merge_params = {"start_id": start_id, "end_id": end_id}
                                merge_result = tx.run(merge_cypher, **merge_params)
                                rel_id = merge_result.single()["rel_id"]

                                # 4. 统计新增/复用
                                all_rel_ids.append(rel_id)
                                if rel_exist_before_merge:
                                    batch_reused += 1
                                    reused_count += 1
                                    logger.debug(f"关系[{start_id}]->[{rel_type}]->[{end_id}]已存在，复用ID：{rel_id}")
                                else:
                                    batch_created += 1
                                    created_count += 1
                                    logger.debug(f"关系[{start_id}]->[{rel_type}]->[{end_id}]创建成功，ID：{rel_id}")

                            tx.commit()
                            logger.info(
                                f"关系创建-批次{batch_idx // batch_size + 1}："
                                f"处理{len(batch_rels)}条，新增{batch_created}个，复用{batch_reused}个[{rel_type}]关系"
                            )
                        break
                except TransientError as e:
                    retry_count += 1
                    logger.warning(
                        f"关系创建-批次{batch_idx // batch_size + 1}："
                        f"临时错误，重试{retry_count}/{retry_times}，错误：{str(e)}"
                    )
                    time.sleep(retry_delay * retry_count)
                except (Neo4jError, Exception) as e:
                    logger.error(
                        f"关系创建-批次{batch_idx // batch_size + 1}："
                        f"非临时错误，错误：{str(e)}"
                    )
                    raise
            else:
                raise Exception(
                    f"关系创建-批次{batch_idx // batch_size + 1}："
                    f"重试{retry_times}次仍失败"
                )

        # 输出跳过的关系日志
        if skipped_rels:
            logger.warning(f"关系过滤完成：共跳过{len(skipped_rels)}个无效关系，原因：{skipped_rels}")
        # 汇总日志
        logger.info(
            f"✅ 关系创建完成：总计{len(all_rel_ids)}条（新增{created_count}个，复用{reused_count}个），"
            f"跳过无效关系{len(skipped_rels)}条"
        )
        return all_rel_ids

    def batch_update_node_props(
            self,
            label: str,
            prop_data: List[Dict],
            unique_key: str,
            batch_size: int = 15,
            retry_times: int = 3,
            retry_delay: int = 2
    ) -> int:
        total_updated = 0
        skipped_nodes = []
        for data in prop_data:
            if unique_key not in data:
                raise KeyError(f"属性数据缺少唯一标识键[{unique_key}]，数据：{data}")
            # 新增：检查unique_key的值是否为NaN
            unique_val = data[unique_key]
            if isinstance(unique_val, float) and math.isnan(unique_val):
                skipped_nodes.append(f"唯一键值为NaN：{data}")
                logger.warning(f"属性更新过滤：唯一键值为NaN，跳过该数据：{data}")
                continue

        for batch_idx in range(0, len(prop_data), batch_size):
            batch_data = prop_data[batch_idx:batch_idx + batch_size]
            batch_updated = 0
            retry_count = 0
            while retry_count < retry_times:
                try:
                    with self.conn_manager.get_session() as session:
                        with session.begin_transaction() as tx:
                            for item in batch_data:
                                unique_val = item[unique_key]
                                # 跳过NaN的unique_val
                                if isinstance(unique_val, float) and math.isnan(unique_val):
                                    continue
                                update_props = {k: v for k, v in item.items() if k != unique_key}
                                # 过滤更新属性中的NaN
                                update_props = {k: v for k, v in update_props.items() if
                                                not (isinstance(v, float) and math.isnan(v))}
                                if not update_props:
                                    logger.warning(
                                        f"属性更新-批次{batch_idx // batch_size + 1}："
                                        f"唯一值[{unique_val}]无待更新属性，跳过"
                                    )
                                    continue

                                check_cypher = f"MATCH (n:{label} {{{unique_key}: ${unique_key}}}) RETURN count(n) > 0 AS exist"
                                check_result = tx.run(check_cypher, **{unique_key: unique_val})
                                node_exist = check_result.single()["exist"]

                                if not node_exist:
                                    skipped_nodes.append(unique_val)
                                    logger.warning(
                                        f"属性更新-批次{batch_idx // batch_size + 1}："
                                        f"节点[{label}-{unique_val}]不存在，跳过更新"
                                    )
                                    continue

                                update_cypher = (
                                    f"MATCH (n:{label} {{{unique_key}: ${unique_key}}}) "
                                    "SET n += $update_props "
                                    "RETURN elementId(n) AS node_id"
                                )
                                params = {unique_key: unique_val, "update_props": update_props}
                                result = tx.run(update_cypher, **params)

                                if result.single():
                                    total_updated += 1
                                    batch_updated += 1
                                    logger.debug(
                                        f"属性更新成功：标签[{label}]，"
                                        f"唯一值[{unique_val}]，属性：{update_props}"
                                    )

                            tx.commit()
                            logger.info(
                                f"属性更新-批次{batch_idx // batch_size + 1}："
                                f"处理{len(batch_data)}条数据，成功更新{batch_updated}个节点"
                            )
                        break
                except TransientError as e:
                    retry_count += 1
                    logger.warning(
                        f"属性更新-批次{batch_idx // batch_size + 1}："
                        f"临时错误，重试{retry_count}/{retry_times}，错误：{str(e)}"
                    )
                    time.sleep(retry_delay * retry_count)
                except (Neo4jError, Exception) as e:
                    logger.error(
                        f"属性更新-批次{batch_idx // batch_size + 1}："
                        f"非临时错误，错误：{str(e)}"
                    )
                    raise
            else:
                raise Exception(
                    f"属性更新-批次{batch_idx // batch_size + 1}："
                    f"重试{retry_times}次仍失败"
                )

        if skipped_nodes:
            logger.warning(
                f"属性更新完成：共跳过{len(skipped_nodes)}个不存在/无效的节点，列表：{skipped_nodes}"
            )
        return total_updated

    # 通用检查节点方法
    def check_nodes_exist(
            self,
            label: str,
            unique_vals: List[str],
            unique_key: str
    ) -> Dict[str, bool]:
        """
        通用检查节点是否存在（适配“已存在节点”校验）
        :param label: 节点标签
        :param unique_vals: 待检查的唯一值列表
        :param unique_key: 唯一标识属性名
        :return: {唯一值: 是否存在, ...}
        """
        exist_map = {}
        with self.conn_manager.get_session() as session:
            for val in unique_vals:
                cypher = f"MATCH (n:{label} {{{unique_key}: $val}}) RETURN count(n) > 0 AS exist"
                result = session.run(cypher, val=val)
                exist_map[val] = result.single()["exist"]
        # 校验是否有不存在的节点
        not_exist = [val for val, exist in exist_map.items() if not exist]
        if not_exist:
            raise ValueError(f"标签[{label}]下，以下节点不存在：{not_exist}")
        logger.info(f"校验通过：标签[{label}]下{len(unique_vals)}个节点均存在")
        return exist_map