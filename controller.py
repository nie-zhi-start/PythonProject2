# controller.py（原qa_controller.py，文件名保持controller.py）
from fastapi import FastAPI, Query, HTTPException
from fastapi.responses import StreamingResponse
from fastapi.middleware.cors import CORSMiddleware  # 新增跨域配置（可选但推荐）
from question import get_qa_answer_stream  # 确保qa_engine.py和controller.py在同一目录
import logging
import os

# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# 初始化FastAPI应用
app = FastAPI(title="代茶饮知识问答API", version="1.0")

# 新增跨域配置（解决前端调用跨域问题）
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # 开发环境允许所有来源，生产环境替换为前端域名
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# 核心接口：代茶饮问答（流式返回）
@app.get("/api/qa", summary="代茶饮知识问答接口（流式返回）")
async def qa_interface(
    question: str = Query(..., min_length=1, max_length=500, description="用户提出的问题")
):
    """
    接收前端GET请求，流式返回问答结果
    :param question: 用户的问题（必填，1-500字符）
    :return: 流式文本响应
    """
    try:
        # 调用引擎层的流式生成方法
        return StreamingResponse(
            get_qa_answer_stream(question),
            media_type="text/plain; charset=utf-8"  # 确保中文不乱码
        )
    except Exception as e:
        logger.error(f"接口处理失败: {str(e)}")
        raise HTTPException(status_code=500, detail=f"服务器内部错误：{str(e)}")

# 启动服务（关键修改：模块名改为controller，适配文件名）
if __name__ == "__main__":
    import uvicorn
    # 修复：将"qa_controller:app"改为"controller:app"（对应文件名controller.py）
    # 新增：关闭reload模式（避免重复初始化数据库），开发时如需reload需额外处理
    uvicorn.run(
        "controller:app",  # 核心修改！模块名=文件名（无.py）
        host="0.0.0.0",    # 允许外部访问
        port=8001,         # 你指定的端口
        reload=False       # 先关闭reload，避免重复连接数据库（后续可优化）
    )