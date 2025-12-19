// test.js（适配node-fetch@2的流式版本）
const fetch = require('node-fetch');

async function getQAAnswer(question) {
  try {
    const encodedQuestion = encodeURIComponent(question);
    // 确保端口和controller一致（8001）
    const url = `http://localhost:8001/api/qa?question=${encodedQuestion}`;
    console.log('请求地址：', url);

    const response = await fetch(url, {
      timeout: 10000,
      headers: {
        'Accept': 'text/plain; charset=utf-8'
      }
    });

    if (!response.ok) {
      throw new Error(`接口返回错误：状态码 ${response.status}，信息 ${response.statusText}`);
    }

    console.log('开始接收回答：');
    let answer = '';

    // 核心修改：node-fetch@2用on('data')处理流式数据
    response.body.on('data', (chunk) => {
      const text = chunk.toString('utf-8'); // 转UTF-8避免中文乱码
      answer += text;
      process.stdout.write(text); // 实时打印流式内容（模拟前端UI更新）
    });

    // 监听流结束事件
    response.body.on('end', () => {
      console.log('\n\n✅ 回答接收完成：');
      console.log(answer);
    });

    // 监听流错误事件
    response.body.on('error', (err) => {
      throw new Error(`流式接收失败：${err.message}`);
    });

  } catch (err) {
    console.error('❌ 调用失败：', err.message);
    console.error('完整错误信息：', err);
  }
}

// 调用示例
getQAAnswer('金银花茶有什么功效？');