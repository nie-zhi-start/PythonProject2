const { createApp, ref, computed, nextTick, onMounted } = Vue;

const API_BASE = "http://localhost:8001/api/qa";

const createChatId = () => {
  if (crypto && crypto.randomUUID) {
    return crypto.randomUUID();
  }
  return `chat_${Date.now()}_${Math.random().toString(16).slice(2)}`;
};

createApp({
  setup() {
    const chatId = ref("");
    const input = ref("");
    const isStreaming = ref(false);
    const messages = ref([
      {
        id: "intro",
        role: "ai",
        text: "你好，我是代茶饮知识图谱助手。你可以问我：清热解毒适合哪些代茶饮？",
      },
    ]);

    const canSend = computed(() => input.value.trim().length > 0 && !isStreaming.value);

    const scrollToBottom = async () => {
      await nextTick();
      const list = document.querySelector(".chat-list");
      if (list) {
        list.scrollTop = list.scrollHeight;
      }
    };

    const pushMessage = (role, text) => {
      const id = `${role}_${Date.now()}_${Math.random().toString(16).slice(2)}`;
      messages.value.push({ id, role, text });
      scrollToBottom();
      return id;
    };

    const updateMessage = (id, text) => {
      const msg = messages.value.find((item) => item.id === id);
      if (msg) {
        msg.text = text;
        scrollToBottom();
      }
    };

    const streamAnswer = async (question, aiId) => {
      const url = `${API_BASE}?question=${encodeURIComponent(question)}`;
      if (!window.fetch) {
        const response = await axios.get(API_BASE, {
          params: { question },
          responseType: "text",
          headers: {
            Accept: "text/plain",
          },
          transitional: {
            forcedJSONParsing: false,
          },
          transformResponse: [(data) => data],
        });
        updateMessage(aiId, response?.data || "");
        return;
      }

      const response = await fetch(url, {
        headers: {
          Accept: "text/plain",
        },
      });

      if (!response.ok || !response.body) {
        throw new Error(`请求失败: ${response.status}`);
      }

      const reader = response.body.getReader();
      const decoder = new TextDecoder("utf-8");
      let fullText = "";

      while (true) {
        const { value, done } = await reader.read();
        if (done) {
          break;
        }
        if (value) {
          fullText += decoder.decode(value, { stream: true });
          updateMessage(aiId, fullText);
        }
      }

      fullText += decoder.decode();
      updateMessage(aiId, fullText);
    };

    const presetQuestions = [
      "金银花茶有什么功效？",
      "夏季适合喝什么茶？",
      "清热解毒的代茶饮有哪些？",
      "上火时可以喝什么茶？",
    ];

    const selectPreset = (text) => {
      input.value = text;
    };

    const sendQuestion = async () => {
      if (!canSend.value) {
        return;
      }

      const question = input.value.trim();
      input.value = "";
      pushMessage("user", question);

      const aiId = pushMessage("ai", "正在思考...\n");
      isStreaming.value = true;

      try {
        await streamAnswer(question, aiId);
      } catch (error) {
        updateMessage(aiId, "抱歉，接口调用失败，请稍后再试。");
      } finally {
        isStreaming.value = false;
      }
    };

    onMounted(() => {
      chatId.value = createChatId();
      scrollToBottom();
    });

    return {
      chatId,
      input,
      messages,
      isStreaming,
      canSend,
      presetQuestions,
      selectPreset,
      sendQuestion,
    };
  },
  template: `
    <div class="page">
      <header class="topbar">
        <div class="title">
          <h1>中药代茶饮知识图谱智能推荐</h1>
          <p>会话 ID：{{ chatId }}</p>
        </div>
        <div class="status" :class="{ active: isStreaming }">
          {{ isStreaming ? "AI 正在生成" : "在线" }}
        </div>
      </header>

      <section class="chat">
        <div class="chat-list">
          <div
            v-for="msg in messages"
            :key="msg.id"
            class="message"
            :class="msg.role"
          >
            <div class="bubble">
              <span v-if="msg.role === 'ai'" class="label">AI</span>
              <span v-else class="label">你</span>
              <p>{{ msg.text }}</p>
            </div>
          </div>
        </div>
      </section>

      <footer class="composer">
        <div class="input-row">
          <textarea
            v-model="input"
            placeholder="输入你的问题，例如：有哪些代茶饮适合清热解毒？"
            rows="2"
            @keydown.enter.exact.prevent="sendQuestion"
          ></textarea>
          <button :disabled="!canSend" @click="sendQuestion">发送</button>
        </div>
        <div class="chips">
          <button
            v-for="item in presetQuestions"
            :key="item"
            class="chip"
            type="button"
            @click="selectPreset(item)"
          >
            {{ item }}
          </button>
        </div>
      </footer>
    </div>
  `,
}).mount("#app");
