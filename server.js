const WebSocket = require('ws');
const express = require('express');
const cors = require('cors');
const os = require('os');

const app = express();
app.use(cors());
const PORT = process.env.PORT || 3001;

// ===== STATE =====
let apiResponseData = {
    "Phien": null, "Xuc_xac_1": null, "Xuc_xac_2": null, "Xuc_xac_3": null,
    "Tong": null, "Ket_qua": "", "id": "@mrtinhios",
    "server_time": new Date().toISOString()
};
let currentSessionId = null;
const patternHistory = [];

// ===== CONFIG =====
const WEBSOCKET_URL = "wss://websocket.azhkthg1.net/websocket?token=eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJhbW91bnQiOjAsInVzZXJuYW1lIjoiU0NfYXBpc3Vud2luMTIzIn0.hgrRbSV6vnBwJMg9ZFtbx3rRu9mX_hZMZ_m5gMNhkw0";
const WS_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
    "Origin": "https://web.sunwin.osaka/?affId=Sunwin"
};

// ===== 🔍 KIỂM TRA TOKEN NGAY KHI KHỞI ĐỘNG =====
function checkToken() {
    try {
        const url = new URL(WEBSOCKET_URL);
        const token = url.searchParams.get('token');
        const payload = JSON.parse(Buffer.from(token.split('.')[1], 'base64').toString());
        console.log('🔑 Token payload:', payload);

        if (payload.exp) {
            const now = Math.floor(Date.now() / 1000);
            const left = payload.exp - now;
            if (left <= 0) {
                console.log(`❌ TOKEN ĐÃ HẾT HẠN ${Math.abs(left)}s trước!`);
                console.log(`   → Cần lấy token mới từ Sun.Win`);
            } else {
                console.log(`✅ Token còn ${Math.floor(left/60)} phút`);
            }
        } else {
            console.log('⚠️ Token không có exp - có thể không hết hạn hoặc format lạ');
        }
    } catch (e) {
        console.log('⚠️ Không decode được token:', e.message);
    }
}

// ===== LOGIN PAYLOAD =====
// ⚠️ QUAN TRỌNG: username trong info PHẢI KHỚP username đăng nhập
const LOGIN_INFO = JSON.stringify({
    ipAddress: "113.185.45.88",
    wsToken: "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJnZW5kZXIiOjAsImNhblZpZXdTdGF0IjpmYWxzZSwiZGlzcGxheU5hbWUiOiJwbGFtYW1hIiwiYm90IjowLCJpc01lcmNoYW50IjpmYWxzZSwidmVyaWZpZWRCYW5rQWNjb3VudCI6ZmFsc2UsInBsYXlFdmVudExvYmJ5IjpmYWxzZSwiY3VzdG9tZXJJZCI6MzMxNDgxMTYyLCJhZmZJZCI6IkdFTVdJTiIsImJhbm5lZCI6ZmFsc2UsImJyYW5kIjoiZ2VtIiwidGltZXN0YW1wIjoxNzY2NDc0NzgwMDA2LCJsb2NrR2FtZXMiOltdLCJhbW91bnQiOjAsImxvY2tDaGF0IjpmYWxzZSwicGhvbmVWZXJpZmllZCI6ZmFsc2UsImlwQWRkcmVzcyI6IjExMy4xODUuNDUuODgiLCJtdXRlIjpmYWxzZSwiYXZhdGFyIjoiaHR0cHM6Ly9pbWFnZXMuc3dpbnNob3AubmV0L2ltYWdlcy9hdmF0YXIvYXZhdGFyXzE4LnBuZyIsInBsYXRmb3JtSWQiOjUsInVzZXJJZCI6IjZhOGI0ZDM4LTFlYzEtNDUxYi1hYTA1LWYyZDkwYWFhNGM1MCIsInJlZ1RpbWUiOjE3NjY0NzQ3NTEzOTEsInBob25lIjoiIiwiZGVwb3NpdCI6ZmFsc2UsInVzZXJuYW1lIjoiR01fYXBpdm9wbmhhYW4ifQ.YFOscbeojWNlRo7490BtlzkDGYmwVpnlgOoh04oCJy4",
    locale: "vi",
    userId: "6a8b4d38-1ec1-451b-aa05-f2d90aaa4c50",
    username: "bucutaoko1",
    timestamp: Date.now(),
    refreshToken: "63d5c9be0c494b74b53ba150d69039fd.7592f06d63974473b4aaa1ea849b2940"
});

const LOGIN_SIGNATURE = "66772A1641AA8B18BD99207CE448EA00ECA6D8A4D457C1FF13AB092C22C8DECF0C0014971639A0FBA9984701A91FCCBE3056ABC1BE1541D1C198AA18AF3C45595AF6601F8B048947ADF8F48A9E3E074162F9BA3E6C0F7543D38BD54FD4C0A2C56D19716CC5353BBC73D12C3A92F78C833F4EFFDC4AB99E55C77AD2CDFA91E296";

const initialMessages = [
    [1, "MiniGame", "bucutaoko1", "123456h", {
        info: LOGIN_INFO,
        signature: LOGIN_SIGNATURE
    }],
    [6, "MiniGame", "bucutaoko1", { cmd: 1005 }],
    [6, "MiniGame", "123456h", { cmd: 10001 }]
];

// ===== GET IP =====
const getNetworkInfo = () => {
    const interfaces = os.networkInterfaces();
    for (const name in interfaces) {
        for (const iface of interfaces[name]) {
            if (!iface.internal && iface.family === 'IPv4') return { localIP: iface.address };
        }
    }
    return { localIP: '127.0.0.1' };
};

// ===== WEBSOCKET =====
let ws = null, pingInterval = null, reconnectTimeout = null;

function connectWebSocket() {
    console.log('\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━');
    console.log('🔌 Đang kết nối WebSocket...');
    console.log('URL:', WEBSOCKET_URL.slice(0, 80) + '...');

    try {
        if (ws) { ws.removeAllListeners(); try { ws.terminate(); } catch(_){} }
    } catch(_){}

    ws = new WebSocket(WEBSOCKET_URL, { headers: WS_HEADERS, handshakeTimeout: 10000 });

    ws.on('open', () => {
        console.log('✅ [OPEN] Kết nối thành công!');
        console.log('   Chuẩn bị gửi', initialMessages.length, 'packet...\n');

        initialMessages.forEach((msg, i) => {
            setTimeout(() => {
                if (ws.readyState === WebSocket.OPEN) {
                    const preview = JSON.stringify(msg).slice(0, 120);
                    console.log(`📤 [SEND #${i+1}]`, preview + (preview.length >= 120 ? '...' : ''));
                    ws.send(JSON.stringify(msg));
                }
            }, i * 800);
        });

        clearInterval(pingInterval);
        pingInterval = setInterval(() => {
            if (ws.readyState === WebSocket.OPEN) {
                try { ws.ping(); } catch(_){}
            }
        }, 15000);
    });

    ws.on('pong', () => console.log('📶 [PONG] Ping OK'));

    // 🔍 LOG MỌI MESSAGE NHẬN ĐƯỢC
    ws.on('message', (message) => {
        const raw = message.toString();
        console.log('\n📥 [RECV RAW]', raw.length > 300 ? raw.slice(0, 300) + '...' : raw);

        try {
            const data = JSON.parse(raw);
            if (!Array.isArray(data) || !data[1] || typeof data[1] !== 'object') {
                console.log('   ⚠️ Không phải packet chuẩn, bỏ qua');
                return;
            }

            const { cmd, sid, d1, d2, d3, gBB } = data[1];
            console.log(`   📋 cmd=${cmd}, sid=${sid}, d=${d1}-${d2}-${d3}, gBB=${!!gBB}`);

            if (cmd === 1008 && sid) {
                currentSessionId = sid;
                console.log(`   🎮 Phiên mới: ${sid}`);
                return;
            }

            if (cmd === 1003) {
                const payload = (gBB && typeof gBB === 'object') ? gBB : data[1];
                const dd1 = payload.d1 ?? payload[0];
                const dd2 = payload.d2 ?? payload[1];
                const dd3 = payload.d3 ?? payload[2];

                if (!dd1 || !dd2 || !dd3) {
                    console.log('   ⚠️ cmd=1003 nhưng thiếu dice:', { dd1, dd2, dd3, payload });
                    return;
                }

                const total = dd1 + dd2 + dd3;
                const result = total > 10 ? "Tài" : "Xỉu";
                const session = currentSessionId ?? apiResponseData.Phien;

                apiResponseData = {
                    "Phien": session,
                    "Xuc_xac_1": dd1,
                    "Xuc_xac_2": dd2,
                    "Xuc_xac_3": dd3,
                    "Tong": total,
                    "Ket_qua": result,
                    "id": "@mrtinhios",
                    "server_time": new Date().toISOString(),
                    "update_count": (apiResponseData.update_count || 0) + 1
                };

                console.log(`   🎲 KẾT QUẢ: Phiên ${session} | ${dd1}-${dd2}-${dd3} = ${total} (${result})`);

                patternHistory.push({ session, dice: [dd1,dd2,dd3], total, result, timestamp: new Date().toISOString() });
                if (patternHistory.length > 100) patternHistory.shift();
                currentSessionId = null;
            }
        } catch (e) {
            console.log('   ❌ Parse error:', e.message);
        }
    });

    ws.on('unexpected-response', (req, res) => {
        console.log(`\n❌ [UNEXPECTED RESPONSE] HTTP ${res.statusCode}`);
        let body = '';
        res.on('data', c => body += c);
        res.on('end', () => {
            console.log('   Body:', body.slice(0, 500));
            console.log('   → Token hoặc header bị server reject!');
        });
    });

    ws.on('close', (code, reason) => {
        const r = reason.toString();
        console.log(`\n❌ [CLOSE] Code=${code} Reason=${r || '(none)'}`);
        if (code === 1006) console.log('   → 1006: kết nối bị ngắt bất thường (token/header sai hoặc mạng)');
        if (code === 1008) console.log('   → 1008: policy violation (login sai)');
        if (code === 4001) console.log('   → 4001: unauthorized (token hết hạn)');
        clearInterval(pingInterval);
        clearTimeout(reconnectTimeout);
        reconnectTimeout = setTimeout(connectWebSocket, 3000);
    });

    ws.on('error', (err) => {
        console.log(`\n❌ [ERROR] ${err.message}`);
        if (err.message.includes('401') || err.message.includes('403')) {
            console.log('   → Token bị từ chối!');
        }
        try { ws.terminate(); } catch(_){}
    });
}

// ===== ROUTES =====
app.get('/api/ditmemaysun', (req, res) => {
    res.set('Cache-Control', 'no-store');
    res.json(apiResponseData);
});
app.get('/api/history', (req, res) => {
    res.json({ current: apiResponseData, history: patternHistory.slice(-20), total: patternHistory.length });
});
app.get('/api/stats', (req, res) => {
    const t = patternHistory.length;
    const tai = patternHistory.filter(x => x.result === "Tài").length;
    res.json({
        total_sessions: t, tai_count: tai, xiu_count: t - tai,
        tai_percentage: t ? ((tai/t)*100).toFixed(2) : "0.00",
        xiu_percentage: t ? (((t-tai)/t)*100).toFixed(2) : "0.00",
        last_update: apiResponseData.server_time
    });
});
app.get('/api/health', (req, res) => {
    res.json({
        status: 'online',
        websocket: ws ? ws.readyState === WebSocket.OPEN : false,
        readyState: ws ? ws.readyState : -1,
        uptime: process.uptime().toFixed(0) + 's',
        total_updates: apiResponseData.update_count || 0
    });
});
app.get('/', (req, res) => {
    res.send(`<pre style="background:#000;color:#0f0;padding:20px;font-size:14px;">
🚀 Sun.Win Stream đang chạy

API:
  http://localhost:${PORT}/api/ditmemaysun
  http://localhost:${PORT}/api/history
  http://localhost:${PORT}/api/stats
  http://localhost:${PORT}/api/health

📺 Xem log console để biết trạng thái kết nối!
</pre>`);
});

// ===== START =====
app.listen(PORT, '0.0.0.0', () => {
    const net = getNetworkInfo();
    console.log(`\n╔═══════════════════════════════════════════╗`);
    console.log(`║  🚀 Sun.Win Stream • ${net.localIP}:${PORT}      ║`);
    console.log(`╚═══════════════════════════════════════════╝`);

    console.log('\n🔍 KIỂM TRA TOKEN...');
    checkToken();

    console.log('\n🔌 Bắt đầu kết nối...');
    connectWebSocket();
});
