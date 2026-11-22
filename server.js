const WebSocket = require('ws');
const express = require('express');
const cors = require('cors');
const os = require('os');

const app = express();
app.use(cors());
const PORT = process.env.PORT || 3001;

// ================== STATE ==================
let apiResponseData = {
    "Phien": null,
    "Xuc_xac_1": null,
    "Xuc_xac_2": null,
    "Xuc_xac_3": null,
    "Tong": null,
    "Ket_qua": "",
    "id": "@mrtinhios",
    "server_time": new Date().toISOString()
};

let currentSessionId = null;
const patternHistory = [];
const MAX_HISTORY = 100;

let ws = null;
let pingInterval = null;
let reconnectTimeout = null;
let isConnecting = false;

// ================== CONFIG ==================
const WEBSOCKET_URL = "wss://websocket.azhkthg1.net/websocket?token=eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJhbW91bnQiOjAsInVzZXJuYW1lIjoiU0NfYXBpc3Vud2luMTIzIn0.hgrRbSV6vnBwJMg9ZFtbx3rRu9mX_hZMZ_m5gMNhkw0";

const WS_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
    "Origin": "https://web.sunwin.osaka/?affId=Sunwin"
};

const RECONNECT_DELAY = 3000;
const PING_INTERVAL = 15000;

// ================== LOGIN PAYLOAD ==================
// TK: bucutaoko1 | MK: 123456h
const LOGIN_INFO = JSON.stringify({
    ipAddress: "113.185.45.88",
    wsToken: "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJnZW5kZXIiOjAsImNhblZpZXdTdGF0IjpmYWxzZSwiZGlzcGxheU5hbWUiOiJwbGFtYW1hIiwiYm90IjowLCJpc01lcmNoYW50IjpmYWxzZSwidmVyaWZpZWRCYW5rQWNjb3VudCI6ZmFsc2UsInBsYXlFdmVudExvYmJ5IjpmYWxzZSwiY3VzdG9tZXJJZCI6MzMxNDgxMTYyLCJhZmZJZCI6IkdFTVdJTiIsImJhbm5lZCI6ZmFsc2UsImJyYW5kIjoiZ2VtIiwidGltZXN0YW1wIjoxNzY2NDc0NzgwMDA2LCJsb2NrR2FtZXMiOltdLCJhbW91bnQiOjAsImxvY2tDaGF0IjpmYWxzZSwicGhvbmVWZXJpZmllZCI6ZmFsc2UsImlwQWRkcmVzcyI6IjExMy4xODUuNDUuODgiLCJtdXRlIjpmYWxzZSwiYXZhdGFyIjoiaHR0cHM6Ly9pbWFnZXMuc3dpbnNob3AubmV0L2ltYWdlcy9hdmF0YXIvYXZhdGFyXzE4LnBuZyIsInBsYXRmb3JtSWQiOjUsInVzZXJJZCI6IjZhOGI0ZDM4LTFlYzEtNDUxYi1hYTA1LWYyZDkwYWFhNGM1MCIsInJlZ1RpbWUiOjE3NjY0NzQ3NTEzOTEsInBob25lIjoiIiwiZGVwb3NpdCI6ZmFsc2UsInVzZXJuYW1lIjoiR01fYXBpdm9wbmhhYW4ifQ.YFOscbeojWNlRo7490BtlzkDGYmwVpnlgOoh04oCJy4",
    locale: "vi",
    userId: "6a8b4d38-1ec1-451b-aa05-f2d90aaa4c50",
    username: "GM_apivopnhaan",
    timestamp: Date.now(),
    refreshToken: "63d5c9be0c494b74b53ba150d69039fd.7592f06d63974473b4aaa1ea849b2940"
});

const LOGIN_SIGNATURE = "66772A1641AA8B18BD99207CE448EA00ECA6D8A4D457C1FF13AB092C22C8DECF0C0014971639A0FBA9984701A91FCCBE3056ABC1BE1541D1C198AA18AF3C45595AF6601F8B048947ADF8F48A9E3E074162F9BA3E6C0F7543D38BD54FD4C0A2C56D19716CC5353BBC73D12C3A92F78C833F4EFFDC4AB99E55C77AD2CDFA91E296";

const initialMessages = [
    // 1. LOGIN - dùng TK/MK thật
    [1, "MiniGame", "bucutaoko1", "123456h", {
        info: LOGIN_INFO,
        signature: LOGIN_SIGNATURE
    }],
    // 2. Subscribe plugin tài xỉu
    [6, "MiniGame", "taixiuPlugin", { cmd: 1005 }],
    // 3. Subscribe plugin lobby
    [6, "MiniGame", "lobbyPlugin", { cmd: 10001 }]
];

// ================== HELPERS ==================
const getNetworkInfo = () => {
    const interfaces = os.networkInterfaces();
    let localIP = '127.0.0.1';
    for (const name in interfaces) {
        for (const iface of interfaces[name]) {
            if (!iface.internal && iface.family === 'IPv4') {
                localIP = iface.address;
                return { localIP };
            }
        }
    }
    return { localIP };
};

// ================== WEBSOCKET ==================
function connectWebSocket() {
    if (isConnecting) return;
    isConnecting = true;

    try {
        if (ws) {
            ws.removeAllListeners();
            try { ws.terminate(); } catch (_) {}
        }
    } catch (_) {}

    console.log('[🔌] Connecting to Sun.Win...');
    ws = new WebSocket(WEBSOCKET_URL, {
        headers: WS_HEADERS,
        handshakeTimeout: 10000
    });

    ws.on('open', () => {
        isConnecting = false;
        console.log('[✅] WebSocket connected');

        // Gửi tuần tự từng message
        initialMessages.forEach((msg, i) => {
            setTimeout(() => {
                if (ws.readyState === WebSocket.OPEN) {
                    ws.send(JSON.stringify(msg));
                    console.log(`[📤] Sent packet #${i + 1}: ${msg[2]}`);
                }
            }, i * 800);
        });

        // Ping giữ kết nối
        clearInterval(pingInterval);
        pingInterval = setInterval(() => {
            if (ws.readyState === WebSocket.OPEN) {
                try { ws.ping(); } catch (_) {}
            }
        }, PING_INTERVAL);
    });

    ws.on('pong', () => {
        console.log('[📶] Ping OK');
    });

    ws.on('message', (message) => {
        try {
            const raw = message.toString();
            const data = JSON.parse(raw);

            if (!Array.isArray(data) || typeof data[1] !== 'object' || data[1] === null) {
                return;
            }

            const { cmd, sid, d1, d2, d3, gBB } = data[1];

            // ---- Phiên mới ----
            if (cmd === 1008 && sid) {
                currentSessionId = sid;
                console.log(`[🎮] Phiên mới: ${sid}`);
                return;
            }

            // ---- Kết quả tài xỉu ----
            if (cmd === 1003) {
                // Hỗ trợ cả 2 format: gBB là array/object hoặc dice nằm trực tiếp
                const payload = (gBB && typeof gBB === 'object') ? gBB : data[1];
                const dd1 = payload.d1 ?? payload[0];
                const dd2 = payload.d2 ?? payload[1];
                const dd3 = payload.d3 ?? payload[2];

                if (!dd1 || !dd2 || !dd3) return;

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

                console.log(`[🎲] Phiên ${session}: ${dd1}-${dd2}-${dd3} = ${total} (${result})`);

                patternHistory.push({
                    session,
                    dice: [dd1, dd2, dd3],
                    total,
                    result,
                    timestamp: new Date().toISOString()
                });
                if (patternHistory.length > MAX_HISTORY) patternHistory.shift();

                // Reset phiên để packet 1008 kế tiếp cập nhật
                currentSessionId = null;
            }
        } catch (e) {
            // Bỏ qua log rác
        }
    });

    ws.on('close', (code, reason) => {
        isConnecting = false;
        console.log(`[🔌] Closed. Code=${code} Reason=${reason.toString() || 'none'}`);
        if (code === 1006 || code === 1008 || code === 4001 || code === 4003) {
            console.warn('⚠️  Có thể token/đăng nhập sai hoặc hết hạn!');
        }
        clearInterval(pingInterval);
        clearTimeout(reconnectTimeout);
        reconnectTimeout = setTimeout(connectWebSocket, RECONNECT_DELAY);
    });

    ws.on('error', (err) => {
        isConnecting = false;
        console.error('[❌] WS error:', err.message);
        try { ws.terminate(); } catch (_) {}
    });
}

// ================== ROUTES ==================
app.get('/api/ditmemaysun', (req, res) => {
    res.set('Cache-Control', 'no-store');
    res.json(apiResponseData);
});

app.get('/api/history', (req, res) => {
    res.json({
        current: apiResponseData,
        history: patternHistory.slice(-20),
        total_sessions: patternHistory.length,
        total_requests: apiResponseData.update_count || 0
    });
});

app.get('/api/stats', (req, res) => {
    const total = patternHistory.length;
    const taiCount = patternHistory.filter(x => x.result === "Tài").length;
    const xiuCount = total - taiCount;

    res.json({
        total_sessions: total,
        tai_count: taiCount,
        xiu_count: xiuCount,
        tai_percentage: total > 0 ? ((taiCount / total) * 100).toFixed(2) : "0.00",
        xiu_percentage: total > 0 ? ((xiuCount / total) * 100).toFixed(2) : "0.00",
        last_update: apiResponseData.server_time,
        server_uptime: process.uptime().toFixed(0) + 's'
    });
});

app.get('/api/health', (req, res) => {
    res.json({
        status: 'online',
        websocket: ws ? ws.readyState === WebSocket.OPEN : false,
        readyState: ws ? ws.readyState : -1,
        uptime: process.uptime().toFixed(0) + 's',
        memory_mb: (process.memoryUsage().rss / 1024 / 1024).toFixed(2),
        current_session: currentSessionId,
        last_result: apiResponseData.Tong,
        total_updates: apiResponseData.update_count || 0
    });
});

// ================== HOME PAGE ==================
app.get('/', (req, res) => {
    const net = getNetworkInfo();
    const html = `
    <!DOCTYPE html>
    <html lang="vi">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Sun.Win Live Data Stream</title>
        <style>
            * { box-sizing: border-box; margin: 0; padding: 0; }
            body { font-family: 'Segoe UI', Arial, sans-serif; background: #0a0e1a; color: #e0e0e0; min-height: 100vh; padding: 20px; }
            .container { max-width: 1100px; margin: 0 auto; }
            .header { text-align: center; padding: 30px 20px; background: linear-gradient(135deg, #1a1f35, #0f1420); border: 1px solid #2a3050; border-radius: 16px; margin-bottom: 20px; }
            .header h1 { color: #00ff88; font-size: 1.8em; margin-bottom: 8px; }
            .header p { color: #7a8298; font-size: 0.9em; }
            .grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(320px, 1fr)); gap: 16px; }
            .card { background: #131825; border: 1px solid #2a3050; border-radius: 14px; padding: 20px; }
            .card h2 { color: #00d4ff; font-size: 1em; margin-bottom: 15px; text-transform: uppercase; letter-spacing: 1px; }
            .result-box { text-align: center; padding: 25px 15px; background: #0a0e1a; border-radius: 10px; margin-bottom: 12px; }
            .dice { font-size: 2.2em; font-weight: bold; letter-spacing: 4px; margin-bottom: 8px; }
            .dice.tai { color: #00ff88; text-shadow: 0 0 20px #00ff8855; }
            .dice.xiu { color: #ff4466; text-shadow: 0 0 20px #ff446655; }
            .dice.waiting { color: #555; font-size: 1.2em; }
            .result-label { font-size: 1.3em; font-weight: bold; }
            .info-row { display: flex; justify-content: space-between; padding: 8px 0; border-bottom: 1px solid #1f2535; font-size: 0.9em; }
            .info-row:last-child { border-bottom: none; }
            .info-row span:first-child { color: #7a8298; }
            .info-row span:last-child { color: #00d4ff; font-weight: 600; }
            .api-list { list-style: none; }
            .api-list li { margin-bottom: 10px; }
            .api-list a { color: #00d4ff; text-decoration: none; padding: 10px 14px; display: block; background: #0a0e1a; border-radius: 8px; border: 1px solid #1f2535; transition: all 0.2s; font-family: monospace; font-size: 0.9em; }
            .api-list a:hover { background: #1a1f35; border-color: #00d4ff; }
            .status-dot { display: inline-block; width: 10px; height: 10px; border-radius: 50%; margin-right: 8px; }
            .status-dot.on { background: #00ff88; box-shadow: 0 0 10px #00ff88; animation: pulse 2s infinite; }
            .status-dot.off { background: #ff4466; }
            @keyframes pulse { 0%,100%{opacity:1} 50%{opacity:0.5} }
            .history-list { max-height: 300px; overflow-y: auto; }
            .history-item { display: flex; justify-content: space-between; padding: 8px 10px; border-radius: 6px; margin-bottom: 5px; background: #0a0e1a; font-size: 0.85em; }
            .history-item.tai { border-left: 3px solid #00ff88; }
            .history-item.xiu { border-left: 3px solid #ff4466; }
        </style>
    </head>
    <body>
        <div class="container">
            <div class="header">
                <h1>🔴 Sun.Win Live Stream</h1>
                <p>Data API • <span id="status">Connecting...</span></p>
                <p style="margin-top:8px;font-family:monospace;font-size:0.8em;">Server: ${net.localIP}:${PORT}</p>
            </div>

            <div class="grid">
                <div class="card">
                    <h2>🎲 Kết Quả Hiện Tại</h2>
                    <div class="result-box">
                        <div class="dice waiting" id="dice">Waiting...</div>
                        <div class="result-label" id="result"></div>
                    </div>
                    <div class="info-row"><span>Phiên</span><span id="session">-</span></div>
                    <div class="info-row"><span>Tổng</span><span id="total">-</span></div>
                    <div class="info-row"><span>Thời gian</span><span id="time">-</span></div>
                </div>

                <div class="card">
                    <h2>📊 API Endpoints</h2>
                    <ul class="api-list">
                        <li><a href="/api/ditmemaysun" target="_blank">/api/ditmemaysun</a></li>
                        <li><a href="/api/history" target="_blank">/api/history</a></li>
                        <li><a href="/api/stats" target="_blank">/api/stats</a></li>
                        <li><a href="/api/health" target="_blank">/api/health</a></li>
                    </ul>
                </div>

                <div class="card">
                    <h2>📈 Lịch Sử Gần Đây</h2>
                    <div class="history-list" id="history">
                        <p style="color:#555;text-align:center;padding:20px;">Chưa có dữ liệu...</p>
                    </div>
                </div>
            </div>
        </div>

        <script>
            function fmtTime(iso) {
                if (!iso) return '-';
                const d = new Date(iso);
                return d.toLocaleTimeString('vi-VN');
            }

            async function refresh() {
                try {
                    const [cur, hist, health] = await Promise.all([
                        fetch('/api/ditmemaysun').then(r => r.json()),
                        fetch('/api/history').then(r => r.json()),
                        fetch('/api/health').then(r => r.json())
                    ]);

                    const dice = document.getElementById('dice');
                    const result = document.getElementById('result');
                    const status = document.getElementById('status');

                    // Status
                    status.innerHTML = health.websocket
                        ? '<span class="status-dot on"></span>Online'
                        : '<span class="status-dot off"></span>Disconnected';

                    // Dice
                    if (cur.Tong) {
                        dice.textContent = cur.Xuc_xac_1 + ' - ' + cur.Xuc_xac_2 + ' - ' + cur.Xuc_xac_3;
                        dice.className = 'dice ' + (cur.Ket_qua === 'Tài' ? 'tai' : 'xiu');
                        result.textContent = '= ' + cur.Tong + ' • ' + cur.Ket_qua;
                        result.style.color = cur.Ket_qua === 'Tài' ? '#00ff88' : '#ff4466';
                    } else {
                        dice.textContent = 'Waiting...';
                        dice.className = 'dice waiting';
                        result.textContent = '';
                    }

                    document.getElementById('session').textContent = cur.Phien || '-';
                    document.getElementById('total').textContent = cur.Tong || '-';
                    document.getElementById('time').textContent = fmtTime(cur.server_time);

                    // History
                    const histEl = document.getElementById('history');
                    if (hist.history && hist.history.length > 0) {
                        histEl.innerHTML = hist.history.slice().reverse().map(h =>
                            \`<div class="history-item \${h.result === 'Tài' ? 'tai' : 'xiu'}">
                                <span>#\${h.session || '?'} • \${h.dice.join('-')}=\${h.total}</span>
                                <span style="color:\${h.result === 'Tài' ? '#00ff88' : '#ff4466'}">\${h.result}</span>
                            </div>\`
                        ).join('');
                    }
                } catch (e) {
                    document.getElementById('status').innerHTML = '<span class="status-dot off"></span>Error';
                }
            }

            refresh();
            setInterval(refresh, 3000);
        </script>
    </body>
    </html>`;
    res.send(html);
});

// ================== START ==================
app.listen(PORT, '0.0.0.0', () => {
    const net = getNetworkInfo();
    console.log(`\n╔════════════════════════════════════════════╗`);
    console.log(`║  🚀 Sun.Win Live Data Stream               ║`);
    console.log(`╚════════════════════════════════════════════╝`);
    console.log(`  Local:   http://localhost:${PORT}`);
    console.log(`  Network: http://${net.localIP}:${PORT}`);
    console.log(`  Public:  http://<VPS_IP>:${PORT}`);
    console.log(`─────────────────────────────────────────────`);
    console.log(`  🔌 Đang kết nối tới Sun.Win WebSocket...`);
    console.log(`─────────────────────────────────────────────\n`);

    connectWebSocket();

    // Watchdog: reconnect nếu ws chết mà không tự reconnect
    setInterval(() => {
        if (!ws || ws.readyState === WebSocket.CLOSED) {
            console.log('[⏰] Watchdog: WS closed, reconnecting...');
            connectWebSocket();
        }
    }, 30000);
});
