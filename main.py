# backend/main.py — SmartTraderBot Pro FastAPI Backend
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
import asyncio, json, logging, random
from datetime import datetime

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("SmartTraderBot")

app = FastAPI(title="SmartTraderBot Pro API", version="2.0.0")

app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"])

# WebSocket manager
class WSManager:
    def __init__(self): self.connections: dict[str, WebSocket] = {}
    async def connect(self, ws: WebSocket, uid: str):
        await ws.accept(); self.connections[uid] = ws
    def disconnect(self, uid: str): self.connections.pop(uid, None)
    async def broadcast(self, data: dict):
        dead = []
        for uid, ws in self.connections.items():
            try: await ws.send_json(data)
            except: dead.append(uid)
        for uid in dead: self.disconnect(uid)

ws_manager = WSManager()

# In-memory store (replace with PostgreSQL/Supabase in production)
users_db = {}
trades_db = {}

FOREX_PAIRS = {
    "EURUSD": 1.08724, "GBPUSD": 1.26180, "USDJPY": 149.420,
    "AUDUSD": 0.64820, "USDCAD": 1.36540, "USDCHF": 0.88920,
    "NZDUSD": 0.59840, "EURGBP": 0.86120,
}

from fastapi import HTTPException
from pydantic import BaseModel, EmailStr
import hashlib, secrets, time

# ── Models ────────────────────────────────────────────────────────────────────
class RegisterReq(BaseModel):
    name: str
    email: str
    password: str
    plan: str = "free"

class LoginReq(BaseModel):
    email: str
    password: str

class TradeReq(BaseModel):
    symbol: str
    direction: str  # buy / sell
    lots: float
    sl: float
    tp: float
    comment: str = ""

class BrokerConnectReq(BaseModel):
    mt5_login: int
    mt5_password: str
    mt5_server: str

def hash_password(pw: str) -> str:
    return hashlib.sha256(pw.encode()).hexdigest()

def make_token(email: str) -> str:
    return secrets.token_hex(32)

def verify_token(token: str) -> dict | None:
    for uid, u in users_db.items():
        if u.get("token") == token:
            return u
    return None

from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from fastapi import Depends

security = HTTPBearer()

def get_current_user(creds: HTTPAuthorizationCredentials = Depends(security)):
    user = verify_token(creds.credentials)
    if not user: raise HTTPException(status_code=401, detail="Invalid or expired token")
    return user

# ── AUTH ──────────────────────────────────────────────────────────────────────
@app.post("/api/auth/register")
async def register(req: RegisterReq):
    if req.email in users_db:
        raise HTTPException(400, "Email already registered")
    uid = secrets.token_hex(8)
    token = make_token(req.email)
    users_db[req.email] = {
        "uid": uid, "name": req.name, "email": req.email,
        "password": hash_password(req.password),
        "plan": req.plan, "token": token,
        "created_at": datetime.utcnow().isoformat(),
        "broker_connected": False, "balance": 10000.0,
        "settings": {
            "risk_pct": 1.0, "max_daily_loss": 5.0, "max_daily_profit": 10.0,
            "max_trades": 3, "use_break_even": True, "use_trailing": True,
            "news_filter": True, "mtf_confirm": True, "use_smc": True,
            "rsi_period": 14, "ema_fast": 20, "ema_slow": 200,
            "atr_sl_mult": 1.5, "atr_tp_mult": 2.5,
            "telegram_token": "", "telegram_chat_id": "",
        }
    }
    logger.info(f"New user registered: {req.email} [{req.plan}]")
    return {"token": token, "user": {k: v for k, v in users_db[req.email].items() if k != "password"}}

@app.post("/api/auth/login")
async def login(req: LoginReq):
    user = users_db.get(req.email)
    if not user or user["password"] != hash_password(req.password):
        raise HTTPException(401, "Invalid credentials")
    token = make_token(req.email)
    user["token"] = token
    return {"token": token, "user": {k: v for k, v in user.items() if k != "password"}}

@app.post("/api/auth/demo")
async def demo_login():
    """Demo login — no credentials needed."""
    demo_email = f"demo_{secrets.token_hex(4)}@demo.com"
    token = make_token(demo_email)
    users_db[demo_email] = {
        "uid": "demo", "name": "Demo User", "email": demo_email,
        "password": "", "plan": "free", "token": token,
        "created_at": datetime.utcnow().isoformat(),
        "broker_connected": False, "balance": 10000.0,
        "settings": {"risk_pct":1.0,"max_daily_loss":5.0,"max_trades":3}
    }
    return {"token": token, "user": {"name":"Demo User","email":demo_email,"plan":"free","balance":10000.0}}

# ── USER ──────────────────────────────────────────────────────────────────────
@app.get("/api/users/me")
async def get_me(user=Depends(get_current_user)):
    return {k: v for k, v in user.items() if k not in ("password","token")}

@app.put("/api/users/settings")
async def update_settings(settings: dict, user=Depends(get_current_user)):
    user["settings"].update(settings)
    return {"status": "updated", "settings": user["settings"]}

@app.get("/api/users/portfolio")
async def get_portfolio(user=Depends(get_current_user)):
    user_trades = [t for t in trades_db.values() if t["user_email"] == user["email"]]
    closed = [t for t in user_trades if t["status"] == "closed"]
    open_t  = [t for t in user_trades if t["status"] == "open"]
    total_pnl = sum(t.get("pnl", 0) for t in closed)
    wins = [t for t in closed if t.get("pnl", 0) > 0]
    win_rate = len(wins) / len(closed) * 100 if closed else 0
    return {
        "balance":    user["balance"],
        "equity":     user["balance"] + sum(t.get("unrealized_pnl", 0) for t in open_t),
        "total_pnl":  round(total_pnl, 2),
        "pnl_pct":    round(total_pnl / 10000 * 100, 2),
        "open_trades": len(open_t),
        "total_trades": len(closed),
        "win_rate":   round(win_rate, 2),
        "daily_pnl":  round(sum(t.get("pnl",0) for t in closed if t.get("date","").startswith(datetime.utcnow().date().isoformat())), 2),
    }

# ── TRADES ────────────────────────────────────────────────────────────────────
@app.get("/api/trades")
async def get_trades(status: str = "all", user=Depends(get_current_user)):
    user_trades = [t for t in trades_db.values() if t["user_email"] == user["email"]]
    if status != "all": user_trades = [t for t in user_trades if t["status"] == status]
    return sorted(user_trades, key=lambda x: x["open_time"], reverse=True)

@app.post("/api/trades/open")
async def open_trade(req: TradeReq, user=Depends(get_current_user)):
    if user["plan"] == "free":
        open_t = [t for t in trades_db.values() if t["user_email"]==user["email"] and t["status"]=="open"]
        if len(open_t) >= 1:
            raise HTTPException(403, "Free plan: max 1 trade. Upgrade to Pro.")
    tid = secrets.token_hex(8)
    price = FOREX_PAIRS.get(req.symbol, 1.0)
    trade = {
        "id": tid, "user_email": user["email"],
        "symbol": req.symbol, "direction": req.direction,
        "lots": req.lots, "entry": price,
        "sl": req.sl, "tp": req.tp,
        "status": "open", "comment": req.comment,
        "open_time": datetime.utcnow().isoformat(),
        "unrealized_pnl": 0.0, "pnl": 0.0,
        "be_moved": False, "magic": 202400,
    }
    trades_db[tid] = trade
    logger.info(f"Trade opened: {req.direction} {req.symbol} [{user['email']}]")
    # Broadcast to user's WS
    await ws_manager.broadcast({"type": "trade_opened", "trade": trade})
    return trade

@app.post("/api/trades/{trade_id}/close")
async def close_trade(trade_id: str, user=Depends(get_current_user)):
    trade = trades_db.get(trade_id)
    if not trade or trade["user_email"] != user["email"]:
        raise HTTPException(404, "Trade not found")
    if trade["status"] == "closed":
        raise HTTPException(400, "Already closed")
    current_price = FOREX_PAIRS.get(trade["symbol"], trade["entry"])
    if trade["direction"] == "buy":
        pnl = (current_price - trade["entry"]) * trade["lots"] * 10000
    else:
        pnl = (trade["entry"] - current_price) * trade["lots"] * 10000
    trade.update({"status":"closed","close_time":datetime.utcnow().isoformat(),"close_price":current_price,"pnl":round(pnl,2)})
    user["balance"] += round(pnl, 2)
    await ws_manager.broadcast({"type": "trade_closed", "trade": trade})
    return trade

@app.get("/api/trades/stats")
async def get_stats(user=Depends(get_current_user)):
    closed = [t for t in trades_db.values() if t["user_email"]==user["email"] and t["status"]=="closed"]
    if not closed: return {"message": "No closed trades yet"}
    wins = [t for t in closed if t.get("pnl",0)>0]
    losses = [t for t in closed if t.get("pnl",0)<=0]
    win_pnl = sum(t["pnl"] for t in wins)
    loss_pnl = sum(t["pnl"] for t in losses)
    return {
        "total":  len(closed), "wins": len(wins), "losses": len(losses),
        "win_rate": round(len(wins)/len(closed)*100,2),
        "avg_win":  round(win_pnl/len(wins),2) if wins else 0,
        "avg_loss": round(loss_pnl/len(losses),2) if losses else 0,
        "profit_factor": round(abs(win_pnl/loss_pnl),2) if loss_pnl else 999,
        "total_pnl": round(win_pnl+loss_pnl,2),
    }

# ── SIGNALS ───────────────────────────────────────────────────────────────────
@app.get("/api/signals/{symbol}")
async def get_signal(symbol: str, timeframe: str = "M15", user=Depends(get_current_user)):
    if user["plan"]=="free" and symbol not in ("EURUSD",""):
        raise HTTPException(403, "Free plan: EUR/USD only. Upgrade to Pro.")
    import random
    price = FOREX_PAIRS.get(symbol, 1.0)
    rsi  = random.uniform(25, 75)
    macd = random.uniform(-0.001, 0.001)
    atr  = random.uniform(0.0005, 0.0015)
    buy_score  = random.randint(2, 6)
    sell_score = 6 - buy_score
    signal = "buy" if buy_score > sell_score else "sell" if sell_score > buy_score else "neutral"
    sl = price - atr*1.5 if signal=="buy" else price + atr*1.5
    tp = price + atr*2.5 if signal=="buy" else price - atr*2.5
    return {
        "symbol": symbol, "timeframe": timeframe,
        "signal": signal, "price": round(price,5),
        "rsi": round(rsi,1), "macd": round(macd,5), "atr": round(atr,5),
        "buy_score": buy_score, "sell_score": sell_score,
        "sl": round(sl,5), "tp": round(tp,5),
        "rr_ratio": round(abs(tp-price)/abs(sl-price),2),
        "reasons": {
            "trend":  buy_score>3, "ema_align": buy_score>2,
            "rsi_ok": rsi<40 if signal=="buy" else rsi>60,
            "macd_ok": macd>0 if signal=="buy" else macd<0,
            "ob_near": random.choice([True,False]),
            "mtf_ok": buy_score>4,
        },
        "ts": datetime.utcnow().isoformat(),
    }

@app.get("/api/signals/screener/top")
async def screener(user=Depends(get_current_user)):
    if user["plan"] == "free":
        raise HTTPException(403, "Screener requires Pro plan.")
    results = []
    for sym, price in FOREX_PAIRS.items():
        import random
        score = random.randint(3,6)
        results.append({"symbol":sym,"price":price,"score":score,"signal":"buy" if score>3 else "sell"})
    return sorted(results, key=lambda x: x["score"], reverse=True)

# ── PLANS & BILLING ───────────────────────────────────────────────────────────
@app.get("/api/plans")
async def get_plans():
    return [
        {"id":"free","name":"Free","price":0,"currency":"USD","interval":"month",
         "features":["1 Forex pair","Delayed signals 15min","Basic backtest","1 MT5 demo"]},
        {"id":"pro","name":"Pro","price":29,"currency":"USD","interval":"month",
         "features":["10 Forex pairs","Real-time signals","Auto trading bot","3 MT5 live accounts","Telegram alerts","Full journal + CSV export"]},
        {"id":"elite","name":"Elite","price":79,"currency":"USD","interval":"month",
         "features":["All Forex pairs","AI/ML signal filter","Copy trading","Unlimited MT5","Screener","News sentiment","Priority support 24/7"]},
        {"id":"lifetime","name":"Lifetime","price":499,"currency":"USD","interval":"once",
         "features":["Everything in Elite","Forever","Beta features","API access","1 onboarding session"]},
    ]

@app.post("/api/plans/upgrade")
async def upgrade_plan(body: dict, user=Depends(get_current_user)):
    plan = body.get("plan")
    if plan not in ("free","pro","elite","lifetime"):
        raise HTTPException(400, "Invalid plan")
    # In production: verify Stripe payment here before upgrading
    user["plan"] = plan
    logger.info(f"Plan upgraded to {plan}: {user['email']}")
    return {"status":"upgraded","plan":plan,"message":"In production: Stripe payment required."}

# ── BROKER ────────────────────────────────────────────────────────────────────
@app.post("/api/broker/connect")
async def connect_broker(req: BrokerConnectReq, user=Depends(get_current_user)):
    # In production: connect to MT5 here via Python MT5 library
    user["broker_connected"] = True
    user["mt5_login"] = req.mt5_login
    user["mt5_server"] = req.mt5_server
    logger.info(f"Broker connected: {req.mt5_server} #{req.mt5_login} [{user['email']}]")
    return {"status":"connected","server":req.mt5_server,"login":req.mt5_login}

@app.get("/api/broker/status")
async def broker_status(user=Depends(get_current_user)):
    return {
        "connected": user.get("broker_connected", False),
        "server": user.get("mt5_server",""),
        "login": user.get("mt5_login",""),
        "balance": user.get("balance", 0),
    }

# ── PRICES ────────────────────────────────────────────────────────────────────
@app.get("/api/prices")
async def get_prices(user=Depends(get_current_user)):
    pairs = list(FOREX_PAIRS.keys()) if user["plan"]!="free" else ["EURUSD"]
    return {sym: {"bid":round(FOREX_PAIRS[sym]-0.0001,5),"ask":round(FOREX_PAIRS[sym]+0.0001,5),"mid":FOREX_PAIRS[sym]} for sym in pairs}

# ── WEBSOCKET ─────────────────────────────────────────────────────────────────
@app.websocket("/ws/{user_id}")
async def websocket_endpoint(ws: WebSocket, user_id: str):
    await ws_manager.connect(ws, user_id)
    try:
        while True:
            data = await ws.receive_text()
            msg = json.loads(data)
            if msg.get("type") == "ping":
                await ws.send_json({"type":"pong","ts":datetime.utcnow().isoformat()})
    except WebSocketDisconnect:
        ws_manager.disconnect(user_id)

# ── HEALTH ────────────────────────────────────────────────────────────────────
@app.get("/")
async def root():
    return {"status":"operational","service":"SmartTraderBot Pro API","version":"2.0.0","ts":datetime.utcnow().isoformat()}

@app.get("/health")
async def health():
    return {"status":"ok","users":len(users_db),"trades":len(trades_db),"ws_connections":len(ws_manager.connections)}

# ── Background price feed ─────────────────────────────────────────────────────
async def price_broadcast_loop():
    while True:
        await asyncio.sleep(1)
        updates = {}
        for sym in FOREX_PAIRS:
            FOREX_PAIRS[sym] = round(FOREX_PAIRS[sym] + random.uniform(-0.00015, 0.00015), 5)
            updates[sym] = {"bid":round(FOREX_PAIRS[sym]-0.0001,5),"ask":round(FOREX_PAIRS[sym]+0.0001,5),"ts":datetime.utcnow().isoformat()}
        if ws_manager.connections:
            await ws_manager.broadcast({"type":"prices","data":updates})

@app.on_event("startup")
async def startup():
    asyncio.create_task(price_broadcast_loop())
    logger.info("✅ SmartTraderBot Pro API — Ready")
