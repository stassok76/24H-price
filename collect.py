#!/usr/bin/env python3
"""
Gate BTC 1h 事件合约盘口采集器（只记录，不下单）

用法：
  python collect.py discover   # 探测：打开事件合约页面，抓下所有接口/WebSocket 数据，找价格从哪来
  python collect.py collect    # 采集：等到整点，记录开盘 5 秒和 60 秒两次盘口价，回填已结算周期
  python collect.py summary    # 统计：逆向 ask 均值、两边之和、延迟价差、累计盈亏

环境变量（在 GitHub 仓库 Settings → Secrets and variables → Actions → Variables 里设）：
  EVENT_PAGE_URL     Gate BTC 1h 事件合约（TWAP 市场）页面地址，从浏览器地址栏复制
  PRICE_URL_KEYWORD  探测后找到的价格接口 URL 关键字（留空则记录所有 JSON 响应，只建议前几次这样）
"""
import csv
import datetime as dt
import json
import os
import sys
import time
from pathlib import Path

import requests

ROOT = Path(__file__).resolve().parent
DATA = ROOT / "data"
CSV_PATH = DATA / "gate-1h-prices.csv"
RAW_DIR = DATA / "raw"
DISC_DIR = DATA / "discovery"

EVENT_PAGE_URL = os.environ.get("EVENT_PAGE_URL", "").strip()
PRICE_URL_KEYWORD = os.environ.get("PRICE_URL_KEYWORD", "").strip()

UTC = dt.timezone.utc
TPE = dt.timezone(dt.timedelta(hours=8))
SPOT_API = "https://api.gateio.ws/api/v4/spot/candlesticks"

STAKE = 5.0        # 每注虚拟 5U
FEE_RATE = 0.001   # 开仓手续费按 0.10% 假设（和回测一致）

FIELDS = [
    "period_start_utc", "period_start_tpe", "late",
    "t1_at", "t1_delay_s", "ask_up_t1", "ask_down_t1", "sum_t1",
    "t2_at", "t2_delay_s", "ask_up_t2", "ask_down_t2", "sum_t2",
    "prev_fast_dir", "bet_dir", "bet_ask",
    "base_open", "settle_close", "outcome", "result", "pnl", "cum_pnl",
    "status", "note",
]


# ---------------------------------------------------------------- K 线

def get_candles(limit=8):
    """Gate 现货 1h K 线。注意字段顺序：[ts, 成交额, 收盘, 最高, 最低, 开盘, 成交量, 是否确认]"""
    r = requests.get(SPOT_API, params={"currency_pair": "BTC_USDT", "interval": "1h", "limit": limit}, timeout=20)
    r.raise_for_status()
    out = []
    for c in r.json():
        out.append({
            "ts": int(c[0]),
            "close": float(c[2]),
            "open": float(c[5]),
            "confirmed": str(c[7]).lower() == "true",
        })
    out.sort(key=lambda x: x["ts"])
    return out


def contrarian_bet(candles, period_start_ts):
    """和回测同一口径：快线 = close[i] - close[i-1]，同向/异向最终都跟快线；逆向 = 反着押。
    i 是 period_start 前最后一根已收盘 K 线。"""
    closed = [c for c in candles if c["ts"] < period_start_ts and c["confirmed"]]
    if len(closed) < 2:
        return "", ""
    fast = closed[-1]["close"] - closed[-2]["close"]
    fast_dir = "up" if fast >= 0 else "down"
    return fast_dir, ("down" if fast_dir == "up" else "up")


# ---------------------------------------------------------------- 时间

def pick_target(now):
    """返回 (要采集的周期开始时间, 是否迟到)。
    cron 设在每小时 :45 触发；正常情况下等到下一个整点。
    如果触发严重延迟、已经过了整点，就采当前周期并标记 late。"""
    hour = now.replace(minute=0, second=0, microsecond=0)
    if now - hour <= dt.timedelta(seconds=60):
        return hour, False
    nxt = hour + dt.timedelta(hours=1)
    if nxt - now <= dt.timedelta(minutes=30):
        return nxt, False
    return hour, True


def sleep_until(t):
    while True:
        left = (t - dt.datetime.now(UTC)).total_seconds()
        if left <= 0:
            return
        time.sleep(min(left, 30))


# ---------------------------------------------------------------- 浏览器抓取

def capture_page(url, keyword="", wait_ms=12000, screenshot=None, max_hits=40, body_cap=8000):
    """打开页面，收集 XHR/fetch 的 JSON 响应和 WebSocket 收到的帧。"""
    from playwright.sync_api import sync_playwright

    responses, ws_frames = [], []
    info = {"page_status": None, "title": "", "text_head": ""}

    with sync_playwright() as p:
        browser = p.chromium.launch()
        ctx = browser.new_context(
            locale="zh-CN",
            user_agent=("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                        "(KHTML, like Gecko) Chrome/130.0 Safari/537.36"),
            viewport={"width": 1440, "height": 900},
        )
        page = ctx.new_page()

        # 事件回调里只收对象，读 body 放到后面，避免 sync API 在回调里阻塞
        page.on("response", lambda r: responses.append(r)
                if r.request.resource_type in ("xhr", "fetch") else None)

        def on_ws(ws):
            def on_frame(payload):
                s = payload if isinstance(payload, str) else payload.decode("utf-8", "replace")
                if (not keyword or keyword in s or keyword in ws.url) and len(ws_frames) < max_hits * 5:
                    ws_frames.append({"ws_url": ws.url, "data": s[:body_cap],
                                      "at": dt.datetime.now(UTC).isoformat()})
            ws.on("framereceived", on_frame)
        page.on("websocket", on_ws)

        resp = page.goto(url, wait_until="domcontentloaded", timeout=60000)
        info["page_status"] = resp.status if resp else None
        page.wait_for_timeout(wait_ms)
        info["title"] = page.title()
        try:
            info["text_head"] = page.inner_text("body")[:3000]
        except Exception as e:  # noqa: BLE001
            info["text_head"] = f"<read body failed: {e}>"
        if screenshot:
            page.screenshot(path=str(screenshot), full_page=False)

        hits = []
        for r in responses:
            if keyword and keyword not in r.url:
                continue
            try:
                if "json" not in (r.headers.get("content-type") or ""):
                    continue
                hits.append({"url": r.url, "status": r.status, "body": r.text()[:body_cap]})
            except Exception as e:  # noqa: BLE001
                hits.append({"url": r.url, "status": r.status, "body": f"<read failed: {e}>"})
            if len(hits) >= max_hits:
                break
        browser.close()

    return info, hits, ws_frames


def parse_prices(hits, ws_frames):
    """从抓到的数据里解析 (涨 ask, 跌 ask)。
    TODO：跑完 discover、确认价格接口和字段后在这里实现。
    在实现之前，原始数据都存在 data/raw/，之后可以离线补解析。"""
    return None, None


# ---------------------------------------------------------------- CSV

def load_rows():
    if not CSV_PATH.exists():
        return []
    with CSV_PATH.open(newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def save_rows(rows):
    DATA.mkdir(parents=True, exist_ok=True)
    tmp = CSV_PATH.with_suffix(".tmp")
    with tmp.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=FIELDS)
        w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k, "") for k in FIELDS})
    tmp.replace(CSV_PATH)


def fmt(x, nd=4):
    return "" if x is None else f"{x:.{nd}f}"


def backfill(rows, candles):
    """用已收盘 K 线回填结果。结算近似：close >= open 算涨（真实结算是 Chainlink TWAP ≥ 基准价）。"""
    by_ts = {c["ts"]: c for c in candles if c["confirmed"]}
    for r in rows:
        if r.get("outcome"):
            continue
        ts = int(dt.datetime.fromisoformat(r["period_start_utc"]).timestamp())
        c = by_ts.get(ts)
        if not c:
            continue
        r["base_open"] = fmt(c["open"], 1)
        r["settle_close"] = fmt(c["close"], 1)
        r["outcome"] = "up" if c["close"] >= c["open"] else "down"
        if r.get("bet_dir"):
            r["result"] = "win" if r["bet_dir"] == r["outcome"] else "loss"
            if r.get("bet_ask"):
                ask = float(r["bet_ask"])
                fee = STAKE * FEE_RATE
                pnl = (STAKE / ask - STAKE - fee) if r["result"] == "win" else (-STAKE - fee)
                r["pnl"] = fmt(pnl, 3)
    cum = 0.0
    for r in rows:
        if r.get("pnl"):
            cum += float(r["pnl"])
            r["cum_pnl"] = fmt(cum, 3)
    return rows


# ---------------------------------------------------------------- 命令

def cmd_discover():
    if not EVENT_PAGE_URL:
        sys.exit("请先设置 EVENT_PAGE_URL")
    DISC_DIR.mkdir(parents=True, exist_ok=True)
    stamp = dt.datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    info, hits, frames = capture_page(EVENT_PAGE_URL, keyword="", wait_ms=20000,
                                      screenshot=DISC_DIR / f"{stamp}.png", max_hits=80)
    out = {"captured_at": stamp, "page": EVENT_PAGE_URL, **info,
           "json_responses": hits, "ws_frames": frames[:300]}
    path = DISC_DIR / f"{stamp}.json"
    path.write_text(json.dumps(out, ensure_ascii=False, indent=1), encoding="utf-8")

    print(f"页面状态码: {info['page_status']}  标题: {info['title']}")
    print(f"JSON 响应 {len(hits)} 个, WebSocket 帧 {len(frames)} 条, 已存 {path}")
    for h in hits:
        print(f"  [{h['status']}] {h['url'][:160]}")
    ws_urls = sorted({f['ws_url'] for f in frames})
    for u in ws_urls:
        print(f"  [ws] {u}")
    print("页面开头文字：", info["text_head"][:400].replace("\n", " | "))


def cmd_collect():
    if not EVENT_PAGE_URL:
        sys.exit("请先设置 EVENT_PAGE_URL")
    now = dt.datetime.now(UTC)
    target, late = pick_target(now)
    print(f"现在 {now:%H:%M:%S} UTC，目标周期 {target:%Y-%m-%d %H:%M} UTC，late={late}")

    row = {
        "period_start_utc": target.isoformat(),
        "period_start_tpe": target.astimezone(TPE).strftime("%Y-%m-%d %H:%M"),
        "late": "1" if late else "0",
        "status": "ok", "note": "",
    }

    try:
        candles = get_candles()
        row["prev_fast_dir"], row["bet_dir"] = contrarian_bet(candles, int(target.timestamp()))
    except Exception as e:  # noqa: BLE001
        candles = []
        row["status"], row["note"] = "error", f"candles: {e}"

    RAW_DIR.mkdir(parents=True, exist_ok=True)
    raw_path = RAW_DIR / f"{target:%Y%m%d}.jsonl"

    for tag, offset in (("t1", 5), ("t2", 60)):
        if not late:
            sleep_until(target + dt.timedelta(seconds=offset))
        at = dt.datetime.now(UTC)
        row[f"{tag}_at"] = at.isoformat(timespec="seconds")
        row[f"{tag}_delay_s"] = f"{(at - target).total_seconds():.0f}"
        try:
            info, hits, frames = capture_page(EVENT_PAGE_URL, keyword=PRICE_URL_KEYWORD, wait_ms=8000)
            up, down = parse_prices(hits, frames)
            row[f"ask_up_{tag}"], row[f"ask_down_{tag}"] = fmt(up), fmt(down)
            if up is not None and down is not None:
                row[f"sum_{tag}"] = fmt(up + down)
            with raw_path.open("a", encoding="utf-8") as f:
                f.write(json.dumps({"period": target.isoformat(), "tag": tag, "at": at.isoformat(),
                                    "page_status": info["page_status"], "hits": hits,
                                    "ws_frames": frames[:50]}, ensure_ascii=False) + "\n")
            if info["page_status"] and info["page_status"] >= 400:
                row["status"], row["note"] = "error", f"page {info['page_status']}"
        except Exception as e:  # noqa: BLE001
            row["status"] = "error"
            row["note"] = (row["note"] + f" {tag}: {e}").strip()[:300]

    if row["bet_dir"]:
        row["bet_ask"] = row.get(f"ask_{row['bet_dir']}_t1", "")

    rows = [r for r in load_rows() if r["period_start_utc"] != row["period_start_utc"]]
    rows.append(row)
    rows.sort(key=lambda r: r["period_start_utc"])
    try:
        candles = get_candles(limit=48)
        rows = backfill(rows, candles)
    except Exception as e:  # noqa: BLE001
        print(f"回填失败（下次再补）: {e}")
    save_rows(rows)
    print(json.dumps(row, ensure_ascii=False, indent=1))


def cmd_summary():
    rows = load_rows()
    def nums(key):
        return [float(r[key]) for r in rows if r.get(key)]
    bet_asks = nums("bet_ask")
    sums1, sums2 = nums("sum_t1"), nums("sum_t2")
    settled = [r for r in rows if r.get("result")]
    wins = sum(r["result"] == "win" for r in settled)
    pnl = sum(nums("pnl"))
    print(f"记录周期: {len(rows)}（late {sum(r.get('late') == '1' for r in rows)}，"
          f"error {sum(r.get('status') == 'error' for r in rows)}）")
    if bet_asks:
        print(f"逆向那边 ask: 均值 {sum(bet_asks)/len(bet_asks):.4f}  最低 {min(bet_asks):.4f}  "
              f"最高 {max(bet_asks):.4f}  （1h 要低于约 0.52 才有戏）")
    if sums1:
        print(f"两边之和 @5s: 均值 {sum(sums1)/len(sums1):.4f}")
    if sums2:
        print(f"两边之和 @60s: 均值 {sum(sums2)/len(sums2):.4f}")
    if settled:
        print(f"已结算 {len(settled)} 注，逆向胜率 {wins/len(settled):.2%}，按真实 ask 累计盈亏 {pnl:+.2f}U")


if __name__ == "__main__":
    cmds = {"discover": cmd_discover, "collect": cmd_collect, "summary": cmd_summary}
    if len(sys.argv) != 2 or sys.argv[1] not in cmds:
        sys.exit(__doc__)
    cmds[sys.argv[1]]()
