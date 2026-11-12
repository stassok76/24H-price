# gate-event-recorder

自动记录 Gate BTC 1h 事件合约（TWAP 市场）的盘口价格，只记录，不下单。
目的：确认"1h 逆向"那一边的真实买入价能不能稳定低于约 0.52。

## 使用步骤

1. 把本目录推到一个 **公开** GitHub 仓库
2. 仓库 Settings → Secrets and variables → Actions → **Variables** 新建：
   - `EVENT_PAGE_URL`：浏览器里打开 Gate BTC 1h 事件合约页面，复制地址栏
3. Actions → `discover` → Run workflow，跑完看日志和 `data/discovery/` 里的 json/截图：
   - 页面状态码 403 / 截图是拦截页 → GitHub 机器被屏蔽，换本地电脑跑
   - 能看到价格 → 找出带涨跌价格的接口或 WebSocket，填 `PRICE_URL_KEYWORD`，并实现 `collect.py` 里的 `parse_prices()`
4. 确认解析正确后，`collect` 会每小时自动跑（:45 触发，等到整点 +5s 和 +60s 各取一次价）
5. 本地 `git pull` 后 `python collect.py summary` 看统计

## 注意

- 公开仓库，不要提交账号、cookie、API key；如果价格必须登录才能拿到，就不要用这个方案
- 结算是近似：用现货 1h K 线 close ≥ open 算涨；真实结算是 Chainlink TWAP ≥ 基准价
- Gate 现货 K 线接口字段顺序是 `[ts, 成交额, 收盘, 最高, 最低, 开盘, 成交量, 是否确认]`
