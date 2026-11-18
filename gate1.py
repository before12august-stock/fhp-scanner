# FHP 15.9.5 - GATE 1 (ด่านแรก) v1.1 - ONE FILE. Upload as gate1.py in repo root.
# Job: scan seed list through Stage 1.5 (Gate 1) ONLY - no Pine engine, no Rank/Score.
# Output: docs/universe.csv (passing tickers -> feeds main scan), docs/gate1.csv (full report), docs/gate1.html (report page).
# Seed list = FALLBACK 193 + docs/my_tickers.txt (user list, optional) + existing docs/universe.csv (if uploaded).
# Gate 1 rules (SAME as main scanner Stage 1.5, copied from run_stock.py v4.4):
#   Shariah B FULL blacklist -> REJECT | Price 1-80 baht -> REJECT outside | Turnover SMA20(close*vol)[1]: <8M REJECT, 8-<20M STANDARD, >=20M HIGH
#   Trade >=80% (vol>0 days in last 60) -> REJECT | Market Cap >=3000M | FreeFloat <15% = FF-LOW FLAG (PASS, not REJECT - matches TradingView) | Equity >0 (if SET active)
#   UNKNOWN (download/mcap/turnover/FF/equity unknown) NEVER counts as PASS - listed separately.
#   PARITY tickers (MAJOR/TU/CRC) are ALWAYS kept in universe.csv even if Gate 1 rejects them,
#   so the main scan can still parity-check them. They are flagged PARITY-KEEP.
# Run: GitHub Actions -> "Gate 1 Scan" -> Run workflow (after 17:30 Thai). Then main scan uses the new universe.csv.

import yfinance as yf, pandas as pd, numpy as np, os, re, json, datetime, time, urllib.request
from concurrent.futures import ThreadPoolExecutor

PARITY = ["MAJOR","TU","CRC"]

SHARIAH_BLACKLIST = {"PTT","PTTEP","TOP","PTTGC","GC","IRPC","SPRC","BCP","BSRC","ESSO","SUSCO","PTG","OR","CENTEL","ERW","MINT"}

SHARIAH_B_FULL = SHARIAH_BLACKLIST | {"BBL","KBANK","SCB","KTB","TTB","BAY","KKP","TISCO","TCAP","CIMBT","LHFG","MTC","SAWAD","TIDLOR","KTC","AEONTS","THANI","ASK","GL","JMT","CHAYO","KGI","GULF","EGCO","RATCH","GPSC","EA","BGRIM","CKP","BANPU","SCC","SCCC","IVL","LH","AP","SPALI","QH","PSH","SIRI","ANAN","ORI","LPN","SC","AMATA","WHA","ROJNA","CPN","HMPRO","MBK","CENTRAL","SF","WHAUP","TPIPP","BCPG","SUPER","SSP","ACE","TTW","BPP"}

FALLBACK_UNIVERSE = ["ADVANC","AOT","BBL","BDMS","BEM","BGRIM","BH","CBG","CENTEL","CK","CPALL","CPN","CRC","DELTA","ERW","GULF","HMPRO","IVL","KBANK","KTB","KTC","LH","MINT","MTC","OSP","PTT","PTTEP","PTTGC","SCB","SCC","TOP","TRUE","TU","MAJOR","THG","XO","CHG","SCGD","KCE","HANA","COM7","GLOBAL","DOHOME","TIDLOR","SAWAD","TISCO","AMATA","WHA","AP","SPALI","QH","SIRI","BCP","BCPG","EGCO","RATCH","GPSC","EA","BANPU","CPAXT","BJC","CPF","CPN","M","MEGA","ONEE","PLANB","VGI","JMART","JMT","SINGER","STA","STGT","SAPPE","ICHI","OSP","CBG","BEM","CK","STEC","UNIQ","ITD","NWR","SEAFCO","PYLON","DCC","DCON","SUSCO","PTG","OR","IRPC","SPRC","ESSO","BSRC","GC","VNT","GGC","TPA","AJ","LHFG","KKP","TCAP","CIMBT","BAY","TTB","AEONTS","THANI","ASK","GL","CHAYO","KGI","CKP","TPIPP","SUPER","SSP","ACE","TTW","BPP","ANAN","ORI","LPN","SC","ROJNA","MBK","CENTRAL","SF","WHAUP","SCCC","M","AU","KEX","SABUY","YGG","DPAINT","PR9","BCH","CHG","THG","EKH","BPP","TNP","TM","BIZ","WINMED","KLINIQ","MASTER","BEAUTY","KAMART","IP","TMI","TMI","SNC","SMT","CCET","SVI","TEAM","HFT","TRU","SAT","STANLY","AH","CNS","FTE","Q-CON","QTC","SKE","ATP30","NCL","KWM","FLOYD","J","JCK","JCKH","GPI","TMILL","TSTE","TSTH","TMT","BSBM","CSP","PERM","MILL","PAP","SAM","AMC","LHK","CEN","CTW","NL","PAF","KUN","K","KTMS","D","DMT","S","SR","SRICHA","SYNEX","SPVI","SIS","MSC","IT","ADVANC"]

def _clean_ticker(s):
    s=(s or "").replace(".BK","").strip().upper()
    if not s or "-" in s or "." in s or " " in s: return None
    if not re.match(r"^[A-Z0-9&]{1,10}$", s): return None  # allow & for S&J (real SET ticker)
    return s

def _drop_partial_today(df):
    # v4.0: Parity FAIL on 2026-11-16 was NOT an engine bug - the run was at 12:29 Thai, mid-session.
    # Yahoo today bar then holds only the morning session (partial candle), so EMA/MACD/Stoch/
    # DayWeighted computed on it do NOT match TradingView Friday-close anchors (MAJOR 72.8 etc).
    # Rule: if Bangkok time is before 17:30 (market not closed) and the last bar IS today, drop it
    # and compute on the last COMPLETED bar (Friday). After close / weekend, keep everything.
    try:
        import datetime as _dt
        bkk=_dt.datetime.now(_dt.timezone(_dt.timedelta(hours=7)))
        if (bkk.hour<17 or (bkk.hour==17 and bkk.minute<30)) and len(df)>261:
            last=pd.to_datetime(df.index[-1]).date()
            if last==bkk.date():
                return df.iloc[:-1]
    except Exception: pass
    return df


def download_with_retry(sym):
    # v3.4: retry 3x with backoff. Returns (df_or_None, reason). reason: None / "download_failed" / "empty"
    last_err=""
    for attempt in (1,2,3):
        try:
            df=yf.download(sym+".BK", period="2y", interval="1d", auto_adjust=False, progress=False)
            if df is None or len(df)==0:
                last_err="empty"; time.sleep(1*attempt); continue
            if isinstance(df.columns, pd.MultiIndex): df.columns=df.columns.get_level_values(0)
            df=df.dropna(subset=["Close"])
            if len(df)==0:
                last_err="empty"; time.sleep(1*attempt); continue
            return df, None
        except Exception as e:
            last_err=str(e)[:100]; time.sleep(1*attempt)
    return None, last_err


def get_fundamentals(symbols):
    # v3: marketCap via fast_info, threaded. Missing -> None (UNKNOWN, never PASS)
    def one(sym):
        try:
            fi=yf.Ticker(sym+".BK").fast_info
            mc=fi.get("marketCap") if hasattr(fi,"get") else None
            if mc is None:
                try: mc=fi.get("market_cap")
                except Exception: mc=None
            return sym, (float(mc) if mc else None)
        except Exception:
            return sym, None
    res={}
    try:
        with ThreadPoolExecutor(max_workers=10) as ex:  # v3.4: 20->10 to reduce Yahoo rate-limit
            for sym, mc in ex.map(one, symbols):
                res[sym]=mc
    except Exception:
        pass
    return res



def get_set_fundamentals(symbols):
    # v4.2 COMPLETE: engine thresholds restored to user Pine file (10M/5M). One complete file (223-line file was engine-only, missing pipeline).
# v4.1: FIX FATAL v4.0 banner crash (Date.max on str+NaN mix) + Jina _raw re-parse.
# v4.0: Use last COMPLETED bar only (mid-day runs drop today partial candle - fixes false Parity FAIL), Universe + Jina/corsproxy, banner shows data-through date.
# v3.9: Universe via public proxies (SET blocks GitHub IPs entirely — headers/impersonation cannot fix it), + CACHED universe.csv (success saves it, failure reuses it, 193 FALLBACK only if no cache exists).
# v3.8: Universe via curl_cffi chrome impersonation (fixes SET 403 + Yahoo 401 seen in v3.7 banner), SET mirrors, yfinance crumb helper, settfex type logged.
# v3.7: Universe — settfex/SET-API shape-agnostic parsing (DataFrame/dict/nested all handled), Yahoo screener with crumb+cookie (fixes 401/403), FAILS shown on page banner (no Actions log hunt).
# v3.6: UNKNOWN never DOWNGRADE, never writes to history (prevents fake UPGRADE next day), Date nan -> "-" via fmt.
# v3.5: Universe 4 sources (SET API direct + settfex + Yahoo query1/query2), no settfex dependency, FALLBACK warning in banner, stage_counts fixed (download_failed/history_short were missing in v3.4 file).
# v3.4: Universe SET-first + Yahoo merged (fix 193 bug) + download retry 3x + UNKNOWN split (download_failed/history_short) + fmt nan->'-' + Trend label Low52x1.25/High52x0.75 + common-stock-only filter (-R/-W dropped).
# v3.3: Equity parsing robust (EN+TH + Assets-Liabilities fallback) + FF/EQ activate independently.
# v3.2: SET API via settfex. Returns {sym: {"free_float": float|None, "equity": float|None}}
    # free_float = percent_free_float from shareholder data (e.g. 45.2 means 45.2%)
    # equity = latest Total Shareholders Equity from balance sheet (in THB). If account not found -> None.
    out = {}
    try:
        import asyncio
        from settfex.services.set import get_shareholder_data, get_balance_sheet
    except Exception as e:
        print("SET API: settfex not installed:", str(e)[:100])
        return out
    async def one(sym):
        ff=None; eq=None
        try:
            d=await get_shareholder_data(sym)
            # settfex: data.free_float.percent_free_float
            try: ff=float(d.free_float.percent_free_float)
            except Exception:
                try: ff=float(getattr(d,"free_float",{}).get("percent_free_float"))
                except Exception: ff=None
        except Exception: ff=None
        try:
            bs=await get_balance_sheet(sym)
            if bs:
                latest=bs[0]
                accts=getattr(latest,"accounts",[]) or []
                def _amt(acc):
                    for _k in ("amount","value","amount_thb","amountThb","balance"):
                        try:
                            v=getattr(acc,_k,None)
                            if v is not None: return float(v)
                        except Exception: pass
                        try:
                            if isinstance(acc, dict) and acc.get(_k) is not None: return float(acc.get(_k))
                        except Exception: pass
                    return None
                def _nm(acc):
                    if isinstance(acc, dict): return str(acc.get("name") or acc.get("account_name") or acc.get("account") or "").lower()
                    return str(getattr(acc,"name","") or getattr(acc,"account_name","") or getattr(acc,"account","") or "").lower()
                # 1) exact total equity (EN + TH)
                for acc in accts:
                    nm=_nm(acc)
                    if nm.strip() in ("total shareholders equity","total equity","total shareholders' equity","total shareholders equity ","รวมส่วนของผู้ถือหุ้น","รวมส่วนของเจ้าของ","ส่วนของผู้ถือหุ้น"):
                        eq=_amt(acc); break
                # 2) any account containing equity / ส่วนของผู้ถือหุ้น + total/รวม
                if eq is None:
                    for acc in accts:
                        nm=_nm(acc)
                        if ("equity" in nm or "ส่วนของผู้ถือหุ้น" in nm or "ส่วนของเจ้าของ" in nm) and ("total" in nm or "รวม" in nm):
                            eq=_amt(acc); break
                # 3) any account containing equity (last resort, pick first)
                if eq is None:
                    for acc in accts:
                        nm=_nm(acc)
                        if "equity" in nm or "ส่วนของผู้ถือหุ้น" in nm:
                            eq=_amt(acc); break
                # 4) fallback: Equity = Total Assets - Total Liabilities
                if eq is None:
                    _ta=None; _tl=None
                    for acc in accts:
                        nm=_nm(acc)
                        if "total assets" in nm or nm.strip()=="รวมสินทรัพย์": _ta=_amt(acc)
                        if "total liabilities" in nm or nm.strip()=="รวมหนี้สิน": _tl=_amt(acc)
                    if _ta is not None and _tl is not None: eq=float(_ta-_tl)
        except Exception: eq=None
        return sym, {"free_float": ff, "equity": eq}
    async def batch(symbols_batch):
        # limit concurrency: run in chunks of 25
        res=[]
        for i in range(0, len(symbols_batch), 25):
            chunk=symbols_batch[i:i+25]
            res.extend(await asyncio.gather(*[one(s) for s in chunk], return_exceptions=True))
        return res
    try:
        import asyncio as _aio
        # run in fresh loop (GitHub Actions has no running loop)
        try: loop=_aio.new_event_loop(); _aio.set_event_loop(loop)
        except Exception: loop=None
        if loop is not None:
            results=loop.run_until_complete(batch(list(symbols)))
        else:
            results=_aio.run(batch(list(symbols)))
        for r in results:
            if isinstance(r, tuple) and len(r)==2:
                out[r[0]]=r[1]
    except Exception as e:
        print("SET API batch failed:", str(e)[:150])
    ok=sum(1 for v in out.values() if v.get("free_float") is not None or v.get("equity") is not None)
    print(f"SET API: got data for {ok}/{len(symbols)} tickers (free_float or equity)")
    return out

def _load_seed():
    seeds = list(FALLBACK_UNIVERSE)
    # (0) universe.py - master list as a script (replaces universe.csv input)
    try:
        from universe import UNIVERSE as _U
        got = [_clean_ticker(x) for x in _U]
        got = [g for g in got if g]
        print(f"Seed: +{len(got)} from universe.py")
        seeds += got
    except Exception as e:
        print(f"Seed: skip universe.py: {str(e)[:80]}")
    # (1) user list: one ticker per line, or comma-separated. Edit on GitHub mobile, commit, re-run Gate 1.
    for _p in ("docs/my_tickers.txt", "my_tickers.txt"):
        try:
            if os.path.exists(_p):
                raw = open(_p, encoding="utf-8").read().replace(",", "\n")
                got = [_clean_ticker(x) for x in raw.splitlines()]
                got = [g for g in got if g]
                print(f"Seed: +{len(got)} from {_p}")
                seeds += got
        except Exception as e:
            print(f"Seed: skip {_p}: {str(e)[:80]}")
    # (2) existing universe.csv (user-uploaded full list) - use as seed too
    for _p in ("docs/universe.csv", "universe.csv"):
        try:
            if os.path.exists(_p):
                import csv as _csv
                with open(_p, encoding="utf-8") as _f:
                    rows = list(_csv.DictReader(_f))
                got = [_clean_ticker(r.get("symbol", "")) for r in rows]
                got = [g for g in got if g]
                print(f"Seed: +{len(got)} from {_p}")
                seeds += got
        except Exception as e:
            print(f"Seed: skip {_p}: {str(e)[:80]}")
    seeds = [s for s in dict.fromkeys(seeds) if s]
    return seeds

def main():
    seeds = _load_seed()
    print(f"GATE 1: scanning {len(seeds)} seed tickers")
    fund = get_fundamentals(seeds)
    setfund = get_set_fundamentals(seeds)
    ok_ff = sum(1 for v in setfund.values() if v.get("free_float") is not None) if setfund else 0
    ok_eq = sum(1 for v in setfund.values() if v.get("equity") is not None) if setfund else 0
    active_ff = ok_ff >= max(1, len(seeds) * 0.5)
    active_eq = ok_eq >= max(1, len(seeds) * 0.5)
    print(f"SET API: FreeFloat active={active_ff} ({ok_ff}/{len(seeds)}), Equity active={active_eq} ({ok_eq}/{len(seeds)})")
    rows, passing = [], []
    counts = {"pass": 0, "shariah": 0, "price": 0, "turnover": 0, "trade": 0, "mcap": 0,
              "freefloat": 0, "equity": 0, "unknown": 0, "parity_keep": 0}
    for t in seeds:
        rec = {"Ticker": t, "Result": "", "Reason": "", "Price": None, "TurnoverM": None,
               "TurnoverLevel": "", "TradePct": None, "MarketCapM": None, "FreeFloat": None, "Equity": None, "FFLow": False}
        if t in SHARIAH_B_FULL:
            rec["Result"] = "REJECT"; rec["Reason"] = "Shariah B"; counts["shariah"] += 1; rows.append(rec); continue
        df, reason = download_with_retry(t)
        if df is None:
            rec["Result"] = "UNKNOWN"; rec["Reason"] = "download_failed"; counts["unknown"] += 1; rows.append(rec); continue
        try:
            df = _drop_partial_today(df)
        except Exception:
            pass
        close, vol = df["Close"], df["Volume"]
        price = float(close.iloc[-1]); rec["Price"] = round(price, 2)
        if price < 1 or price > 80:
            rec["Result"] = "REJECT"; rec["Reason"] = f"price {price:.2f}"; counts["price"] += 1; rows.append(rec); continue
        to = float((close * vol).rolling(20).mean().shift(1).iloc[-1] / 1e6) if len(df) >= 21 else float("nan")
        if pd.isna(to):
            rec["Result"] = "UNKNOWN"; rec["Reason"] = "turnover unknown"; counts["unknown"] += 1; rows.append(rec); continue
        rec["TurnoverM"] = round(to, 1)
        rec["TurnoverLevel"] = "HIGH LIQUIDITY" if to >= 20 else "STANDARD"
        if to < 8:
            rec["Result"] = "REJECT"; rec["Reason"] = f"turnover {to:.1f}M"; counts["turnover"] += 1; rows.append(rec); continue
        trade = float((df.tail(60)["Volume"] > 0).mean() * 100)
        rec["TradePct"] = round(trade, 1)
        if trade < 80:
            rec["Result"] = "REJECT"; rec["Reason"] = f"trade {trade:.0f}%"; counts["trade"] += 1; rows.append(rec); continue
        mc = fund.get(t)
        if mc is None:
            rec["Result"] = "UNKNOWN"; rec["Reason"] = "mcap unknown"; counts["unknown"] += 1; rows.append(rec); continue
        rec["MarketCapM"] = round(mc / 1e6, 0)
        if mc < 3000e6:
            rec["Result"] = "REJECT"; rec["Reason"] = f"mcap {mc/1e6:.0f}M"; counts["mcap"] += 1; rows.append(rec); continue
        sf = setfund.get(t, {}) if isinstance(setfund, dict) else {}
        ff, eq = sf.get("free_float"), sf.get("equity")
        rec["FreeFloat"] = round(float(ff), 1) if ff is not None else None
        rec["Equity"] = round(float(eq), 0) if eq is not None else None
        if active_ff:
            if ff is None:
                rec["Result"] = "UNKNOWN"; rec["Reason"] = "freefloat unknown"; counts["unknown"] += 1; rows.append(rec); continue
            if ff < 15.0:
                rec["FFLow"] = True; counts["freefloat"] += 1  # v1.1: FF<15% = FLAG, not REJECT - still PASS, still in universe.csv
        if active_eq:
            if eq is None:
                rec["Result"] = "UNKNOWN"; rec["Reason"] = "equity unknown"; counts["unknown"] += 1; rows.append(rec); continue
            if not (eq > 0):
                rec["Result"] = "REJECT"; rec["Reason"] = "equity <= 0"; counts["equity"] += 1; rows.append(rec); continue
        rec["Result"] = "PASS"; rec["Reason"] = rec["TurnoverLevel"] + (" + FF-LOW" if rec.get("FFLow") else ""); counts["pass"] += 1
        passing.append(t); rows.append(rec)
    # Parity tickers always kept in universe.csv (so main scan can parity-check them)
    universe = sorted(set(passing) | set(PARITY))
    for t in PARITY:
        if t not in passing:
            counts["parity_keep"] += 1
    os.makedirs("docs", exist_ok=True)
    with open("docs/universe.csv", "w", encoding="utf-8") as f:
        f.write("symbol\n" + "\n".join(universe) + "\n")
    pd.DataFrame(rows).to_csv("docs/gate1.csv", index=False)
    now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M")
    tr = "".join(f"<tr><td>{r['Ticker']}</td><td>{r['Result']}</td><td>{r['Reason']}</td><td>{r['Price'] if r['Price'] is not None else '-'}</td><td>{r['TurnoverM'] if r['TurnoverM'] is not None else '-'}</td><td>{r['TradePct'] if r['TradePct'] is not None else '-'}</td><td>{r['MarketCapM'] if r['MarketCapM'] is not None else '-'}</td><td>{(str(r['FreeFloat']) + ' ⚠️ FF-LOW') if r.get('FFLow') else (r['FreeFloat'] if r['FreeFloat'] is not None else '-')}</td><td>{r['Equity'] if r['Equity'] is not None else '-'}</td></tr>" for r in rows)
    html = f"""<!doctype html><meta charset=utf-8><meta name=viewport content='width=device-width,initial-scale=1'>
<title>FHP Gate 1 - ด่านแรก</title><body style='font-family:sans-serif;background:#000;color:#fff'>
<h2>FHP Gate 1 - ด่านแรก (Stage 1.5)</h2><p>อัปเดตล่าสุด: {now} - Yahoo Finance + SET API</p>
<p style='background:#22c55e;color:#000;padding:10px'>สแกน {len(seeds)} ตัว - PASS {counts['pass']} ตัว - อยู่ใน universe.csv {len(universe)} ตัว (รวม Parity MAJOR/TU/CRC)</p>
<p>ตัดออก: Shariah {counts['shariah']} / ราคา {counts['price']} / Turnover {counts['turnover']} / เทรด {counts['trade']} / Mcap {counts['mcap']} / Equity {counts['equity']} / UNKNOWN {counts['unknown']} (UNKNOWN ไม่นับเป็น PASS)</p><p>⚠️ ติดป้าย FF-LOW: {counts['freefloat']} ตัว (FF &lt;15% — ยังคำนวณและยังอยู่ใน universe.csv เหมือน TradingView)</p>
<table border=1 cellpadding=6 style='border-collapse:collapse;background:#fff;color:#000'><tr><th>หุ้น</th><th>ผล</th><th>เหตุผล</th><th>ราคา</th><th>Turnover(M)</th><th>เทรด%</th><th>Mcap(M)</th><th>FF%</th><th>Equity</th></tr>{tr}</table>
<p><a href=universe.csv>โหลด universe.csv</a> | <a href=gate1.csv>โหลด gate1.csv</a></p>
<p>ไฟล์นี้คือด่านแรกเท่านั้น - สแกน FHP เต็ม (Rank/Score) อยู่ที่หน้า index.html</p></body>"""
    open("docs/gate1.html", "w", encoding="utf-8").write(html)
    print(f"GATE 1 DONE: seed {len(seeds)} -> PASS {counts['pass']} -> universe.csv {len(universe)} tickers (parity-keep {counts['parity_keep']})")
    print(f"Counts: {counts}")
    print("WROTE docs/universe.csv + docs/gate1.csv + docs/gate1.html")

if __name__ == "__main__":
    main()
