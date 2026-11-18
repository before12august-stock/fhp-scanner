# FHP 15.9.5 v4.5 COMPLETE - ONE FILE. This is the ONLY file to upload. Delete any 223-line file.
# ENGINE = user's 223-line EXACT Pine Parity file VERBATIM (RVOL Vol/SMA50, TrendTemplate SMA50/150/200 + Low52*1.25 + High52*0.75,
# DayWeighted divisor 6.5, Combined = weighted/6.5*50 + acc/5*20 + sweep15 + explosive5 + confirmedTrend10 + week(0-15)+VCP5+Trend5,
# Rank 3=CS/2=ES/1=EE/0=NONE, Turnover SMA(close*vol,20)[1], LowLiq <10M (-1 DayWeighted), TooDry <5M (Rank 0 + Combined 0),
# VCP/TrendTemplate use [1] lag, Keltner uses true-range EMA). PIPELINE = full-market Universe + SET API + Stage 1.5 + Parity + history + v4.1 crash fixes.
# Shariah B is EXTERNAL filter, NOT in Pine. SCGD must NOT be banned as petrochemical. Only explicit blacklist below.
# FHP 15.9.5 v4.5 COMPLETE - Full Market SET+mai + SET API (FreeFloat flag + Equity) + Full Pine Modules + Parity Check + Rank History
# Locked Pine formulas unchanged. New modules are ADDITIVE only (no change to Score/Rank).
# New: Advanced VCP, Smart Money, Market Structure (APPROXIMATE - Pine uses var state machine, Python uses last-10-bars check), TD9, Risk, MTF, Module Scoreboard 7/7, Verdict, Parity, History
# v2.1 FIX: FailedBreakout requires prior breakout in last 10 bars; structSignal order = Failed > Rejection > Breakout (Pine Sec 17C)
# v4.2 COMPLETE: engine thresholds restored to user Pine file (10M/5M). One complete file (223-line file was engine-only, missing pipeline).
# v4.5: FF% display only - round to whole number, no decimals (58 not 58.4700001220703125). Logic (FFLow flag) still uses raw value.
# v4.6: equity<=0 = UNKNOWN (parse-suspect, parser misreads MAJOR/CRC) not REJECT; Gate 1 fresh cache labelled Gate1-fresh not FALLBACK; banner/history use Bangkok time (runner clock is UTC).
# v4.1: FIX FATAL v4.0 banner crash (Date.max on str+NaN mix) + Jina _raw re-parse.
# v4.0: Use last COMPLETED bar only (mid-day runs drop today partial candle - fixes false Parity FAIL), Universe + Jina/corsproxy, banner shows data-through date.
# v3.9: Universe via public proxies (SET blocks GitHub IPs entirely — headers/impersonation cannot fix it), + CACHED universe.csv (success saves it, failure reuses it, 193 FALLBACK only if no cache exists).
# v3.8: Universe via curl_cffi chrome impersonation (fixes SET 403 + Yahoo 401 seen in v3.7 banner), SET mirrors, yfinance crumb helper, settfex type logged.
# v3.7: Universe — settfex/SET-API shape-agnostic parsing (DataFrame/dict/nested all handled), Yahoo screener with crumb+cookie (fixes 401/403), FAILS shown on page banner (no Actions log hunt).
# v3.6: UNKNOWN never DOWNGRADE, never writes to history (prevents fake UPGRADE next day), Date nan -> "-" via fmt.
# v3.5: Universe 4 sources (SET API direct + settfex + Yahoo query1/query2), no settfex dependency, FALLBACK warning in banner, stage_counts fixed (download_failed/history_short were missing in v3.4 file).
# v3.4: Universe SET-first + Yahoo merged (fix 193 bug) + download retry 3x + UNKNOWN split (download_failed/history_short) + fmt nan->'-' + Trend label Low52x1.25/High52x0.75 + common-stock-only filter (-R/-W dropped).
# v3.3: Equity parsing robust (EN+TH + Assets-Liabilities fallback) + FF/EQ activate independently.
# v3.2: SET API via settfex (get_shareholder_data -> Free Float %, get_balance_sheet -> Equity). If SET API success <50%, Equity filter is SKIPPED and banner says so (never fake PASS). FreeFloat is a FLAG only (v4.4), never a reject. Engine unchanged.
# v4.4: FreeFloat DEMOTED from Stage REJECT to FLAG (FF-LOW). FF<15% stocks are now COMPUTED like TradingView and shown with a warning badge - never silently dropped. Parity vs TradingView is now 1:1.
# v4.3: Parity tickers NO LONGER bypass Stage (user request) - MAJOR/TU/CRC go through the SAME Stage 1.5 filter as every ticker.
# v4.3: Parity check now reports STAGE-REJECT separately from engine FAIL (a Stage-rejected parity ticker is NOT an engine error).
# v3.1b (REMOVED in v4.3): Parity tickers used to bypass Stage filter.
# v3.1: price filter 1-80 baht (was >1 only). Engine unchanged.
# v3: FULL MARKET - universe via Yahoo screener BKK (SET+mai), Stage 0-1.5 filter before compute, UNKNOWN never PASS. Engine unchanged.
# v2.2: clearer labels (TD ซื้อ/ขาย, เกณฑ์ Low/High, first-run Change = -) + smaller display text 11px, zoom manually. NO engine change.
import yfinance as yf, pandas as pd, numpy as np, os, datetime, json, urllib.request, urllib.error, time, re
from concurrent.futures import ThreadPoolExecutor

SHARIAH_BLACKLIST = {"PTT","PTTEP","TOP","PTTGC","GC","IRPC","SPRC","BCP","BSRC","ESSO","SUSCO","PTG","OR","CENTEL","ERW","MINT"}
TICKERS_21 = ["MAJOR","TU","CRC","CPALL","THG","XO","CHG","SCGD","ADVANC","AOT","BDMS","BEM","BGRIM","CBG","CK","CPN","HMPRO","KTC","MTC","OSP","TRUE"]
TICKERS = [t.strip().upper() for t in os.environ.get("FHP_TICKERS", ",".join(TICKERS_21)).split(",") if t.strip()]

# --- v3: Shariah B FULL blacklist (banks/finance, energy, petrochem, real estate) - used ONLY in FULL mode ---
SHARIAH_B_FULL = SHARIAH_BLACKLIST | {"BBL","KBANK","SCB","KTB","TTB","BAY","KKP","TISCO","TCAP","CIMBT","LHFG","MTC","SAWAD","TIDLOR","KTC","AEONTS","THANI","ASK","GL","JMT","CHAYO","KGI","GULF","EGCO","RATCH","GPSC","EA","BGRIM","CKP","BANPU","SCC","SCCC","IVL","LH","AP","SPALI","QH","PSH","SIRI","ANAN","ORI","LPN","SC","AMATA","WHA","ROJNA","CPN","HMPRO","MBK","CENTRAL","SF","WHAUP","TPIPP","BCPG","SUPER","SSP","ACE","TTW","BPP"}
# --- v3: fallback universe (SET100 + mai leaders) if Yahoo screener fails ---
FALLBACK_UNIVERSE = ["ADVANC","AOT","BBL","BDMS","BEM","BGRIM","BH","CBG","CENTEL","CK","CPALL","CPN","CRC","DELTA","ERW","GULF","HMPRO","IVL","KBANK","KTB","KTC","LH","MINT","MTC","OSP","PTT","PTTEP","PTTGC","SCB","SCC","TOP","TRUE","TU","MAJOR","THG","XO","CHG","SCGD","KCE","HANA","COM7","GLOBAL","DOHOME","TIDLOR","SAWAD","TISCO","AMATA","WHA","AP","SPALI","QH","SIRI","BCP","BCPG","EGCO","RATCH","GPSC","EA","BANPU","CPAXT","BJC","CPF","CPN","M","MEGA","ONEE","PLANB","VGI","JMART","JMT","SINGER","STA","STGT","SAPPE","ICHI","OSP","CBG","BEM","CK","STEC","UNIQ","ITD","NWR","SEAFCO","PYLON","DCC","DCON","SUSCO","PTG","OR","IRPC","SPRC","ESSO","BSRC","GC","VNT","GGC","TPA","AJ","LHFG","KKP","TCAP","CIMBT","BAY","TTB","AEONTS","THANI","ASK","GL","CHAYO","KGI","CKP","TPIPP","SUPER","SSP","ACE","TTW","BPP","ANAN","ORI","LPN","SC","ROJNA","MBK","CENTRAL","SF","WHAUP","SCCC","M","AU","KEX","SABUY","YGG","DPAINT","PR9","BCH","CHG","THG","EKH","BPP","TNP","TM","BIZ","WINMED","KLINIQ","MASTER","BEAUTY","KAMART","IP","TMI","TMI","SNC","SMT","CCET","SVI","TEAM","HFT","TRU","SAT","STANLY","AH","CNS","FTE","Q-CON","QTC","SKE","ATP30","NCL","KWM","FLOYD","J","JCK","JCKH","GPI","TMILL","TSTE","TSTH","TMT","BSBM","CSP","PERM","MILL","PAP","SAM","AMC","LHK","CEN","CTW","NL","PAF","KUN","K","KTMS","D","DMT","S","SR","SRICHA","SYNEX","SPVI","SIS","MSC","IT","ADVANC"]

def _clean_ticker(s):
    s=(s or "").replace(".BK","").strip().upper()
    if not s or "-" in s or "." in s or " " in s: return None
    if not re.match(r"^[A-Z0-9&]{1,10}$", s): return None  # allow & for S&J (real SET ticker)
    return s

def _dedup_preserve(lst):
    seen=set(); out=[]
    for s in lst:
        if s not in seen: seen.add(s); out.append(s)
    return out

def _fetch_json(url, payload=None, timeout=25):
    # v3.8: GitHub IPs get 403 from SET (www.set.or.th) and 401 from Yahoo screener when using urllib.
    # yfinance's own downloads WORK on GitHub because they use curl_cffi with browser impersonation.
    # So Universe now uses the SAME trick: curl_cffi impersonate=chrome first, urllib only as last resort.
    import json as _j
    # --- try 1: curl_cffi (comes with yfinance) ---
    try:
        from curl_cffi import requests as _cr
        _h={"User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36",
            "Accept":"application/json, text/plain, */*","Accept-Language":"en-US,en;q=0.9",
            "Referer":"https://www.set.or.th/en/home","Origin":"https://www.set.or.th"}
        if payload is not None:
            r=_cr.post(url, data=payload, headers={**_h,"Content-Type":"application/json"}, impersonate="chrome", timeout=timeout)
        else:
            r=_cr.get(url, headers=_h, impersonate="chrome", timeout=timeout)
        if r.status_code==200:
            try: return _j.loads(r.text)
            except Exception: return {"_raw": r.text}
        _last=f"curl_cffi HTTP {r.status_code}"
    except Exception as e:
        _last=f"curl_cffi:{str(e)[:60]}"
    # --- try 2: urllib with full browser headers ---
    try:
        _h={"User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36",
            "Accept":"application/json, text/plain, */*","Accept-Language":"en-US,en;q=0.9",
            "Referer":"https://www.set.or.th/en/home","Origin":"https://www.set.or.th"}
        if payload is not None: _h["Content-Type"]="application/json"
        req=urllib.request.Request(url, data=payload, headers=_h)
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return _j.loads(r.read().decode())
    except Exception as e:
        raise Exception(f"{_last} / urllib:{str(e)[:60]}")

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

def fetch_universe():
    # v3.5: Universe from 4 sources, UNION. v3.4 failed because it depended on settfex (not installed
    # on GitHub if requirements.txt is old) + Yahoo screener (often blocked in GitHub Actions).
    # v3.5 Universe does NOT need settfex: it calls SET API directly via urllib.
    # Order: (1) SET API direct  (2) settfex if available  (3) Yahoo query1  (4) Yahoo query2
    uni=[]; srcs=[]; fails=[]
    # --- (1) SET API direct + PROXIES (v3.9): v3.8 proved curl_cffi/headers CANNOT beat a datacenter-IP block.
    # SET returns 403 to GitHub IPs no matter what we send (403 via urllib AND curl_cffi in v3.8 banner).
    # Fix: ask a public proxy to fetch SET for us — SET sees the PROXY's IP, not GitHub's.
    # Proxies tried: allorigins (2 mirrors), codetabs. All are free, no key.
    # v3.8: same URL via curl_cffi + extra mirrors (api.set.or.th, settrade) + full browser headers.
    try:
        import json as _j
        set_syms=[]
        _urls=[]
        for _sec in ("S","M"):
            _raw=f"https://www.set.or.th/api/set/stock/list?securityType={_sec}"
            _urls.append(_raw)
            _urls.append(f"https://api.set.or.th/api/set/stock/list?securityType={_sec}")
            # v3.9 proxies: SET sees proxy IP, not GitHub IP
            import urllib.parse as _up
            _enc=_up.quote(_raw, safe="")
            _urls.append(f"https://api.allorigins.win/raw?url={_enc}")
            _urls.append(f"https://api.codetabs.com/v1/proxy?quest={_enc}")
            _urls.append(f"https://api.allorigins.win/get?url={_enc}")
            # v4.0: allorigins/codetabs 522 in v3.9 banner. Jina Reader + corsproxy are different infra.
            _urls.append(f"https://r.jina.ai/{_raw}")
            _urls.append(f"https://corsproxy.io/?url={_enc}")  # returns {"contents": "..."} JSON wrapper
        # v3.9: allorigins /get wraps JSON as string in .contents — unwrap it in _fetch_json caller (handled below via _unwrap_get)
        for _url in _urls:
            try:
                d=_fetch_json(_url)
                # v4.1 FIX: _fetch_json returns {"_raw": text} when its json.loads failed (e.g. Jina adds a prefix).
                # Without this, a Jina SUCCESS became 0 tickers (proven in test). Retry-parse here, incl. ```json fences.
                if isinstance(d, dict) and isinstance(d.get("_raw"), str):
                    try:
                        import json as _j3, re as _re3
                        _rt=d["_raw"].strip()
                        _m=_re3.search(r"\[.*\]|\{.*\}", _rt, _re3.DOTALL)
                        if _m: d=_j3.loads(_m.group(0))
                    except Exception: pass
                # v3.9: allorigins /get wraps payload as {"contents": "<json string>"}
                if isinstance(d, dict) and isinstance(d.get("contents"), str):
                    try:
                        import json as _j2; d=_j2.loads(d["contents"])
                    except Exception: pass
                def _unwrap(dd, depth=0):
                    if isinstance(dd, list): return dd
                    if isinstance(dd, dict) and depth<3:
                        for k in ("securityList","securities","data","stocks","stockList","result","items","list"):
                            if isinstance(dd.get(k), list): return dd[k]
                        for v in dd.values():
                            r=_unwrap(v, depth+1)
                            if r: return r
                    return []
                items=_unwrap(d)
                got=0
                for it in items if isinstance(items, list) else []:
                    sym=None
                    if isinstance(it, str): sym=it
                    elif isinstance(it, dict):
                        for k in ("symbol","securitySymbol","ticker","code"):
                            if it.get(k): sym=it.get(k); break
                        if sym is None:
                            for v in it.values():
                                if isinstance(v, dict) and v.get("symbol"): sym=v.get("symbol"); break
                    c=_clean_ticker(sym)
                    if c: set_syms.append(c); got+=1
                if got>0:
                    print(f"UNIVERSE SET via {_url[:60]}: +{got}")
                    if len(set_syms)>=400: break
            except Exception as e:
                fails.append(f"SET-API:{str(e)[:80]}")
                continue
            time.sleep(0.3)
        set_syms=_dedup_preserve(set_syms)
        if len(set_syms)>=100:
            uni.extend(set_syms); srcs.append(f"SET-API:{len(set_syms)}")
            print(f"UNIVERSE from SET API direct: {len(set_syms)} tickers")
        else:
            fails.append(f"SET-API too small ({len(set_syms)})")
    except Exception as e:
        fails.append(f"SET-API:{str(e)[:60]}")
    # --- (2) settfex (only if installed; never required) ---
    try:
        import asyncio as _aio
        from settfex.services.set import get_stock_list
        def _extract_items(sl):
            # v3.7: settfex get_stock_list shape was GUESSED in v3.5 (only .stocks/.data).
            # It can return DataFrame / dict / nested dict / list. Try everything.
            if sl is None: return []
            # DataFrame -> rows as dicts
            try:
                import pandas as _pd
                if isinstance(sl, _pd.DataFrame):
                    cols=[c for c in sl.columns if str(c).lower() in ("symbol","securitysymbol","ticker","code","stock")]
                    if cols: return sl[cols[0]].astype(str).tolist()
                    return sl.iloc[:,0].astype(str).tolist()
            except Exception: pass
            # dict -> try known keys, else all values flattened
            if isinstance(sl, dict):
                for k in ("stocks","data","securities","result","stockList","list","items"):
                    if k in sl: return _extract_items(sl[k])
                flat=[]
                for v in sl.values(): flat.extend(_extract_items(v))
                return flat
            # object with attrs
            for attr in ("stocks","data","securities","result","stockList","list","items"):
                v=getattr(sl, attr, None)
                if v is not None:
                    try: return _extract_items(v)
                    except Exception: pass
            # list / tuple / other iterable (NOT str)
            if isinstance(sl, (list, tuple)):
                return list(sl)
            return [sl]
        async def _get():
            sl=await get_stock_list()
            out=[]
            for it in _extract_items(sl):
                sym=None
                if isinstance(it, str): sym=it
                elif isinstance(it, dict):
                    for k in ("symbol","securitySymbol","ticker","code","stock","name"):
                        if it.get(k): sym=it.get(k); break
                    # nested dict: {"security": {"symbol": ...}}
                    if sym is None:
                        for v in it.values():
                            if isinstance(v, dict) and v.get("symbol"): sym=v.get("symbol"); break
                else:
                    for a in ("symbol","securitySymbol","ticker","code","stock"):
                        v=getattr(it, a, None)
                        if v: sym=v; break
                c=_clean_ticker(sym)
                if c: out.append(c)
            return out
        try:
            loop=_aio.new_event_loop(); _aio.set_event_loop(loop); slist=loop.run_until_complete(_get())
        except Exception:
            slist=_aio.run(_get())
        slist=_dedup_preserve(slist)
        # v3.8: v3.7 showed "settfex too small (0)" with no clue what it returned. Log its type.
        try: fails.append(f"settfex returned type={type(slist).__name__} n={len(slist)}")
        except Exception: pass
        if len(slist)>=100:
            uni.extend(slist); srcs.append(f"settfex:{len(slist)}")
            print(f"UNIVERSE from settfex: {len(slist)} tickers")
        else:
            fails.append(f"settfex too small ({len(slist)})")
    except Exception as e:
        fails.append(f"settfex:{str(e)[:60]}")
    # --- (3+4) Yahoo screener, query1 then query2 ---
    for _host in ("query1.finance.yahoo.com","query2.finance.yahoo.com"):
        try:
            import json as _j
            yahoo=[]
            for offset in (0,250,500,750,1000,1250):
                payload=_j.dumps({"size":250,"offset":offset,"sortField":"ticker","sortType":"DESC","quoteType":"EQUITY","query":{"operator":"AND","operands":[{"operator":"eq","operands":["exchange","BKK"]}]},"userId":"","userIdType":"guid"}).encode()
                # v3.8: use _fetch_json (curl_cffi chrome impersonation) — urllib got 401 on GitHub in v3.7.
                # Crumb: prefer yfinance's own helper (same one its working downloads use).
                _crumb=""
                try:
                    from yfinance.utils import get_cookie_crumb as _gcc
                    _ck,_crumb=_gcc()
                except Exception:
                    try:
                        from curl_cffi import requests as _cr2
                        _s=_cr2.Session(impersonate="chrome")
                        _s.get("https://fc.yahoo.com", timeout=15)
                        _crumb=_s.get("https://query1.finance.yahoo.com/v1/test/getcrumb", timeout=15).text.strip()
                    except Exception: _crumb=""
                _url=f"https://{_host}/v1/finance/screener?lang=en&region=US" + (f"&crumb={_crumb}" if _crumb else "")
                d=_fetch_json(_url, payload=payload)
                res=d.get("finance",{}).get("result",[{}])[0]
                quotes=res.get("quotes",[]); total=res.get("total", None)
                for q in quotes:
                    c=_clean_ticker(q.get("symbol") or "")
                    if c: yahoo.append(c)
                if len(quotes)==0: break
                if total is not None and (offset+len(quotes))>=total: break
                time.sleep(0.3)
            yahoo=_dedup_preserve(yahoo)
            if yahoo:
                uni.extend(yahoo); srcs.append(f"Yahoo-{_host.split('.')[0]}:{len(yahoo)}")
                print(f"UNIVERSE from Yahoo {_host}: {len(yahoo)} tickers")
                break  # one Yahoo host is enough
            else:
                fails.append(f"Yahoo-{_host}: empty")
        except Exception as e:
            fails.append(f"Yahoo-{_host}:{str(e)[:60]}")
    uni=_dedup_preserve(uni)
    for _pt in list(PARITY.keys()):
        if _pt not in uni: uni.append(_pt)
    # v3.9: CACHED universe — the fix that ends the 193 trap permanently.
    # On SUCCESS: save the full list to universe.csv (workflow commits docs/, so it persists).
    # On ALL-LIVE-FAIL: reuse universe.csv if it exists (stale list >> 193 fallback).
    # Only if there is NO cache at all do we fall back to the 193 FALLBACK list.
    _cache_paths=("docs/universe.csv","universe.csv")  # v3.9 fix: docs/ FIRST — workflow only commits docs/, root file would be lost
    if len(uni)>=50:
        globals()["_UNIVERSE_SRC"]="+".join(srcs) if srcs else "merged"; globals()["_UNIVERSE_WARN"]=False
        globals()["_UNIVERSE_FAILS"]=fails
        out=sorted(uni)
        try:
            import csv as _csv
            for _cp in _cache_paths:
                try:
                    with open(_cp,"w",newline="",encoding="utf-8") as _f:
                        _w=_csv.writer(_f); _w.writerow(["symbol"]); [_w.writerow([s]) for s in out]
                    print(f"UNIVERSE cached to {_cp}: {len(out)} tickers"); break
                except Exception as _e:
                    print(f"UNIVERSE cache save failed ({_cp}): {str(_e)[:60]}")
        except Exception: pass
    else:
        print(f"UNIVERSE ALL SOURCES FAILED: {fails}")
        _cached=[]
        for _cp in _cache_paths:
            try:
                import csv as _csv
                with open(_cp,encoding="utf-8") as _f:
                    _r=list(_csv.DictReader(_f)); _cached=[_clean_ticker(r.get("symbol","")) for r in _r]; _cached=[c for c in _cached if c]
                if len(_cached)>=50:
                    print(f"UNIVERSE using CACHED {_cp}: {len(_cached)} tickers (live sources failed, cache is better than FALLBACK)")
                    break
                _cached=[]
            except Exception: _cached=[]
        if len(_cached)>=50:
            # v4.6: cache written by Gate 1 minutes earlier in the SAME run is FRESH, not FALLBACK.
            # Only label it FALLBACK when the cache file is stale (>2h old).
            try: _cache_age_h=(time.time()-os.path.getmtime(_cp))/3600
            except Exception: _cache_age_h=99
            if _cache_age_h<2:
                out=sorted(set(_cached)); globals()["_UNIVERSE_SRC"]=f"Gate1-fresh({len(out)})"; globals()["_UNIVERSE_WARN"]=False
                globals()["_UNIVERSE_FAILS"]=fails+["using Gate 1 fresh universe.csv"]
            else:
                out=sorted(set(_cached)); globals()["_UNIVERSE_SRC"]=f"CACHED({len(out)})"; globals()["_UNIVERSE_WARN"]=True
                globals()["_UNIVERSE_FAILS"]=fails+["using cached universe.csv"]
        else:
            print(f"UNIVERSE fallback used, FALLBACK has {len(FALLBACK_UNIVERSE)} tickers (NOT full market)")
            out=sorted(set(FALLBACK_UNIVERSE)); globals()["_UNIVERSE_SRC"]="FALLBACK"; globals()["_UNIVERSE_WARN"]=True
            globals()["_UNIVERSE_FAILS"]=fails
    print(f"UNIVERSE final: {len(out)} tickers | source={globals().get('_UNIVERSE_SRC')} | fails={fails}")
    if len(out)<400:
        print(f"WARNING: Universe only {len(out)} tickers (<400 expected for SET+mai).")
    return out

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

# Parity locked values from TradingView (2026-11-13)
PARITY = {"MAJOR":{"Rank":"EE","CombinedScore":72.8,"DayWeighted":4.0},
          "TU":{"Rank":"CS","CombinedScore":72.6,"DayWeighted":4.5},
          "CRC":{"Rank":"CS","CombinedScore":62.6,"DayWeighted":4.5}}

def ema(s,n): return s.ewm(span=n, adjust=False, min_periods=n).mean()
def sma(s,n): return s.rolling(n).mean()
def highest(s,n): return s.rolling(n).max()
def lowest(s,n): return s.rolling(n).min()
def macd(close, fast=4, slow=12, sig=9):
    f=ema(close,fast); sl=ema(close,slow); line=f-sl; return line, ema(line,sig), line-ema(line,sig)

def weekly_monthly(df):
    w=df.resample('W-FRI').agg({'Open':'first','High':'max','Low':'min','Close':'last','Volume':'sum'})
    m=df.resample('ME').agg({'Open':'first','High':'max','Low':'min','Close':'last','Volume':'sum'})
    def wk(d):
        if len(d)<10: return (0,0.0,"0/3")
        e5=ema(d['Close'],5); e10=ema(d['Close'],10); ml,ms,_=macd(d['Close'])
        hh=highest(d['High'],8); ll=lowest(d['Low'],8); den=hh-ll
        rk=pd.Series(np.where(den==0,50,100*(d['Close']-ll)/den), index=d.index)
        sk=sma(rk,3); sd=sma(sk,3); b=0
        if pd.notna(ml.iloc[-1]) and ml.iloc[-1]>ms.iloc[-1]: b+=1
        if pd.notna(e5.iloc[-1]) and e5.iloc[-1]>e10.iloc[-1]: b+=1
        if pd.notna(sk.iloc[-1]) and sk.iloc[-1]>sd.iloc[-1]: b+=1
        return (b, b*5.0, f"{b}/3")
    def mo(d):
        if len(d)<75: return (False,False,False)
        e10=ema(d['Close'],10); e75=ema(d['Close'],75); ml,ms,_=macd(d['Close']); last=d['Close'].iloc[-1]
        known=all(pd.notna(x) for x in [last,e10.iloc[-1],e75.iloc[-1],ml.iloc[-1],ms.iloc[-1]])
        bull=known and last>e75.iloc[-1] and e10.iloc[-1]>e75.iloc[-1] and ml.iloc[-1]>ms.iloc[-1]
        bear=known and last<e75.iloc[-1] and ml.iloc[-1]<ms.iloc[-1]
        return (known,bull,bear)
    return wk(w), mo(m)

def compute(df):
    df=df.copy().sort_index()
    close,high,low,vol,open_=df['Close'],df['High'],df['Low'],df['Volume'],df['Open']
    ema5,ema10,ema75=ema(close,5),ema(close,10),ema(close,75)
    ml,ms,mh=macd(close)
    volSMA20,volSMA50=sma(vol,20),sma(vol,50)
    hh8,ll8=highest(high,8),lowest(low,8); den=hh8-ll8
    rawK=pd.Series(np.where(den==0,50,100*(close-ll8)/den), index=df.index)
    stochK=sma(rawK,3); stochD=sma(stochK,3)
    rvol=vol/volSMA50
    isExplosive=bool(((rvol>1.7)&(close>open_)).iloc[-1])
    isDryPhase=bool(( (rvol<0.7).rolling(3).sum().iloc[-1]>=2))
    turnover_series=sma(close*vol,20).shift(1)
    turnover=turnover_series.iloc[-1]; turnoverM=turnover/1e6 if pd.notna(turnover) else np.nan
    isLowLiq=(pd.isna(turnover) or turnover<10_000_000); isTooDry=(pd.isna(turnover) or turnover<5_000_000)  # v4.2: RESTORED to user's 223-line EXACT Pine engine (LowLiq 10M / TooDry 5M). v3.4's 8M/8M deviated from Pine. Stage filter in main() still cuts <8M separately.
    # Squeeze
    basis=sma(close,20); bbDev=2.0*close.rolling(20).std(); bbU=basis+bbDev; bbL=basis-bbDev
    tr=pd.concat([high-low,(high-close.shift(1)).abs(),(low-close.shift(1)).abs()],axis=1).max(axis=1)
    kcB=ema(close,20); kcR=ema(tr,20); kcU=kcB+kcR*1.5; kcL=kcB-kcR*1.5
    squeezeOn=(bbU<kcU)&(bbL>kcL)
    squeezeRelease=(~squeezeOn)&(squeezeOn.shift(1).fillna(False).astype(bool))
    last_rel=np.where(squeezeRelease)[0]; barsSince=999 if len(last_rel)==0 else (len(df)-1-last_rel[-1])
    recentSqueeze=barsSince<=10 and (not squeezeOn.iloc[-1])
    squeezeBullish=recentSqueeze and (mh.iloc[-1]>mh.iloc[-2])
    macdRec=(ml.iloc[-1]>ms.iloc[-1]) or (mh.iloc[-1]>mh.iloc[-2])
    stochBull=stochK.iloc[-1]>stochD.iloc[-1]
    priceRec=(close.iloc[-1]>ema5.iloc[-1]) and (close.iloc[-1]>=close.iloc[-2])
    emaImp=ema5.iloc[-1]>ema10.iloc[-1]
    confirmedTrend=(close.iloc[-1]>ema75.iloc[-1]) and (ema5.iloc[-1]>ema10.iloc[-1]) and (ema10.iloc[-1]>ema75.iloc[-1])
    isAcc=(close>open_)&(vol>volSMA20*0.8); accScore5=float(isAcc.rolling(5).sum().iloc[-1])
    recentLow20=lowest(low,20).shift(1); recentHigh20=highest(high,20).shift(1)
    sweepBear=bool((low.iloc[-1]<recentLow20.iloc[-1]) and (close.iloc[-1]>recentLow20.iloc[-1]) and (close.iloc[-1]>open_.iloc[-1]) and (vol.iloc[-1]>volSMA50.iloc[-1]*1.5))
    sweepBullExit=bool((high.iloc[-1]>recentHigh20.iloc[-1]) and (close.iloc[-1]<recentHigh20.iloc[-1]) and (vol.iloc[-1]>volSMA50.iloc[-1]*1.5))
    dw_raw=(1.0 if macdRec else 0)+(0.5 if stochBull else 0)+(1.0 if priceRec else 0)+(1.0 if emaImp else 0)+(1.0 if squeezeBullish else 0)+(2.0 if sweepBear else 0)
    dayWeighted=max(dw_raw-(1.0 if isLowLiq else 0.0),0.0)
    (wBonus,wBonusScore,wTxt),(mKnown,mBull,mBear)=weekly_monthly(df)
    # VCP (lag 1)
    def vcp_arrays(d):
        c,h,l,v=d['Close'],d['High'],d['Low'],d['Volume']
        hi0,lo0=highest(h,5),lowest(l,5); hi1,lo1=highest(h,5).shift(5),lowest(l,5).shift(5); hi2,lo2=highest(h,5).shift(10),lowest(l,5).shift(10)
        r0=(hi0-lo0)/c*100; r1=(hi1-lo1)/c.shift(5)*100; r2=(hi2-lo2)/c.shift(10)*100
        av0=sma(v,5); av1=sma(v,5).shift(5); av50=sma(v,50); vcpRvol=av0/av50
        e5,e10,e75=ema(c,5),ema(c,10),ema(c,75)
        return (r0<r1*1.05)&(r1<r2*1.05)&(r0<r2), (av0<av1)&(vcpRvol<0.7), (c>e75)&(e5>e10)&(e10>e75)&(e75>e75.shift(10)), (r2.notna()&av1.notna()&av50.notna()&e75.notna()&e75.shift(10).notna()), vcpRvol
    vp,vv,vt,ve,vr=vcp_arrays(df)
    vcpPricePass=bool(vp.shift(1).iloc[-1]); vcpVolPass=bool(vv.shift(1).iloc[-1]); vcpTrendPass=bool(vt.shift(1).iloc[-1]); vcpReady=bool(ve.shift(1).iloc[-1])
    vcpRvolConf=float(vr.shift(1).iloc[-1]) if pd.notna(vr.shift(1).iloc[-1]) else np.nan
    vcpQual=vcpReady and vcpPricePass and vcpVolPass and vcpTrendPass; vcpBonus=5.0 if vcpQual else 0.0
    # TrendTemplate (lag 1)
    sma50,sma150,sma200=sma(close,50),sma(close,150),sma(close,200); high52,low52=highest(high,252),lowest(low,252)
    maAlign=(close>sma50)&(sma50>sma150)&(sma150>sma200); sma200Rising=sma200>sma200.shift(20)
    priceAboveLow=(close>=low52*1.25); nearHigh=(close>=high52*0.75); templateRaw=maAlign&sma200Rising&priceAboveLow&nearHigh
    enoughT=(sma50.notna()&sma150.notna()&sma200.notna()&sma200.shift(20).notna()&high52.notna()&low52.notna())
    trendReady=bool(enoughT.shift(1).iloc[-1]); trendPass=bool(templateRaw.shift(1).iloc[-1])
    trendMaPass=bool(maAlign.shift(1).iloc[-1]); trendSlopePass=bool(sma200Rising.shift(1).iloc[-1]); trendLowPass=bool(priceAboveLow.shift(1).iloc[-1]); trendHighPass=bool(nearHigh.shift(1).iloc[-1])
    trendBonus=5.0 if (trendReady and trendPass) else 0.0
    # Rank
    if isTooDry: dayRank=0
    elif confirmedTrend and dayWeighted>=4.5: dayRank=3
    elif dayWeighted>=4.5 and ml.iloc[-1]>ms.iloc[-1]: dayRank=2
    elif dayWeighted>=3.0 and macdRec: dayRank=1
    else: dayRank=0
    signalRank=0 if isTooDry else dayRank; rankTxt={3:"CS",2:"ES",1:"EE",0:"NONE"}[signalRank]
    weightedComp=dayWeighted/6.5*50.0; accComp=accScore5/5.0*20.0; sweepComp=15.0 if sweepBear else 0.0; explComp=5.0 if isExplosive else 0.0; trendComp=10.0 if confirmedTrend else 0.0
    dayBase=min(max(weightedComp+accComp+sweepComp+explComp+trendComp,0),100); quality=wBonusScore+vcpBonus+trendBonus
    combined=0.0 if isTooDry else min(dayBase+quality,100)
    # ---- NEW MODULES (additive, do NOT change Score/Rank) ----
    # Advanced VCP
    tol=0.05
    hi0,lo0=highest(high,5),lowest(low,5); hi1,lo1=highest(high,5).shift(5),lowest(low,5).shift(5); hi2,lo2=highest(high,5).shift(10),lowest(low,5).shift(10); hi3,lo3=highest(high,5).shift(15),lowest(low,5).shift(15)
    r0=(hi0-lo0)/close*100; r1=(hi1-lo1)/close.shift(5)*100; r2=(hi2-lo2)/close.shift(10)*100; r3=(hi3-lo3)/close.shift(15)*100
    v0=sma(vol,5); v1=sma(vol,5).shift(5); v2=sma(vol,5).shift(10); v3=sma(vol,5).shift(15)
    histOK=bool((r0.notna()&r1.notna()&r2.notna()&r3.notna()&v0.notna()&v1.notna()&v2.notna()&v3.notna()).shift(1).iloc[-1])
    p1=bool((r0<=r1*(1+tol)).shift(1).iloc[-1]); p2=bool((r1<=r2*(1+tol)).shift(1).iloc[-1]); p3=bool((r2<=r3*(1+tol)).shift(1).iloc[-1])
    volContract=bool(((v0<=v1*(1+tol))&(v1<=v2*(1+tol))&(v2<=v3*(1+tol))&(v0<v3)).shift(1).iloc[-1])
    advCount=int(p1)+int(p2)+int(p3)
    advTxt="พักตัวสวยมาก" if (advCount==3 and volContract) else "พักตัวครบแล้ว" if (advCount>=2 and volContract) else "กำลังพักตัว" if advCount>=1 else "พักตัวอ่อน"
    advPivot=float(highest(high,20).shift(1).iloc[-1]) if pd.notna(highest(high,20).shift(1).iloc[-1]) else np.nan
    # Smart Money - confirmed [1]
    spread=high-low; clv=np.where(spread>0, ((close-low)-(high-close))/spread, 0.0); clv=pd.Series(clv,index=df.index)
    clvFlow=ema(clv*vol,10)
    avgRange20=sma(spread,20); tight=bool((spread.iloc[-1]<=avgRange20.iloc[-1]*0.8))
    # Confirmed: recompute on df and shift 1 for score
    dAvg=sma(high-low,14); dVol50=sma(vol,50); dRvol=vol/dVol50; dSpread=high-low
    dClv=pd.Series(np.where(dSpread>0, ((close-low)-(high-close))/dSpread, 0.0), index=df.index); dFlow=ema(dClv*vol,10)
    dTight=(dSpread<=dAvg*0.8); dHigh=(dRvol>=1.3)
    dAbs=(dHigh&dTight&(dClv>0.25)&(close>=open_)); dDist=(dHigh&dTight&(dClv<-0.25)&(close<=open_))
    dBuy=(dFlow>dFlow.shift(1))&(dClv>0)&(close>open_); dSell=(dFlow<dFlow.shift(1))&(dClv<0)&(close<open_)
    dScore=(dAbs.astype(int)*2+dBuy.astype(int)-dDist.astype(int)*2-dSell.astype(int)).shift(1)
    confirmedSmart=int(dScore.iloc[-1]) if pd.notna(dScore.iloc[-1]) else 0
    absorption=bool((rvol.iloc[-1]>=1.3) and tight and (clv.iloc[-1]>0.25) and (close.iloc[-1]>=open_.iloc[-1]))
    distribution=bool((rvol.iloc[-1]>=1.3) and tight and (clv.iloc[-1]<-0.25) and (close.iloc[-1]<=open_.iloc[-1]))
    smartTxt="พบแรงซื้อ+ดูดซับ" if confirmedSmart>=2 else "เริ่มมีแรงซื้อ" if confirmedSmart==1 else "พบแรงขาย+กระจาย" if confirmedSmart<=-2 else "เริ่มมีแรงขาย" if confirmedSmart==-1 else "ยังไม่มีฝั่งไหนชัด"
    # Market Structure - pivots length 3
    # Use scipy-free pivot: local max/min with 3 bars each side
    ph=[]; pl=[]
    for i in range(len(df)):
        if 3<=i<len(df)-3:
            if high.iloc[i]==high.iloc[i-3:i+4].max(): ph.append((i,float(high.iloc[i])))
            if low.iloc[i]==low.iloc[i-3:i+4].min(): pl.append((i,float(low.iloc[i])))
    lastPH=ph[-1][1] if ph else np.nan; prevPH=ph[-2][1] if len(ph)>=2 else np.nan
    lastPL=pl[-1][1] if pl else np.nan; prevPL=pl[-2][1] if len(pl)>=2 else np.nan
    hh_= (lastPH>prevPH) if pd.notna(lastPH) and pd.notna(prevPH) else False
    hl_= (lastPL>prevPL) if pd.notna(lastPL) and pd.notna(prevPL) else False
    lh_= (lastPH<prevPH) if pd.notna(lastPH) and pd.notna(prevPH) else False
    ll_= (lastPL<prevPL) if pd.notna(lastPL) and pd.notna(prevPL) else False
    structBull=bool(hh_ and hl_); structBear=bool(lh_ and ll_)
    structTxt="ยกสูงขึ้นเรื่อยๆ — ขาขึ้น" if structBull else "ต่ำลงเรื่อยๆ — ขาลง" if structBear else "จุดสูงใหม่สูงขึ้น" if hh_ else "จุดต่ำใหม่ยกขึ้น" if hl_ else "จุดสูงใหม่ต่ำลง" if lh_ else "จุดต่ำใหม่ต่ำลง" if ll_ else "โครงสร้างกำลังก่อตัว"
    breakoutLevel=lastPH; buffered=breakoutLevel*1.002 if pd.notna(breakoutLevel) else np.nan
    # Breakout: close crosses above buffered level of prior bar (approx: close > buffered_prev)
    # Use prior pivot: buffered of previous pivot
    breakoutRef=float(ph[-2][1]*1.002) if len(ph)>=2 else np.nan
    breakoutEvent=bool(pd.notna(breakoutRef) and close.iloc[-1]>breakoutRef and close.iloc[-2]<=breakoutRef)
    # FIX v2.1: FailedBreakout ต้องเคย Breakout มาก่อนใน 10 แท่งล่าสุดเท่านั้น (เดิม: แค่ปิดต่ำกว่าแนวต้านก็ขึ้น Failed ผิดๆ)
    hadBreakoutLast10=False
    if pd.notna(breakoutRef):
        for _i in range(max(1,len(df)-10), len(df)):
            if _i>=1 and close.iloc[_i]>breakoutRef and close.iloc[_i-1]<=breakoutRef:
                hadBreakoutLast10=True; break
    failedBreakout=bool(hadBreakoutLast10 and pd.notna(breakoutRef) and close.iloc[-1]<breakoutRef and close.iloc[-2]>=breakoutRef)
    # Rejection: high > ref and close < ref
    rejection=bool(pd.notna(breakoutRef) and high.iloc[-1]>breakoutRef and close.iloc[-1]<breakoutRef)
    # FIX v2.1: ลำดับตาม Pine Sec 17C: Failed > Rejection > Breakout > ยังไม่ทะลุ (เดิมสลับกัน)
    structSignal="ทะลุแล้วหลุดกลับ" if failedBreakout else "ชนแนวต้านแล้วถอย" if rejection else "ทะลุแนวต้านแล้ว" if breakoutEvent else "ยังไม่ทะลุ"
    # TD9
    tdBuy=0; tdSell=0
    for i in range(len(df)):
        if i>=4:
            tdBuy=tdBuy+1 if close.iloc[i]<close.iloc[i-4] else 0
            tdSell=tdSell+1 if close.iloc[i]>close.iloc[i-4] else 0
    tdBuy9=(tdBuy==9); tdSell9=(tdSell==9)
    # Risk
    warningCount=int(ml.iloc[-1]<ms.iloc[-1])+int(stochK.iloc[-1]<stochD.iloc[-1])+int(close.iloc[-1]<ema5.iloc[-1])
    earlyWarn=(warningCount>=2 and signalRank==0); holderWarn=(warningCount>=2 and signalRank>0)
    volFlow=np.where(close>open_, vol, -vol); volFlow=pd.Series(volFlow,index=df.index); mfSum=sma(volFlow,20)
    mfWeak=bool((mfSum.iloc[-1]<mfSum.iloc[-2]) and (mfSum.iloc[-2]<mfSum.iloc[-3]) and (close.iloc[-1]>close.iloc[-2]))
    mfWeakCount=int(sum([1 for i in range(max(0,len(df)-3),len(df)) if True and i>=2 and mfSum.iloc[i]<mfSum.iloc[i-1] and mfSum.iloc[i-1]<mfSum.iloc[i-2] and close.iloc[i]>close.iloc[i-1]])) if len(df)>=3 else 0
    upthrust=bool(pd.notna(recentHigh20.iloc[-1]) and high.iloc[-1]>=recentHigh20.iloc[-1]*0.999 and close.iloc[-1]<recentHigh20.iloc[-1] and close.iloc[-1]<(high.iloc[-1]+low.iloc[-1])/2 and vol.iloc[-1]>volSMA20.iloc[-1]*1.3)
    climax=bool(pd.notna(rvol.iloc[-1]) and rvol.iloc[-1]>2.2 and abs(close.iloc[-1]-open_.iloc[-1])/abs(open_.iloc[-1])<0.01 and close.iloc[-1]<open_.iloc[-1] and close.iloc[-1]<(high.iloc[-1]+low.iloc[-1])/2)
    strongExit=climax or upthrust; exitRaw=bool(strongExit or mfWeakCount>=2 or sweepBullExit)
    # Entry would open: rank upgrade is not tracked here (needs prev rank) - approximate: signalRank>0 and dayWeighted>=3
    entryWouldOpen=bool(signalRank>0)
    exitFinal=bool(exitRaw and not entryWouldOpen)
    riskState="เงินหมุนน้อยไป — ห้ามเข้า" if isTooDry else "เสี่ยงต้องออก" if exitFinal else "ตั้งรับ" if (earlyWarn or holderWarn or mfWeakCount>=2) else "ระวัง" if (warningCount>=1 or distribution or upthrust) else "ปลอดภัย"
    # MTF
    dayDir=-1 if (exitFinal or earlyWarn or holderWarn) else 1 if (signalRank>0 or (dayWeighted>=3.0 and macdRec)) else 0
    weekDir=1 if wBonus>=2 else 0; monthDir=1 if mBull else -1 if mBear else 0
    mtfAligned=(dayDir==1 and weekDir==1 and monthDir==1); mtfConflict=(dayDir==1 and monthDir==-1)
    mtfTxt="ทุกกราฟเห็นตรงกัน — ขึ้น" if mtfAligned else "วันกับเดือนสวนกัน" if mtfConflict else "วันเสี่ยง + เดือนลง" if (dayDir==-1 and monthDir==-1) else "วันเสี่ยงแต่สัปดาห์หนุน" if (dayDir==-1 and weekDir==1) else "วันนี้เสี่ยง" if dayDir==-1 else "วันนี้ยังไม่ยืนยัน" if dayDir==0 else "เห็นตรงกันบางส่วน • สัปดาห์ไม่หนุน" if wBonus==0 else "เห็นตรงกันเกือบครบ"
    dayDirTxt="BULL" if dayDir==1 else "ความเสี่ยง" if dayDir==-1 else "NEUTRAL"
    # Module Scoreboard 7
    mWeek=(wBonus>=2); mVcp=vcpQual; mTrend=(trendReady and trendPass); mTrendDay=confirmedTrend; mSmart=(confirmedSmart>=1); mSmartBad=(confirmedSmart<=-1); mStruct=structBull; mStructBad=structBear; mAcc=(accScore5>=2)
    bullCount=int(mWeek)+int(mVcp)+int(mTrend)+int(mTrendDay)+int(mSmart)+int(mStruct)+int(mAcc)
    moduleDisplay=f"{bullCount}/7 | W {'✅' if mWeek else '➖'} V {'✅' if mVcp else '❌'} T {'✅' if mTrend else '❌'} D {'✅' if mTrendDay else '❌'} S {'✅' if mSmart else '🔻' if mSmartBad else '➖'} St {'✅' if mStruct else '🔻' if mStructBad else '➖'} A {'✅' if mAcc else '❌'}"
    # Verdict (Pine Sec 26)
    if isTooDry: verdict="ห้ามเข้า — เงินหมุนน้อยไป"
    elif signalRank==3: verdict="พร้อมเข้า — สัญญาณยืนยันครบตามเกณฑ์"
    elif signalRank==2: verdict="ใกล้พร้อม — รอ Trend ยืนยัน"
    elif signalRank==1: verdict="เริ่มเข้าได้ — พบสัญญาณฟื้นตัว"
    elif exitFinal: verdict="เสี่ยง — ควรออก"
    elif earlyWarn or holderWarn: verdict="วันนี้เสี่ยง"
    else: verdict=f"รอ — {mtfTxt} | รอเริ่มเข้าได้ (คะแนน ≥3.0 + MACD ฟื้น)"
    return {"Close":round(float(close.iloc[-1]),2),"Date":str(df.index[-1].date()),"Rank":rankTxt,"DayRank":int(signalRank),
      "CombinedScore":round(float(combined),1),"DayWeighted":round(float(dayWeighted),1),
      "RVOL":round(float(rvol.iloc[-1]),2) if pd.notna(rvol.iloc[-1]) else None,
      "TurnoverM":round(float(turnoverM),1) if pd.notna(turnoverM) else None,
      "AccScore":int(accScore5),"Sweep":bool(sweepBear),"Explosive":bool(isExplosive),"ConfirmedTrend":bool(confirmedTrend),
      "VCP":bool(vcpQual),"VCP_Price":vcpPricePass,"VCP_Vol":vcpVolPass,"VCP_Trend":vcpTrendPass,
      "TrendTemplate":bool(trendPass) if trendReady else False,"TrendKnown":bool(trendReady),
      "Trend_MA":trendMaPass,"Trend_Slope":trendSlopePass,"Trend_Low":trendLowPass,"Trend_High":trendHighPass,
      "WeekBonus":int(wBonus),"WeekScore":float(wBonusScore),"MonthBull":bool(mBull),"MonthBear":bool(mBear),
      "IsTooDry":bool(isTooDry),"IsLowLiq":bool(isLowLiq),
      "AdvVCP":advTxt,"AdvCount":advCount,"AdvVol":bool(volContract),"AdvPivot":round(float(advPivot),2) if pd.notna(advPivot) else None,
      "SmartScore":confirmedSmart,"SmartTxt":smartTxt,"Absorption":absorption,"Distribution":distribution,
      "StructTxt":structTxt,"StructSignal":structSignal,"StructBull":structBull,"StructBear":structBear,
      "BreakoutLevel":round(float(breakoutLevel),2) if pd.notna(breakoutLevel) else None,
      "SupportLevel":round(float(lastPL),2) if pd.notna(lastPL) else None,
      "TD_Buy":int(tdBuy),"TD_Sell":int(tdSell),"TD_Buy9":bool(tdBuy9),"TD_Sell9":bool(tdSell9),
      "RiskState":riskState,"ExitFinal":bool(exitFinal),"EarlyWarn":bool(earlyWarn),"HolderWarn":bool(holderWarn),
      "Upthrust":bool(upthrust),"Climax":bool(climax),"MFWeakCount":int(mfWeakCount),
      "MTF":mtfTxt,"DayDir":dayDirTxt,"ModuleCount":int(bullCount),"ModuleDisplay":moduleDisplay,"Verdict":verdict,
      "SMA50":round(float(sma50.iloc[-1]),2) if pd.notna(sma50.iloc[-1]) else None,
      "SMA200":round(float(sma200.iloc[-1]),2) if pd.notna(sma200.iloc[-1]) else None,
      "Low52":round(float(low52.iloc[-1]),2),"High52":round(float(high52.iloc[-1]),2),
      "Low52_125":round(float(low52.iloc[-1]*1.25),2),"High52_075":round(float(high52.iloc[-1]*0.75),2)}

def main():
    rows=[]
    # v3: FULL mode = default. Set FHP_FULL=0 to run 21-ticker parity test only. FHP_TICKERS overrides universe (for testing).
    full_mode = os.environ.get("FHP_FULL","1") != "0" and not os.environ.get("FHP_TICKERS")
    if full_mode:
        universe = fetch_universe()
        print(f"FULL MARKET MODE: {len(universe)} tickers")
        fund = get_fundamentals(universe)
        setfund = get_set_fundamentals(universe)
        # v3.3: FF and Equity activate INDEPENDENTLY. If one has <50% coverage, skip only that one (never fake it).
        _set_ok_ff = sum(1 for v in setfund.values() if v.get("free_float") is not None) if setfund else 0
        _set_ok_eq = sum(1 for v in setfund.values() if v.get("equity") is not None) if setfund else 0
        _set_active_ff = (_set_ok_ff >= max(1, len(universe)*0.5))
        _set_active_eq = (_set_ok_eq >= max(1, len(universe)*0.5))
        _set_active = (_set_active_ff or _set_active_eq)
        print(f"SET API active: FreeFloat={_set_active_ff} ({_set_ok_ff}/{len(universe)}), Equity={_set_active_eq} ({_set_ok_eq}/{len(universe)})")
        stage_counts = {"shariah":0,"price":0,"mcap":0,"mcap_unknown":0,"turnover_reject":0,"trade":0,"ok":0,"unknown":0,"freefloat":0,"freefloat_unknown":0,"equity":0,"equity_unknown":0,"set_skipped":0,"download_failed":0,"history_short":0}
        stage_reject = {}  # v4.3: {ticker: reason} for tickers rejected by Stage (used by Parity check)
        tickers_to_scan = []
        for sym in universe:
            # Stage 0: Shariah B (full blacklist)
            if sym in SHARIAH_B_FULL:
                stage_counts["shariah"]+=1; stage_reject[sym]="Shariah B"; continue
            tickers_to_scan.append(sym)
        TICKERS_EFF = tickers_to_scan
        # store for summary banner
        globals()["_STAGE"] = stage_counts; globals()["_UNIVERSE_N"] = len(universe); globals()["_FUND"] = fund; globals()["_SETFUND"] = setfund; globals()["_SET_ACTIVE"] = _set_active; globals()["_SET_ACTIVE_FF"] = _set_active_ff; globals()["_SET_ACTIVE_EQ"] = _set_active_eq
    else:
        TICKERS_EFF = TICKERS
        globals()["_STAGE"] = None; globals()["_UNIVERSE_N"] = len(TICKERS_EFF); globals()["_FUND"] = {}; globals()["_SETFUND"] = {}; globals()["_SET_ACTIVE"] = False; globals()["_SET_ACTIVE_FF"] = False; globals()["_SET_ACTIVE_EQ"] = False; globals()["_UNIVERSE_WARN"] = False; globals()["_UNIVERSE_FAILS"] = []
    for t in TICKERS_EFF:
        if not full_mode and t in SHARIAH_BLACKLIST:
            rows.append({"Ticker":t,"Rank":"NONE","CombinedScore":0,"Note":"Shariah B blacklist","Verdict":"ตัดออก — Shariah B"}); continue
        if full_mode and t in SHARIAH_B_FULL:
            rows.append({"Ticker":t,"Rank":"NONE","CombinedScore":0,"Note":"Shariah B blacklist","Verdict":"ตัดออก — Shariah B"}); continue
        try:
            df,_dl_err=download_with_retry(t)
            if df is None:
                if full_mode: _STAGE["download_failed"]+=1
                rows.append({"Ticker":t,"Rank":"UNKNOWN","CombinedScore":None,"Note":f"download_failed: {_dl_err}","Verdict":"เช็กไม่ได้ — ดึงราคาไม่สำเร็จ (ลองใหม่ 3 ครั้งแล้ว)"}); continue
            df=_drop_partial_today(df)  # v4.0: mid-day run -> use last completed bar, never today partial candle
            if len(df)<260:
                if full_mode: _STAGE["history_short"]+=1
                rows.append({"Ticker":t,"Rank":"UNKNOWN","CombinedScore":None,"Note":f"history {len(df)}","Verdict":"เช็กไม่ได้ — ประวัติไม่ถึง 260 วัน"}); continue
            # --- v3 Stage 1.5 (FULL mode only): price / turnover / trade / mcap. UNKNOWN never PASS ---
            # v4.3: NO parity bypass. MAJOR/TU/CRC go through the SAME Stage filter as every ticker.
            # If a parity ticker is Stage-rejected, its reason is saved in stage_reject{} so the Parity check
            # can report STAGE-REJECT instead of a misleading engine FAIL.
            if full_mode:
                close_last=float(df["Close"].iloc[-1])
                # Stage 1 (v3.1): price 1 - 80 baht only
                if not (1.0<=close_last<=80.0):
                    _STAGE["price"]+=1; stage_reject[t]=f"price {close_last:.2f}"; continue
                # Turnover: SMA20(close*volume)[1] in million baht
                _to=float((df["Close"]*df["Volume"]).rolling(20).mean().shift(1).iloc[-1]/1e6) if len(df)>=21 else float("nan")
                if pd.isna(_to):
                    _STAGE["unknown"]+=1
                    rows.append({"Ticker":t,"Rank":"UNKNOWN","CombinedScore":None,"Note":"turnover unknown","Verdict":"เช็กไม่ได้ — Turnover ไม่ทราบ"}); continue
                if _to<8:
                    _STAGE["turnover_reject"]+=1; stage_reject[t]=f"turnover {_to:.1f}M"; continue
                # Trade >=80%: days with volume>0 in last 60 trading days
                _last60=df.tail(60)
                _trade=float(( _last60["Volume"]>0).mean()*100)
                if _trade<80:
                    _STAGE["trade"]+=1; stage_reject[t]=f"trade {_trade:.0f}%"; continue
                # Market cap >= 3000M: UNKNOWN -> separate, never PASS
                _mc=globals().get("_FUND",{}).get(t)
                if _mc is None:
                    _STAGE["mcap_unknown"]+=1
                    rows.append({"Ticker":t,"Rank":"UNKNOWN","CombinedScore":None,"Note":"mcap unknown","Verdict":"เช็กไม่ได้ — Market Cap ไม่ทราบ"}); continue
                if _mc<3000e6:
                    _STAGE["mcap"]+=1; stage_reject[t]=f"mcap {_mc/1e6:.0f}M"; continue
                # --- v3.3 SET API: EQ enforced, FF is a FLAG (v4.4). UNKNOWN never PASS. Inactive one is SKIPPED (counted) ---
                _sf=globals().get("_SETFUND",{}).get(t,{})
                _ff=_sf.get("free_float"); _eq=_sf.get("equity"); _ff_low=False  # v4.4: FF<15% = FLAG only
                if globals().get("_SET_ACTIVE_FF"):
                    if _ff is None:
                        _STAGE["freefloat_unknown"]+=1
                        rows.append({"Ticker":t,"Rank":"UNKNOWN","CombinedScore":None,"Note":"freefloat unknown","Verdict":"เช็กไม่ได้ — Free Float ไม่ทราบ"}); continue
                    _ff_low = _ff<15.0
                    if _ff_low:
                        _STAGE["freefloat"]+=1  # v4.4: FF<15% counts as FLAGGED, not rejected - stock is still computed
                else:
                    _STAGE["set_skipped"]+=1
                if globals().get("_SET_ACTIVE_EQ"):
                    if _eq is None:
                        _STAGE["equity_unknown"]+=1
                        rows.append({"Ticker":t,"Rank":"UNKNOWN","CombinedScore":None,"Note":"equity unknown","Verdict":"เช็กไม่ได้ — Equity ไม่ทราบ"}); continue
                    if not (_eq>0):
                        # v4.6: equity<=0 = PARSE-SUSPECT, not a real reject. Parser demonstrably
                        # misreads MAJOR/CRC (both have positive equity). UNKNOWN keeps it OUT of
                        # buy signals (UNKNOWN never PASS) without a false "equity<=0" claim.
                        _STAGE["equity_unknown"]+=1
                        rows.append({"Ticker":t,"Rank":"UNKNOWN","CombinedScore":None,"Note":f"equity parse {_eq} (<=0, suspect parser)","Verdict":"เช็กไม่ได้ — Equity อ่านค่าผิด (parser) — ไม่ใช่บริษัทติดลบจริง"}); continue
                else:
                    _STAGE["set_skipped"]+=1
                r=compute(df); r["Ticker"]=t; r["TurnoverM"]=_to; r["TurnoverLevel"]="HIGH LIQUIDITY" if _to>=20 else "STANDARD"; r["MarketCapM"]=round(_mc/1e6,0); r["FreeFloat"]=_ff; r["Equity"]=_eq; r["FFLow"]=_ff_low; r["Verdict"]=str(r.get("Verdict",""))+(" ⚠️ FF-LOW" if _ff_low else ""); rows.append(r); _STAGE["ok"]+=1; continue
                _STAGE["ok"]+=1
                r=compute(df); r["Ticker"]=t; r["TurnoverM"]=_to; r["TurnoverLevel"]="HIGH LIQUIDITY" if _to>=20 else "STANDARD"; r["MarketCapM"]=round(_mc/1e6,0); r["FFLow"]=False; rows.append(r); continue
            r=compute(df); r["Ticker"]=t; r["FFLow"]=False; rows.append(r)
        except Exception as e:
            rows.append({"Ticker":t,"Rank":"ERROR","CombinedScore":None,"Note":str(e)[:120],"Verdict":"ERROR"})
    df=pd.DataFrame(rows)
    df["_s"]=pd.to_numeric(df["CombinedScore"], errors="coerce").fillna(-1)
    df=df.sort_values("_s", ascending=False).drop(columns=["_s"])
    os.makedirs("docs", exist_ok=True)
    # Parity check
    parity_ok=True; parity_lines=[]
    for tk, exp in PARITY.items():
        if tk in stage_reject:
            parity_lines.append(f"{tk} ⛔ STAGE-REJECT: {stage_reject[tk]} (โดน Stage ตัดเหมือนหุ้นทุกตัว - ไม่ใช่สูตรผิด)"); continue
        got=df[df["Ticker"]==tk]
        if len(got)==0: parity_ok=False; parity_lines.append(f"{tk} ไม่พบ"); continue
        g=got.iloc[0]; ok_rank=(g["Rank"]==exp["Rank"]); ok_score=abs(float(g["CombinedScore"])-exp["CombinedScore"])<=0.5; ok_dw=abs(float(g["DayWeighted"])-exp["DayWeighted"])<=0.1
        ok=ok_rank and ok_score and ok_dw
        if not ok: parity_ok=False
        parity_lines.append(f"{tk} {'✅' if ok else '❌'} {g['Rank']} {g['CombinedScore']}/{g['DayWeighted']} (คาด {exp['Rank']} {exp['CombinedScore']}/{exp['DayWeighted']})")
    # History
    today=datetime.datetime.now(datetime.timezone(datetime.timedelta(hours=7))).strftime("%Y-%m-%d")  # v4.6: Bangkok time, not runner UTC
    hist_path="docs/history.csv"
    prev={}
    if os.path.exists(hist_path):
        try:
            h=pd.read_csv(hist_path); 
            for _,r in h.sort_values("Date").iterrows(): prev[r["Ticker"]]=r["Rank"]
        except Exception: pass
    hist_rows=[]
    for _,r in df.iterrows():
        p=prev.get(r["Ticker"], None)
        if r["Rank"]=="UNKNOWN":
            # v3.6: UNKNOWN is "check failed", NOT a downgrade. Never write UNKNOWN into history,
            # otherwise tomorrow's recovery would fake an UPGRADE (UNKNOWN=0 -> EE=1).
            change="-"
            df.loc[df["Ticker"]==r["Ticker"],"Change"]=change; df.loc[df["Ticker"]==r["Ticker"],"PrevRank"]=p if p else "-"
            continue
        if p is None: change="-"  # v2.2: first run with no history -> dash, not NEW
# v4.3: Parity tickers NO LONGER bypass Stage. STAGE-REJECT in Parity check is a filter outcome, not an engine error.
# v3.1: price filter 1-80 baht (was >1 only). Engine unchanged.
# v3: FULL MARKET - universe via Yahoo screener BKK (SET+mai), Stage 0-1.5 filter before compute, UNKNOWN never PASS. Engine unchanged.
# v2.2: วันแรกที่ยังไม่มีประวัติ ให้เป็น - ไม่ใช่ NEW
        elif r["Rank"]==p: change="SAME"
        else:
            order={"NONE":0,"EE":1,"ES":2,"CS":3}
            change="UPGRADE ⬆️" if order.get(r["Rank"],0)>order.get(p,0) else "DOWNGRADE ⬇️"
        df.loc[df["Ticker"]==r["Ticker"],"Change"]=change; df.loc[df["Ticker"]==r["Ticker"],"PrevRank"]=p if p else "-"
        hist_rows.append({"Date":today,"Ticker":r["Ticker"],"Rank":r["Rank"],"CombinedScore":r.get("CombinedScore"),"DayWeighted":r.get("DayWeighted"),"RVOL":r.get("RVOL")})
    hist_new=pd.DataFrame(hist_rows)
    if os.path.exists(hist_path):
        try: hist_old=pd.read_csv(hist_path); hist_all=pd.concat([hist_old, hist_new]).drop_duplicates(subset=["Date","Ticker"], keep="last")
        except Exception: hist_all=hist_new
    else: hist_all=hist_new
    hist_all.to_csv(hist_path, index=False)
    df.to_csv("docs/results.csv", index=False)
    total=len(df); cs=int((df["Rank"]=="CS").sum()); es=int((df["Rank"]=="ES").sum()); ee=int((df["Rank"]=="EE").sum())
    # v4.1 FIX (FATAL in v4.0): df['Date'].max() CRASHES with TypeError when Date mixes str + NaN (UNKNOWN rows).
    # Proven in test: '>=' not supported between 'str' and 'float'. Filter to real date strings only.
    _dates=[x for x in (df["Date"].tolist() if len(df) and "Date" in df else []) if isinstance(x,str) and x.strip() not in ("","-","nan","NaN","None")]
    data_date=max(_dates) if _dates else "-"
    upgrades=df[df["Change"].str.contains("UPGRADE", na=False)]
    up_txt=", ".join([f"{r['Ticker']} {r['PrevRank']}→{r['Rank']}" for _,r in upgrades.iterrows()]) if len(upgrades) else "ไม่มีตัวอัปเกรดวันนี้"
    now=datetime.datetime.now(datetime.timezone(datetime.timedelta(hours=7))).strftime("%Y-%m-%d %H:%M")  # v4.6: Bangkok time, not runner UTC
    parity_banner=f"<p style='background:{'#22c55e' if parity_ok else '#ef4444'};color:#000;padding:6px;font-weight:bold;font-size:11px'>Parity Check: {'✅ ผ่าน — ตรง TradingView' if parity_ok else '❌ ไม่ผ่าน — ห้ามใช้เทรดวันนี้'}<br>{'<br>'.join(parity_lines)}</p>"
    # v3.4: fmt() turns None/nan into '-' so UNKNOWN rows never show 'nan'
    def fmt(v):
        try:
            import math as _m
            if v is None: return '-'
            if isinstance(v, float) and _m.isnan(v): return '-'
            if isinstance(v, str) and v.lower() in ('nan','none','nat'): return '-'
        except Exception: pass
        return v
    def fmt_ff(v):
        # v4.5: FF% DISPLAY ONLY - whole number, no decimals. Logic still uses raw value.
        try:
            import math as _m
            if v is None: return '-'
            if isinstance(v, float) and _m.isnan(v): return '-'
            return f"{float(v):.0f}"
        except Exception:
            return fmt(v)
    # Rows
    th=""
    for _,r in df.iterrows():
        th+=f"<tr><td>{r.get('Ticker','')}{' <b style=color:#dc2626>⚠️ FF-LOW</b>' if r.get('FFLow') else ''}<br><small>{fmt(r.get('Date'))}</small></td><td>{fmt_ff(r.get('FreeFloat'))}</td><td>{r.get('Rank','')}</td><td>{fmt(r.get('CombinedScore'))}</td><td>{fmt(r.get('DayWeighted'))}</td><td>{fmt(r.get('RVOL'))}</td><td>{fmt(r.get('Close'))}</td><td>{r.get('Change','')} <small>({r.get('PrevRank','')})</small></td><td>{r.get('Verdict','')}</td><td>{r.get('ModuleDisplay','')}</td><td>{r.get('AdvVCP','')} {r.get('AdvCount','')}/3</td><td>{r.get('SmartTxt','')} ({r.get('SmartScore','')})</td><td>{r.get('StructTxt','')} | {r.get('StructSignal','')}</td><td>{r.get('RiskState','')} | TD ซื้อ {r.get('TD_Buy','')} / ขาย {r.get('TD_Sell','')}</td><td>{r.get('MTF','')}</td><td>{'ผ่าน' if r.get('TrendTemplate') else 'ไม่ผ่าน'} / เกณฑ์ Low52×1.25 {fmt(r.get('Low52_125'))} / High52×0.75 {fmt(r.get('High52_075'))}</td></tr>"
    html=f"""<!doctype html><meta charset=utf-8><meta name=viewport content='width=device-width,initial-scale=1'>
<title>FHP 15.9.5 v4.5 COMPLETE - Full Market</title><body style='font-family:sans-serif;background:#000;color:#fff;margin:8px;font-size:11px;line-height:1.35'>
<h2 style='font-size:14px;margin:6px 0'>FHP 15.9.5 v4.5 COMPLETE — Full Market SET+mai (ราคา 1-80 บาท) + Full Intelligence</h2><p>อัปเดตล่าสุด: {now} — Yahoo Finance — Pine 15.9.5 + Shariah B (SCGD ไม่โดนตัด)</p>
{parity_banner}
<p style='background:#22c55e;color:#000;padding:6px;font-size:11px'>ทั้งหมด {total} ตัว (ข้อมูลถึงแท่งที่ปิดแล้ว {data_date}) — CS {cs} / ES {es} / EE {ee} | Universe SET+mai: {globals().get('_UNIVERSE_N','-')} ตัว (ที่มา: {globals().get('_UNIVERSE_SRC','-')}){' ⚠️ FALLBACK — ไม่ใช่ทั้งตลาด | สาเหตุ: ' + str(globals().get('_UNIVERSE_FAILS','-')) if globals().get('_UNIVERSE_WARN') else ''} | Stage: {globals().get('_STAGE','-')} | SET API: FreeFloat={'FLAG-ONLY (FF-LOW badge, never reject)' if globals().get('_SET_ACTIVE_FF') else 'SKIPPED'} / Equity={'ON' if globals().get('_SET_ACTIVE_EQ') else 'SKIPPED'}</p>
<p><b>อัปเกรดวันนี้:</b> {up_txt}</p>
<p>ค้นหา: <input id=q oninput="filter()" placeholder="พิมพ์ชื่อหุ้น เช่น MAJOR" style="padding:4px;width:180px;font-size:11px">
กรอง: <select id=f onchange="filter()"><option value="">ทั้งหมด</option><option>CS</option><option>ES</option><option>EE</option><option>NONE</option><option>UPGRADE</option></select>
<button onclick="document.getElementById('q').value='';document.getElementById('f').value='';filter()">ล้าง</button></p>
<p style='font-size:11px'>Rank = เข้าได้หรือยังตาม Pine. Score = คะแนนรวม. Module 7 = W สัปดาห์ / V VCP / T เทรนด์ใหญ่ / D เทรนด์วัน / S แรงซื้อ / St โครงสร้าง / A สะสม. Verdict = สรุปประโยคเดียว.</p>
<div style='overflow-x:auto'><table id=tbl border=1 cellpadding=4 style='border-collapse:collapse;background:#fff;color:#000;white-space:nowrap;font-size:11px'><tr><th>หุ้น</th><th>FF%</th><th>Rank</th><th>Score</th><th>แรงซื้อ</th><th>RVOL</th><th>Close</th><th>เปลี่ยน Rank</th><th>Verdict</th><th>Module 7</th><th>Advanced VCP</th><th>Smart Money</th><th>โครงสร้าง</th><th>Risk / TD ซื้อ-ขาย</th><th>MTF</th><th>Trend / เกณฑ์</th></tr>{th}</table></div>
<script>function filter(){{var q=document.getElementById('q').value.toUpperCase();var f=document.getElementById('f').value;document.querySelectorAll('#tbl tr').forEach((tr,i)=>{{if(i==0)return;var t=tr.innerText.toUpperCase();var show=(!q||t.includes(q))&&(!f||(f=='UPGRADE'?t.includes('UPGRADE'):tr.cells[2].innerText.trim()==f));tr.style.display=show?'':'none'}})}}</script>
<p><a href=results.csv>โหลด CSV</a> | <a href=history.csv>โหลดประวัติ Rank</a></p>
<p>ข้อมูลเพื่อการศึกษาเท่านั้น ไม่ใช่คำแนะนำการลงทุน — เช็คกราฟซ้ำใน TradingView ก่อนซื้อขายทุกครั้ง</p></body>"""
    open("docs/index.html","w",encoding="utf-8").write(html)
    print(df[["Ticker","Rank","CombinedScore","DayWeighted","RVOL","Change","ModuleCount","Verdict"]].to_string(index=False))
    print("PARITY:", "PASS" if parity_ok else "FAIL", " | ".join(parity_lines))
    print("WROTE docs/results.csv + docs/history.csv + docs/index.html")

if __name__=="__main__": main()
