#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
مبدل و جمع‌آوری خودکار کانفیگ‌های V2Ray به Clash
نسخه بهینه‌شده با سرعت بالا، لیمیت استاندارد و گروه‌بندی هوشمند
"""

import re
import json
import base64
import socket
import time
import logging
from urllib.parse import urlparse, parse_qs
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Dict, Optional

import requests
import yaml

# ========================== تنظیمات ==========================
CONFIG = {
    "INPUT_URLS": [
        "https://raw.githubusercontent.com/igareck/vpn-configs-for-russia/refs/heads/main/Vless-Reality-White-Lists-Rus-Mobile.txt",
        "https://raw.githubusercontent.com/igareck/vpn-configs-for-russia/refs/heads/main/Vless-Reality-White-Lists-Rus-Mobile-2.txt",
        "https://raw.githubusercontent.com/igareck/vpn-configs-for-russia/refs/heads/main/BLACK_VLESS_RUS_mobile.txt",
        "https://raw.githubusercontent.com/igareck/vpn-configs-for-russia/refs/heads/main/WHITE-CIDR-RU-checked.txt",
        "https://raw.githubusercontent.com/igareck/vpn-configs-for-russia/refs/heads/main/BLACK_VLESS_RUS.txt",
        "https://raw.githubusercontent.com/igareck/vpn-configs-for-russia/refs/heads/main/BLACK_SS+All_RUS.txt",
        "https://raw.githubusercontent.com/Mosifree/-FREE2CONFIG/refs/heads/main/FRAGMENT",
        "https://raw.githubusercontent.com/ShadowException/VPN/refs/heads/main/configs/VPN-cat",
        "https://raw.githubusercontent.com/F0rc3Run/F0rc3Run/main/splitted-by-protocol/vless.txt",
        "https://raw.githubusercontent.com/barry-far/V2ray-config/main/Sub1.txt",
        "https://raw.githubusercontent.com/barry-far/V2ray-Config/main/Sub2.txt",
        "https://raw.githubusercontent.com/barry-far/V2ray-Config/main/Sub3.txt",
        "https://raw.githubusercontent.com/ebrasha/free-v2ray-public-list/refs/heads/main/V2Ray-Config-By-EbraSha.txt",
        "https://raw.githubusercontent.com/MohammadBahemmat/V2ray-Collector/main/subscriptions/all.txt",
        "https://raw.githubusercontent.com/ALIILAPRO/v2rayNG-Config/main/sub.txt",
        "https://raw.githubusercontent.com/Pawdroid/Free-servers/main/sub",
        "https://raw.githubusercontent.com/mfuu/v2ray/master/v2ray.txt",
        "https://raw.githubusercontent.com/ermaozi/get_subscribe/main/subscribe/v2ray.txt",
        "https://mifa.world/ss",
        "https://empty-mouse-fbb7.alizareh4024.workers.dev/sync?sub=%D8%B3%D9%88%D8%B3%D9%85%D8%A7%D8%B1%F0%9F%A6%8E",
        "https://raw.githubusercontent.com/pytimusprime/FreeV2ray/refs/heads/main/all_servers.txt",
        "https://raw.githubusercontent.com/ThomasJasperthecat/sub/main/sublist1.txt",
        "https://raw.githubusercontent.com/masir-sefid/Sub/main/@Masir_Sefid.txt",
        "https://sub.whitedns.one/sub/mihomo.yaml",
        "http://main.pythash.tr/FRkh99yBGCllN/01736620-2086-4c0b-a86e-52ebfe64dd12/#pythash",
        "https://raw.githubusercontent.com/masir-sefid/Sub/main/Telegram-Channel-@Masir_Sefid.txt",
        "https://c6et83fe1u99lr8j5w4s9iwik9565bqx.pages.dev/sub/fragment/g4lWgI*%40zehfoOEK?app=xray#%F0%9F%92%A6%20BPB%20Fragment",
        "https://raw.githubusercontent.com/AmyraxVPN-Main/AmyraxVPN/refs/heads/main/AmyraxVPN.txt",
        "https://raw.githubusercontent.com/arshiacomplus/v2rayExtractor/refs/heads/main/mix/sub.html",
        "https://raw.githubusercontent.com/MahsaNetConfigTopic/config/refs/heads/main/xray_final.txt",
        "https://raw.githubusercontent.com/barry-far/V2ray-config/main/All_Configs_base64_Sub.txt",
        "https://v2.alicivil.workers.dev"
    ],
    "FETCH_TIMEOUT": 7,          # ثانیه برای دریافت هر URL
    "FETCH_WORKERS": 15,         # دانلود همزمان لینک‌ها
    "TCP_TIMEOUT": 2,            # کاهش تایم‌اوت به ۲ ثانیه برای عبور سریع از نودهای مرده
    "TCP_WORKERS": 200,          # افزایش شدید تردها برای تست فوق سریع پینگ
    "MAX_OUTPUT_NODES": 150,     # تعداد استاندارد و نهایی نودها در خروجی
    "OUTPUT_FILE": "Tehronstore.yaml",
    "PROXY_NAME_PREFIX": "Tehronstore",
}

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# ========================== ابزارهای کمکی ==========================

def fetch_text(url: str) -> Optional[str]:
    try:
        resp = requests.get(url, timeout=CONFIG["FETCH_TIMEOUT"])
        if resp.status_code == 200:
            return resp.text
    except Exception:
        pass
    return None

def extract_links_from_text(text: str) -> List[str]:
    links = []
    if not any(proto in text for proto in ["vmess://", "vless://", "trojan://", "ss://"]):
        try:
            decoded = base64.b64decode(text.strip()).decode('utf-8', errors='ignore')
            if any(proto in decoded for proto in ["vmess://", "vless://", "trojan://", "ss://"]):
                text = decoded
        except Exception:
            pass
    pattern = r'(vmess://[^\s]+|vless://[^\s]+|trojan://[^\s]+|ss://[^\s]+)'
    raw_links = re.findall(pattern, text)
    for link in raw_links:
        link = link.strip()
        if link and len(link) > 10:
            links.append(link)
    return links

# توابع Parse (بدون تغییر نسبت به نسخه اصلی، به دلیل عملکرد صحیح)
def parse_proxy(link: str) -> Optional[Dict]:
    if link.startswith("vmess://"): return parse_vmess(link)
    elif link.startswith("vless://"): return parse_vless(link)
    elif link.startswith("trojan://"): return parse_trojan(link)
    elif link.startswith("ss://"): return parse_ss(link)
    return None

def parse_vmess(link: str) -> Optional[Dict]:
    try:
        b64 = link.replace("vmess://", "")
        b64 += "=" * (4 - len(b64) % 4)
        data = json.loads(base64.b64decode(b64).decode('utf-8'))
        return {"type": "vmess", "server": data.get("add") or data.get("host", ""), "port": int(data.get("port", 0)), "uuid": data.get("id") or data.get("uuid", ""), "alterId": int(data.get("aid") or data.get("alterId", 0)), "cipher": data.get("scy") or data.get("security", "auto"), "tls": data.get("tls") == "tls" or data.get("security") == "tls", "skip-cert-verify": data.get("allowInsecure") == "true" or data.get("skip-cert-verify") == "true", "network": data.get("net") or data.get("type", "tcp"), "ws-path": data.get("path", ""), "ws-host": data.get("host", ""), "sni": data.get("sni") or data.get("host", "")}
    except: return None

def parse_vless(link: str) -> Optional[Dict]:
    try:
        parsed = urlparse(link)
        params = parse_qs(parsed.query)
        return {"type": "vless", "server": parsed.hostname, "port": parsed.port or 443, "uuid": parsed.username or "", "cipher": params.get("encryption", ["none"])[0], "tls": params.get("security", [""])[0] in ["tls", "reality"], "skip-cert-verify": params.get("allowInsecure", ["0"])[0] == "1" or params.get("skip-cert-verify", ["false"])[0] == "true", "network": params.get("type", ["tcp"])[0], "ws-path": params.get("path", [""])[0], "ws-host": params.get("host", [""])[0], "sni": params.get("sni", [""])[0] or parsed.hostname, "flow": params.get("flow", [""])[0]}
    except: return None

def parse_trojan(link: str) -> Optional[Dict]:
    try:
        parsed = urlparse(link)
        params = parse_qs(parsed.query)
        return {"type": "trojan", "server": parsed.hostname, "port": parsed.port or 443, "password": parsed.username or "", "sni": params.get("sni", [parsed.hostname])[0], "skip-cert-verify": params.get("allowInsecure", ["0"])[0] == "1" or params.get("skip-cert-verify", ["false"])[0] == "true", "tls": True, "network": "tcp"}
    except: return None

def parse_ss(link: str) -> Optional[Dict]:
    try:
        raw = link.replace("ss://", "")
        if "@" in raw:
            before, after = raw.split("@", 1)
            try:
                method, password = base64.b64decode(before).decode('utf-8').split(":", 1)
            except:
                method, password = before.split(":", 1)
            server, port = after.split("#", 1)[0].split(":", 1) if "#" in after else after.split(":", 1)
        else:
            method, password = base64.b64decode(raw).decode('utf-8').split("@", 1)[0].split(":", 1)
            server, port = base64.b64decode(raw).decode('utf-8').split("@", 1)[1].split(":", 1)
        return {"type": "ss", "server": server, "port": int(port), "method": method, "password": password, "cipher": method}
    except: return None

# ========================== تست TCP ==========================

def tcp_ping(proxy: Dict) -> Optional[float]:
    server, port = proxy.get("server"), proxy.get("port")
    if not server or not port: return None
    try:
        start = time.time()
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(CONFIG["TCP_TIMEOUT"])
        sock.connect((server, int(port)))
        sock.close()
        return (time.time() - start) * 1000
    except Exception:
        return None

def test_proxies(proxies: List[Dict]) -> List[Dict]:
    logger.info(f"شروع تست TCP روی {len(proxies)} نود منحصر‌به‌فرد...")
    results = []
    with ThreadPoolExecutor(max_workers=CONFIG["TCP_WORKERS"]) as executor:
        future_to_proxy = {executor.submit(tcp_ping, p): p for p in proxies}
        for future in as_completed(future_to_proxy):
            proxy = future_to_proxy[future]
            ping = future.result()
            if ping is not None:
                proxy["ping"] = round(ping, 2)
                results.append(proxy)
    results.sort(key=lambda x: x["ping"])
    return results[:CONFIG["MAX_OUTPUT_NODES"]] # اعمال سقف خروجی

# ========================== تولید YAML Clash ==========================

def build_clash_config(proxies: List[Dict]) -> str:
    for idx, p in enumerate(proxies, start=1):
        p["name"] = f"☬ {CONFIG['PROXY_NAME_PREFIX']}-{idx:02d} | {int(p['ping'])}ms"

    clash_proxies = []
    for p in proxies:
        entry = {"name": p["name"], "type": p["type"], "server": p["server"], "port": p["port"]}
        if p["type"] == "vmess":
            entry.update({"uuid": p.get("uuid", ""), "alterId": p.get("alterId", 0), "cipher": p.get("cipher", "auto"), "tls": p.get("tls", False), "skip-cert-verify": p.get("skip-cert-verify", False), "network": p.get("network", "tcp")})
            if p.get("network") == "ws": entry["ws-opts"] = {"path": p.get("ws-path", "/"), "headers": {"Host": p.get("ws-host", "")}}
            if p.get("sni"): entry["sni"] = p["sni"]
        elif p["type"] == "vless":
            entry.update({"uuid": p.get("uuid", ""), "cipher": p.get("cipher", "none"), "tls": p.get("tls", False), "skip-cert-verify": p.get("skip-cert-verify", False), "network": p.get("network", "tcp")})
            if p.get("network") == "ws": entry["ws-opts"] = {"path": p.get("ws-path", "/"), "headers": {"Host": p.get("ws-host", "")}}
            if p.get("sni"): entry["sni"] = p["sni"]
            if p.get("flow"): entry["flow"] = p["flow"]
        elif p["type"] == "trojan":
            entry.update({"password": p.get("password", ""), "sni": p.get("sni", p["server"]), "skip-cert-verify": p.get("skip-cert-verify", False), "network": "tcp"})
        elif p["type"] == "ss":
            entry.update({"password": p.get("password", ""), "cipher": p.get("cipher", "")})
        clash_proxies.append(entry)

    proxy_names = [p["name"] for p in proxies]
    config = {
        "port": 7890,
        "socks-port": 7891,
        "allow-lan": True,
        "mode": "rule",
        "log-level": "info",
        "proxies": clash_proxies,
        "proxy-groups": [
            {
                "name": "Tehronstore-Select",
                "type": "select",
                "proxies": ["Auto-Tehronstore"] + proxy_names
            },
            {
                "name": "Auto-Tehronstore",
                "type": "url-test",
                "url": "http://cp.cloudflare.com/generate_204",
                "interval": 300,
                "tolerance": 50,
                "proxies": proxy_names
            }
        ],
        "rules": [
            "MATCH,Tehronstore-Select"
        ]
    }
    return yaml.dump(config, allow_unicode=True, sort_keys=False)

# ========================== اصلی ==========================

def main():
    logger.info("شروع پردازش موازی سورس‌ها...")
    all_links = []
    
    # دریافت موازی لینک‌ها (افزایش چشمگیر سرعت)
    with ThreadPoolExecutor(max_workers=CONFIG["FETCH_WORKERS"]) as executor:
        future_to_url = {executor.submit(fetch_text, url): url for url in CONFIG["INPUT_URLS"]}
        for future in as_completed(future_to_url):
            content = future.result()
            if content:
                all_links.extend(extract_links_from_text(content))

    logger.info(f"تعداد کل لینک‌های استخراج شده: {len(all_links)}")

    # فیلتر تکراری‌ها بر اساس IP و Port (نه صرفاً رشته متنی)
    unique_proxies = {}
    for link in all_links:
        p = parse_proxy(link)
        if p and p.get("server") and p.get("port"):
            key = f"{p['server']}:{p['port']}"
            if key not in unique_proxies:
                unique_proxies[key] = p
                
    proxies_list = list(unique_proxies.values())
    logger.info(f"نودهای تکراری حذف شدند. آماده برای تست {len(proxies_list)} نود.")

    if not proxies_list:
        logger.error("هیچ پراکسی معتبری یافت نشد.")
        return

    # تست TCP روی نودهای فیلترشده
    active_proxies = test_proxies(proxies_list)

    if not active_proxies:
        logger.error("هیچ پراکسی فعالی پس از تست باقی نماند.")
        return

    # تولید و ذخیره YAML نهایی
    yaml_content = build_clash_config(active_proxies)
    with open(CONFIG["OUTPUT_FILE"], "w", encoding="utf-8") as f:
        f.write(yaml_content)
        
    logger.info(f"عملیات موفق! فایل {CONFIG['OUTPUT_FILE']} با {len(active_proxies)} پراکسی سبک و استاندارد ذخیره شد.")

if __name__ == "__main__":
    main()
