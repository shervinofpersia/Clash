#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
مبدل و جمع‌آوری خودکار کانفیگ‌های V2Ray به Clash
با تست TCP و حذف تکراری و مرتب‌سازی بر اساس پینگ
"""

import re
import json
import base64
import socket
import time
import logging
from urllib.parse import urlparse, parse_qs
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Dict, Optional, Tuple

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
        "https://raw.githubusercontent.com/Mosifree/-FREE2CONFIG/refs/heads/main/FRAGMENT",
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
    "TIMEOUT": 10,           # ثانیه برای دریافت هر URL
    "TCP_TIMEOUT": 3,        # ثانیه برای تست TCP
    "MAX_WORKERS": 20,       # تعداد نخ‌های همزمان برای تست TCP
    "OUTPUT_FILE": "Tehronstore.yaml",
    "PROXY_NAME_PREFIX": "Tehronstore",  # نام همه پراکسی‌ها با این پیشوند + شماره
}

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


# ========================== ابزارهای کمکی ==========================

def fetch_text(url: str) -> Optional[str]:
    """دریافت محتوای متنی از یک URL با timeout مشخص"""
    try:
        resp = requests.get(url, timeout=CONFIG["TIMEOUT"])
        if resp.status_code == 200:
            return resp.text
        else:
            logger.warning(f"HTTP {resp.status_code} برای {url}")
            return None
    except Exception as e:
        logger.warning(f"خطا در دریافت {url}: {e}")
        return None


def extract_links_from_text(text: str) -> List[str]:
    """
    استخراج تمام لینک‌های پروتکل‌های vmess://, vless://, trojan://, ss://
    همچنین سعی در دیکد کردن Base64 در صورت وجود
    """
    links = []
    # ابتدا بررسی می‌کنیم که آیا کل متن Base64 است یا خیر
    # (اگر متن شامل لینک نباشد و فقط شامل کاراکترهای Base64 باشد)
    if not any(proto in text for proto in ["vmess://", "vless://", "trojan://", "ss://"]):
        try:
            # بررسی می‌کنیم که آیا می‌توان دیکد کرد و نتیجه شامل لینک است
            decoded = base64.b64decode(text.strip()).decode('utf-8', errors='ignore')
            if any(proto in decoded for proto in ["vmess://", "vless://", "trojan://", "ss://"]):
                text = decoded
        except Exception:
            pass  # متن Base64 نیست

    # استخراج با regex
    pattern = r'(vmess://[^\s]+|vless://[^\s]+|trojan://[^\s]+|ss://[^\s]+)'
    raw_links = re.findall(pattern, text)
    # پاکسازی whitespace
    for link in raw_links:
        link = link.strip()
        if link and len(link) > 10:
            links.append(link)
    return links


def parse_proxy(link: str) -> Optional[Dict]:
    """
    تبدیل لینک به دیکشنری اطلاعات پراکسی با فیلدهای:
    - type, server, port, name, uuid, password, cipher, tls, skip-cert-verify, network, ...
    """
    if link.startswith("vmess://"):
        return parse_vmess(link)
    elif link.startswith("vless://"):
        return parse_vless(link)
    elif link.startswith("trojan://"):
        return parse_trojan(link)
    elif link.startswith("ss://"):
        return parse_ss(link)
    else:
        return None


def parse_vmess(link: str) -> Optional[Dict]:
    try:
        b64 = link.replace("vmess://", "")
        # گاهی اوقات base64 با padding ناقص است
        b64 += "=" * (4 - len(b64) % 4)
        data = json.loads(base64.b64decode(b64).decode('utf-8'))
        return {
            "type": "vmess",
            "server": data.get("add") or data.get("host", ""),
            "port": int(data.get("port", 0)),
            "uuid": data.get("id") or data.get("uuid", ""),
            "alterId": int(data.get("aid") or data.get("alterId", 0)),
            "cipher": data.get("scy") or data.get("security", "auto"),
            "tls": data.get("tls") == "tls" or data.get("security") == "tls",
            "skip-cert-verify": data.get("allowInsecure") == "true" or data.get("skip-cert-verify") == "true",
            "network": data.get("net") or data.get("type", "tcp"),
            "ws-path": data.get("path", ""),
            "ws-host": data.get("host", ""),
            "sni": data.get("sni") or data.get("host", ""),
            "name": data.get("ps") or data.get("name", "vmess-node"),
        }
    except Exception as e:
        logger.debug(f"خطا در parse vmess: {e}")
        return None


def parse_vless(link: str) -> Optional[Dict]:
    try:
        parsed = urlparse(link)
        params = parse_qs(parsed.query)
        name = parsed.fragment or "vless-node"
        return {
            "type": "vless",
            "server": parsed.hostname,
            "port": parsed.port or 443,
            "uuid": parsed.username or "",
            "cipher": params.get("encryption", ["none"])[0],
            "tls": params.get("security", [""])[0] in ["tls", "reality"],
            "skip-cert-verify": params.get("allowInsecure", ["0"])[0] == "1" or params.get("skip-cert-verify", ["false"])[0] == "true",
            "network": params.get("type", ["tcp"])[0],
            "ws-path": params.get("path", [""])[0],
            "ws-host": params.get("host", [""])[0],
            "sni": params.get("sni", [""])[0] or parsed.hostname,
            "flow": params.get("flow", [""])[0],
            "name": name,
        }
    except Exception as e:
        logger.debug(f"خطا در parse vless: {e}")
        return None


def parse_trojan(link: str) -> Optional[Dict]:
    try:
        parsed = urlparse(link)
        params = parse_qs(parsed.query)
        name = parsed.fragment or "trojan-node"
        return {
            "type": "trojan",
            "server": parsed.hostname,
            "port": parsed.port or 443,
            "password": parsed.username or "",
            "sni": params.get("sni", [parsed.hostname])[0],
            "skip-cert-verify": params.get("allowInsecure", ["0"])[0] == "1" or params.get("skip-cert-verify", ["false"])[0] == "true",
            "tls": True,
            "network": "tcp",
            "name": name,
        }
    except Exception as e:
        logger.debug(f"خطا در parse trojan: {e}")
        return None


def parse_ss(link: str) -> Optional[Dict]:
    try:
        # فرمت ss://base64(method:password)@server:port#name
        raw = link.replace("ss://", "")
        if "@" in raw:
            # بخش قبل از @ شامل method:password است
            before, after = raw.split("@", 1)
            # ممکن است before خود base64 باشد
            try:
                before_decoded = base64.b64decode(before).decode('utf-8')
                method, password = before_decoded.split(":", 1)
            except Exception:
                # شاید قبل از @ یک روش قدیمی با فرمت method:password به صورت plain باشد
                method, password = before.split(":", 1)
            # بعد از @: server:port
            if "#" in after:
                server_port, name = after.split("#", 1)
            else:
                server_port, name = after, "ss-node"
            server, port = server_port.split(":", 1)
            return {
                "type": "ss",
                "server": server,
                "port": int(port),
                "method": method,
                "password": password,
                "cipher": method,
                "name": name,
            }
        else:
            # فرمت قدیمی: ss://base64(method:password@server:port)
            decoded = base64.b64decode(raw).decode('utf-8')
            # method:password@server:port
            method_password, server_port = decoded.split("@", 1)
            method, password = method_password.split(":", 1)
            server, port = server_port.split(":", 1)
            return {
                "type": "ss",
                "server": server,
                "port": int(port),
                "method": method,
                "password": password,
                "cipher": method,
                "name": "ss-node",
            }
    except Exception as e:
        logger.debug(f"خطا در parse ss: {e}")
        return None


# ========================== تست TCP ==========================

def tcp_ping(proxy: Dict) -> Optional[float]:
    """بررسی اتصال TCP و برگرداندن زمان پاسخ (میلی‌ثانیه) در صورت موفقیت"""
    server = proxy.get("server")
    port = proxy.get("port")
    if not server or not port:
        return None
    try:
        start = time.time()
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(CONFIG["TCP_TIMEOUT"])
        sock.connect((server, int(port)))
        sock.close()
        elapsed = (time.time() - start) * 1000  # میلی‌ثانیه
        return elapsed
    except Exception:
        return None


def test_proxies(proxies: List[Dict]) -> List[Dict]:
    """تست TCP همزمان روی همه پراکسی‌ها و حذف موارد ناموفق"""
    logger.info(f"شروع تست TCP روی {len(proxies)} پراکسی...")
    results = []
    with ThreadPoolExecutor(max_workers=CONFIG["MAX_WORKERS"]) as executor:
        future_to_proxy = {executor.submit(tcp_ping, p): p for p in proxies}
        for future in as_completed(future_to_proxy):
            proxy = future_to_proxy[future]
            ping = future.result()
            if ping is not None:
                proxy["ping"] = round(ping, 2)
                results.append(proxy)
    logger.info(f"تست کامل شد. {len(results)} پراکسی فعال باقی ماندند.")
    return sorted(results, key=lambda x: x["ping"])


# ========================== تولید YAML Clash ==========================

def build_clash_config(proxies: List[Dict]) -> str:
    """ساخت کانفیگ کامل Clash با proxy-groups و rules"""
    # تغییر نام همه به Tehronstore با شماره
    for idx, p in enumerate(proxies, start=1):
        p["name"] = f"{CONFIG['PROXY_NAME_PREFIX']}-{idx:02d}"

    # ساختار proxies به فرمت مورد نیاز Clash
    clash_proxies = []
    for p in proxies:
        entry = {
            "name": p["name"],
            "type": p["type"],
            "server": p["server"],
            "port": p["port"],
        }
        if p["type"] == "vmess":
            entry["uuid"] = p.get("uuid", "")
            entry["alterId"] = p.get("alterId", 0)
            entry["cipher"] = p.get("cipher", "auto")
            entry["tls"] = p.get("tls", False)
            entry["skip-cert-verify"] = p.get("skip-cert-verify", False)
            entry["network"] = p.get("network", "tcp")
            if p.get("network") == "ws":
                entry["ws-opts"] = {
                    "path": p.get("ws-path", "/"),
                    "headers": {"Host": p.get("ws-host", "")}
                }
            if p.get("sni"):
                entry["sni"] = p["sni"]
        elif p["type"] == "vless":
            entry["uuid"] = p.get("uuid", "")
            entry["cipher"] = p.get("cipher", "none")
            entry["tls"] = p.get("tls", False)
            entry["skip-cert-verify"] = p.get("skip-cert-verify", False)
            entry["network"] = p.get("network", "tcp")
            if p.get("network") == "ws":
                entry["ws-opts"] = {
                    "path": p.get("ws-path", "/"),
                    "headers": {"Host": p.get("ws-host", "")}
                }
            if p.get("sni"):
                entry["sni"] = p["sni"]
            if p.get("flow"):
                entry["flow"] = p["flow"]
        elif p["type"] == "trojan":
            entry["password"] = p.get("password", "")
            entry["sni"] = p.get("sni", p["server"])
            entry["skip-cert-verify"] = p.get("skip-cert-verify", False)
            entry["network"] = "tcp"
        elif p["type"] == "ss":
            entry["password"] = p.get("password", "")
            entry["cipher"] = p.get("cipher", "")
        clash_proxies.append(entry)

    # proxy-groups
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
                "name": "PROXY",
                "type": "select",
                "proxies": proxy_names
            }
        ],
        "rules": [
            "MATCH,PROXY"
        ]
    }
    return yaml.dump(config, allow_unicode=True, sort_keys=False)


# ========================== اصلی ==========================

def main():
    logger.info("شروع پردازش...")
    all_links = []
    # مرحله ۱: دریافت همه لینک‌ها از URL ها
    for url in CONFIG["INPUT_URLS"]:
        logger.info(f"دریافت از {url}")
        content = fetch_text(url)
        if content:
            links = extract_links_from_text(content)
            logger.info(f"  {len(links)} لینک استخراج شد")
            all_links.extend(links)
        else:
            logger.warning(f"  محتوایی دریافت نشد")

    # حذف تکراری‌ها (بر اساس خود لینک)
    unique_links = list(dict.fromkeys(all_links))
    logger.info(f"تعداد کل لینک‌های یکتا: {len(unique_links)}")

    # مرحله ۲: پارس کردن لینک‌ها به اشیاء پراکسی
    proxies = []
    for link in unique_links:
        p = parse_proxy(link)
        if p:
            # اگر name نداشت، یک نام پیش‌فرض بگذار
            if not p.get("name"):
                p["name"] = f"{p['type']}-node"
            proxies.append(p)
    logger.info(f"تعداد پراکسی‌های معتبر: {len(proxies)}")

    if not proxies:
        logger.error("هیچ پراکسی معتبری یافت نشد. خروج.")
        return

    # مرحله ۳: تست TCP و فیلتر کردن
    active_proxies = test_proxies(proxies)

    if not active_proxies:
        logger.error("هیچ پراکسی فعالی پس از تست باقی نماند.")
        return

    # مرحله ۴: تولید YAML
    yaml_content = build_clash_config(active_proxies)

    # مرحله ۵: ذخیره فایل
    with open(CONFIG["OUTPUT_FILE"], "w", encoding="utf-8") as f:
        f.write(yaml_content)
    logger.info(f"فایل {CONFIG['OUTPUT_FILE']} با {len(active_proxies)} پراکسی ذخیره شد.")


if __name__ == "__main__":
    main()
