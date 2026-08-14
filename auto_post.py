import os
import time
import requests
import sqlite3

# جلب الإعدادات من متغيرات هيروكو
BOT_TOKEN = os.environ.get("BOT_TOKEN")
CHANNEL_ID = os.environ.get("CHANNEL_ID")  # معرف القناة مثل @MyChannel
DB_PATH = os.environ.get("DB_PATH", "data/proxybot.db")
# الفترة بالثواني (الافتراضي 3600 ثانية = ساعة)
INTERVAL = int(os.environ.get("AUTO_POST_INTERVAL", 3600))

def get_top_proxies(limit=5):
    """جلب أسرع بروكسيات شغالة من قاعدة البيانات"""
    if not os.path.exists(DB_PATH):
        return []
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        # جلب البروكسيات الحية مرتبة بالأسرع (أقل بنج)
        cursor.execute("""
            SELECT ip, port, protocol, country, ping 
            FROM proxies 
            WHERE status = 'live' 
            ORDER BY ping ASC 
            LIMIT ?
        """, (limit,))
        rows = cursor.fetchall()
        conn.close()
        return rows
    except Exception as e:
        print(f"Database error: {e}")
        return []

def send_to_channel(text):
    """إرسال الرسالة إلى القناة عبر Telegram Bot API مباشرة"""
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": CHANNEL_ID,
        "text": text,
        "parse_mode": "Markdown",
        "disable_web_page_preview": True
    }
    try:
        res = requests.post(url, json=payload, timeout=10)
        return res.json()
    except Exception as e:
        print(f"Error sending message: {e}")
        return None

def main():
    if not BOT_TOKEN or not CHANNEL_ID:
        print("Error: BOT_TOKEN or CHANNEL_ID is not set in Config Vars!")
        return

    print("Auto-poster service started successfully...")
    while True:
        try:
            proxies = get_top_proxies(limit=5)
            if proxies:
                msg = "⚡ **أفضل بروكسيات سريعة وشغالة الآن (Live Proxies):**\n\n"
                for p in proxies:
                    ip, port, proto, country, ping = p
                    msg += f"🔹 `{ip}:{port}` | ⚡ {ping}ms | 🌍 {country or 'Unknown'}\n"
                
                msg += "\n🤖 يتم التحديث والفحص تلقائياً."
                send_to_channel(msg)
                print("Posted new proxies to channel successfully.")
            else:
                print("No live proxies found in database to post.")
        except Exception as e:
            print(f"Error in loop: {e}")

        # انتظار المدة المحددة قبل النشر القادم
        time.sleep(INTERVAL)

if __name__ == "__main__":
    main()
