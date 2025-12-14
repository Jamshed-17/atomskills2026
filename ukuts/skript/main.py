import csv
import time
import requests
import sys
from typing import Dict, Optional, Tuple
from datetime import datetime
from urllib.parse import urlencode

# Подключаем цвета
try:
    from colorama import init, Fore, Style
    init(autoreset=True)
except ImportError:
    # Заглушка, если colorama не установлена
    class Fore:
        RED = ""
        GREEN = ""
        YELLOW = ""
        CYAN = ""
        RESET = ""
    class Style:
        RESET_ALL = ""
        BRIGHT = ""

# =======================
# НАСТРОЙКИ
# =======================

BASE_URL = "https://fgis.gost.ru/fundmetrology"
RESULTS_PAGE = f"{BASE_URL}/cm/results"
API_BASE = f"{BASE_URL}/eapi"

INPUT_CSV = "data.csv"
OUTPUT_CSV = "filled_data.csv"

TIMEOUT = 30
DELAY_BETWEEN_REQUESTS = 1.0  # Не более 2 запросов/сек [cite: 94]

# =======================
# ПОМОЩНИКИ
# =======================

def format_date(iso_date: str) -> str:
    """Форматирует ISO дату в ДД.ММ.ГГГГ"""
    if not iso_date:
        return ""
    try:
        dt = datetime.fromisoformat(iso_date.replace("Z", "+00:00"))
        return dt.strftime("%d.%m.%Y")
    except ValueError:
        return iso_date

# =======================
# API И СЕССИЯ
# =======================

def create_session() -> requests.Session:
    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
        "Accept": "application/json, text/plain, */*",
        "Connection": "keep-alive",
    })
    return session

def get_latest_vri(session: requests.Session, mi_number: str) -> Tuple[Optional[Dict], str]:
    """Получает последнюю поверку. Возвращает (Данные, Ссылка)"""
    
    now_str = datetime.now().strftime("%Y-%m-%d")
    
    params = {
        "mi_number": mi_number.strip(),
        "sort": "verification_date desc", 
        "rows": 1,
        "start": 0,
        "verification_date_start": "2000-01-01", 
        "verification_date_end": now_str
    }
    
    api_url = f"{API_BASE}/vri"
    full_url = f"{api_url}?{urlencode(params)}" 

    try:
        r = session.get(
            api_url,
            params=params,
            headers={"X-Requested-With": "XMLHttpRequest"},
            timeout=TIMEOUT
        )
        
        if r.status_code == 429: # Too Many Requests [cite: 94]
            time.sleep(5)
            r = session.get(api_url, params=params, timeout=TIMEOUT)

        if not r.ok:
            return None, full_url

        data = r.json()
        items = data.get("result", {}).get("items", [])
        
        if items:
            return items[0], full_url
        return None, full_url

    except Exception:
        return None, full_url

# =======================
# ОСНОВНАЯ ЛОГИКА
# =======================

def main():
    session = create_session()

    # 1. Читаем входной файл
    try:
        with open(INPUT_CSV, encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            input_rows = list(reader)
            fieldnames = reader.fieldnames
    except FileNotFoundError:
        print(f"{Fore.RED}Файл {INPUT_CSV} не найден!{Style.RESET_ALL}")
        return

    if not input_rows:
        print("Входной CSV пуст")
        return
    
    # Добавляем новые колонки
    target_fields = [
        "vri_id", "org_title", "mit_number", "mit_title", "mit_notation", 
        "mi_modification", "mi_number", "verification_date", "valid_date", 
        "result_docnum", "sticker_num", "applicability"
    ]
    
    for field in target_fields:
        if field not in fieldnames:
            fieldnames.append(field)

    # === СЧЕТЧИКИ СТАТИСТИКИ ===
    stats = {
        "total": len(input_rows),
        "already_filled": 0,  # Было (Оранжевый)
        "found": 0,           # Найдено API (Зеленый)
        "not_found": 0,       # Не найдено API (Красный)
        "skipped": 0          # Пустые номера или ошибки
    }

    print(f"Начинаем обработку {stats['total']} строк. Результат пишется в {OUTPUT_CSV} в реальном времени.\n")

    # 2. Открываем файл на запись
    with open(OUTPUT_CSV, "w", newline="", encoding="utf-8-sig") as f_out:
        writer = csv.DictWriter(f_out, fieldnames=fieldnames)
        writer.writeheader()
        f_out.flush() 

        for idx, row in enumerate(input_rows, start=1):
            mi_number = row.get("номер прибора", "").strip()
            existing_vri = row.get("vri_id", "").strip()

            prefix = f"[{idx}/{stats['total']}] Прибор: {mi_number:<15}"

            # --- СЦЕНАРИЙ 1: УЖЕ ЗАПОЛНЕНО (БЫЛО) ---
            if existing_vri:
                stats["already_filled"] += 1
                print(f"{prefix} {Fore.YELLOW}-> Уже заполнен (ID: {existing_vri}){Style.RESET_ALL}")
                writer.writerow(row)
                f_out.flush()
                continue
            
            # Если номера нет
            if not mi_number:
                stats["skipped"] += 1
                print(f"{prefix} {Fore.YELLOW}-> Нет номера прибора (пропуск){Style.RESET_ALL}")
                writer.writerow(row)
                f_out.flush()
                continue

            # Запрос к API
            vri, request_url = get_latest_vri(session, mi_number)

            # --- СЦЕНАРИЙ 2: НЕ НАЙДЕНО ---
            if not vri:
                stats["not_found"] += 1
                print(f"{prefix} {Fore.RED}-> Не найдено{Style.RESET_ALL}")
                print(f"     Ссылка для проверки: {request_url}")
                writer.writerow(row)
                f_out.flush()
            
            # --- СЦЕНАРИЙ 3: НАЙДЕНО ---
            else:
                stats["found"] += 1
                
                # [cite_start]Заполняем данные [cite: 192, 198]
                row["org_title"] = vri.get("org_title", "")
                row["mit_number"] = vri.get("mit_number", "")
                row["mit_title"] = vri.get("mit_title", "")
                row["mit_notation"] = vri.get("mit_notation", "")
                row["mi_modification"] = vri.get("mi_modification", "")
                row["mi_number"] = vri.get("mi_number", mi_number)

                row["verification_date"] = format_date(vri.get("verification_date", ""))
                row["valid_date"] = format_date(vri.get("valid_date", ""))
                row["result_docnum"] = vri.get("result_docnum", "")
                row["sticker_num"] = vri.get("sticker_num", "") 
                row["applicability"] = "true" if vri.get("applicability") else "false"
                row["vri_id"] = vri.get("vri_id", "")

                # Fallback для ID
                if not row["vri_id"] and "/" in row["result_docnum"]:
                     parts = row["result_docnum"].split("/")
                     if len(parts) >= 3:
                         row["vri_id"] = f"{row.get('arshin', '')}-{parts[2]}"

                print(f"{prefix} {Fore.GREEN}-> Найдено! Дата: {row['verification_date']} (ID: {row['vri_id']}){Style.RESET_ALL}")
                
                writer.writerow(row)
                f_out.flush()

            time.sleep(DELAY_BETWEEN_REQUESTS)

    # === ИТОГОВАЯ СТАТИСТИКА ===
    print("\n" + "="*30)
    print(f"{Style.BRIGHT}ИТОГОВЫЙ ОТЧЕТ:{Style.RESET_ALL}")
    print("="*30)
    print(f"Всего строк:        {stats['total']}")
    print(f"{Fore.YELLOW}Было заполнено:     {stats['already_filled']}{Style.RESET_ALL}")
    print(f"{Fore.GREEN}Найдено API:        {stats['found']}{Style.RESET_ALL}")
    print(f"{Fore.RED}Не найдено:         {stats['not_found']}{Style.RESET_ALL}")
    if stats['skipped'] > 0:
        print(f"Пропущено (пустые): {stats['skipped']}")
    print("="*30)
    print(f"Результат сохранен в {OUTPUT_CSV}")

if __name__ == "__main__":
    main()