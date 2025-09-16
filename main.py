import requests
from requests.auth import HTTPBasicAuth
from datetime import datetime, timedelta, time
try:
    from zoneinfo import ZoneInfo, ZoneInfoNotFoundError  # Python 3.9+
except ImportError:
    ZoneInfo = None
    class ZoneInfoNotFoundError(Exception): ...
    pass

import db
import sys, os, shutil, subprocess, json

APP_VERSION = "1.0.0"
LATEST_URL = "https://raw.githubusercontent.com/jolie-croquette/ludov-seeder/refs/heads/main/latest.json"

BASE_URL = "https://ludov.inlibro.net/api/v1"
ENDPOINT = "/biblios"       # ou "/items"
USERNAME = "apicatalogue"
PASSWORD = "apicatalogue"

PER_PAGE = 10000            # pagination serveur
CHECK_DATE = False          # True => ne prendre que la fenêtre 5h-5h d'aujourd'hui (Toronto)

ALL_BIBLIOS = []

print("""
=========================================
   LUDOV SEEDER
   Générateur de données pour LUDOV
   Auteur : jolie-croquette
   Date : 16/09/2025
=========================================
""")

def check_for_update():
    try:
        resp = requests.get(LATEST_URL, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        latest_version = data["version"]
        download_url = data["url"]

        if latest_version != APP_VERSION:
            print(f"🔄 Nouvelle version {latest_version} trouvée (actuelle {APP_VERSION})")
            update_app(download_url)
        else:
            print("✅ Application à jour")
    except Exception as e:
        print(f"⚠ Impossible de vérifier les mises à jour : {e}")

def update_app(download_url):
    exe_path = sys.argv[0]
    new_path = exe_path + ".new"

    print("⬇ Téléchargement de la mise à jour...")
    with requests.get(download_url, stream=True) as r:
        r.raise_for_status()
        with open(new_path, "wb") as f:
            shutil.copyfileobj(r.raw, f)

    print("🔄 Remplacement de l'exécutable...")
    os.replace(new_path, exe_path)

    print("🚀 Redémarrage...")
    subprocess.Popen([exe_path] + sys.argv[1:])
    sys.exit(0)

def main():
    conn = db.create_connection()
    if conn is None:
        return

    try:
        db.ensure_database(conn)
        db.use_database(conn)
        db.preview_wipe(conn)

        input(f"La BD sera vidée. Appuyez sur Entrée pour confirmer...")
        db.confirm_and_wipe(conn)

        print("\n=== Import du SQL embarqué ===")
        db.run_embedded_sql(conn)
        print("✅ Schéma importé avec succès.")

        fetch_all_biblios()

        seed_games_from_koha(conn)
        seed_console_from_game(conn)
        seed_users(conn)
        seed_reservations(conn)
    finally:
        try:
            if conn.is_connected():
                conn.close()
                print("Connexion fermée proprement.")
        except NameError:
            pass

def get_toronto_tz():
    if ZoneInfo:
        try:
            return ZoneInfo("America/Toronto")
        except ZoneInfoNotFoundError:
            pass
    # fallback: fuseau local
    return datetime.now().astimezone().tzinfo

TIMEZONE = get_toronto_tz()

def window_for_today_5am_toronto():
    now_local = datetime.now(TIMEZONE)
    today_local = now_local.date()
    start = datetime.combine(today_local - timedelta(days=1), time(5, 0), tzinfo=TIMEZONE)
    end   = datetime.combine(today_local, time(5, 0), tzinfo=TIMEZONE)
    return start, end

def iso_to_toronto(iso_str: str) -> datetime:
    # Accepte 'YYYY-MM-DDTHH:MM:SS[.fff][+tz]'
    try:
        dt = datetime.fromisoformat(iso_str.replace("Z", "+00:00"))
    except ValueError:
        # fallback très permissif
        try:
            dt = datetime.strptime(iso_str[:19], "%Y-%m-%dT%H:%M:%S")
            dt = dt.replace(tzinfo=ZoneInfo("UTC") if ZoneInfo else None)
        except Exception:
            return datetime.now(TIMEZONE)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=ZoneInfo("UTC") if ZoneInfo else None)
    return dt.astimezone(TIMEZONE)

def fetch_all_biblios():
    """Récupère toutes les biblios en une seule passe et les stocke dans ALL_BIBLIOS"""
    global ALL_BIBLIOS
    page = 1
    ALL_BIBLIOS = []
    total = 0
    print("=== TÉLÉCHARGEMENT DES DONNÉES KOHA ===")

    while True:
        try:
            batch = fetch_biblios_page(page)
        except Exception as e:
            print(f"Erreur réseau/API page {page}: {e}")
            break

        if not batch:
            break

        ALL_BIBLIOS.extend(batch)
        total += len(batch)

        if len(batch) < PER_PAGE:
            break
        page += 1

    print(f"📦 Données Koha téléchargées : {len(ALL_BIBLIOS)} enregistrements.")
    return ALL_BIBLIOS

def fetch_biblios_page(page: int):
    url = f"{BASE_URL}{ENDPOINT}"
    headers = {
        "Accept": "application/json",
        "Accept-Encoding": "gzip",
        "User-Agent": "LUDOVSeeder/1.0",
    }
    params = {"_page": page, "_per_page": PER_PAGE}  # <-- correction
    resp = requests.get(
        url,
        auth=HTTPBasicAuth(USERNAME, PASSWORD),
        headers=headers,
        params=params,
        timeout=60
    )
    resp.raise_for_status()
    return resp.json()

def seed_games_from_koha(conn):
    print("\n=== SEED JEUX KOHA: démarrage ===")
    to_upsert = []

    for b in ALL_BIBLIOS:
        if b.get("item_type") != "JEU":
            continue

        ts_str = b.get("timestamp")
        if not ts_str:
            continue
        ts_local = iso_to_toronto(ts_str)

        biblio_id = b.get("biblio_id")
        if not biblio_id:
            continue
        try:
            gid = int(biblio_id)
        except Exception:
            continue

        title = (b.get("title") or "").strip()
        subtitle = (b.get("subtitle") or "").strip()
        titre = f"{title} - {subtitle}" if subtitle else title

        author = (b.get("author") or None)
        picture = ""  
        available = 1

        to_upsert.append((
            gid, titre, gid, author, picture, available,
            ts_local.strftime("%Y-%m-%d %H:%M:%S"),
        ))

    if not to_upsert:
        print("Aucun jeu à insérer.")
        return
    
    db.insertGameIntoDatabase(conn, to_upsert)



def seed_console_from_game(conn):
    to_upsert = []

    for b in ALL_BIBLIOS:
        if b.get("item_type") != "JEU":
            continue
        
        name = (b.get("edition_statement") or None)
        if not name:
            continue

        to_upsert.append((name,))

    if not to_upsert:
        print("Aucune console à insérer.")
        return
    
    db.insert_console(conn, to_upsert)



def seed_users(conn):
    user = [
        
    ]

def seed_reservations(conn):
    reservations = []

if __name__ == "__main__":
    check_for_update()
    main()
