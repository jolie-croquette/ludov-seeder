import requests
from requests.auth import HTTPBasicAuth
from datetime import datetime, timedelta, time as dtime
import time
import re
try:
    from zoneinfo import ZoneInfo, ZoneInfoNotFoundError
except ImportError:
    ZoneInfo = None
    class ZoneInfoNotFoundError(Exception): ...
    pass

import db
import sys, os, shutil, subprocess, json

CONFIG = db.get_config()

APP_VERSION = "2.0"

BASE_URL = "https://ludov.inlibro.net/api/v1"
ENDPOINT = "/biblios"
USERNAME = CONFIG["API_USERNAME"]
PASSWORD = CONFIG["API_PASSWORD"]

PER_PAGE = 999999
CHECK_DATE = False

ALL_BIBLIOS = []

# URLs Ludov pour mapping plateforme
LUDOV_CONSOLES_URL = "https://www.ludov.ca/koha/consoles/catalogue_source_consoles.json"
LUDOV_JEUX_URL = "https://www.ludov.ca/koha/jeux/catalogue_source_jeux_access.json"


# Credentials IGDB
TWITCH_CLIENT_ID = CONFIG["TWITCH_CLIENT_ID"]
TWITCH_CLIENT_SECRET = CONFIG["TWITCH_CLIENT_SECRET"]

# Mapping console -> IGDB Platform ID
PLATFORM_NAME_TO_IGDB = {
    "Sony PlayStation": 7,
    "Sony PlayStation 2": 8,
    "Sony PlayStation 3": 9,
    "Sony PlayStation 4": 48,
    "Sony PlayStation 5": 167,
    "PlayStation One": 7,
    "Sony PlayStation Portable": 38,
    "Sony PlayStation Vita": 46,
    "Sony PlayStation Classic": 7,
    "Xbox": 11,
    "Xbox 360": 12,
    "Xbox One": 49,
    "Xbox Series X": 169,
    "Nintendo Entertainment System": 18,
    "Famicom": 99,
    "Super Nintendo Entertainment System": 19,
    "Super Famicom": 58,
    "Nintendo 64": 4,
    "GameCube": 21,
    "Wii": 5,
    "Wii U": 41,
    "Nintendo Switch": 130,
    "Game Boy": 33,
    "Game Boy Color": 22,
    "Game Boy Advance": 24,
    "Game Boy Advance SP": 24,
    "Nintendo DS Lite": 20,
    "Nintendo DSi": 20,
    "Nintendo 3DS": 37,
    "Nintendo 2DS": 37,
    "Virtual Boy": 87,
    "Sega Master System": 64,
    "Sega Genesis": 29,
    "Sega Saturn": 32,
    "Sega Saturn (Japon)": 32,
    "Sega Dreamcast": 23,
    "Sega Game Gear": 35,
    "Sega CD [Model 2][NTSC]": 78,
    "Sega CD 32X": 78,
    "Sega 32X": 30,
    "Atari 2600/VCS": 59,
    "Atari 5200": 66,
    "Atari 7800": 60,
    "Atari Jaguar": 62,
    "Atari Lynx": 61,
    "Steam": 6,
    "Windows 95": 6,
    "Windows 98": 6,
    "Windows XP": 6,
    "Windows 7": 6,
    "Windows 10": 6,
    "Ordinateur": 6,
    "GOG Galaxy": 6,
    "Epic Games Launcher": 6,
    "EA Origin: Windows 10": 6,
    "itch.io": 6,
    "Macintosh": 14,
    "Macintosh Plus": 14,
}

print("""
=========================================
   LUDOV SEEDER v2.0
   Générateur de données pour LUDOV
   + Intégration IGDB
=========================================
""")

# ============================================
# Classes IGDB
# ============================================

class IGDBClient:
    """Client pour récupérer les covers depuis IGDB"""
    
    def __init__(self, client_id: str, client_secret: str):
        self.client_id = client_id
        self.client_secret = client_secret
        self.access_token = None
        self.token_expiry = 0
        
    def get_access_token(self):
        if self.access_token and time.time() < self.token_expiry:
            return self.access_token
        
        print("Obtention token Twitch OAuth...")
        url = "https://id.twitch.tv/oauth2/token"
        params = {
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "grant_type": "client_credentials"
        }
        
        resp = requests.post(url, params=params, timeout=10)
        data = resp.json()
        
        self.access_token = data["access_token"]
        self.token_expiry = time.time() + data["expires_in"] - 300
        
        return self.access_token
    
    def search_game_cover(self, game_title: str, platform_id: int = None):
        """Recherche une cover sur IGDB"""
        token = self.get_access_token()
        
        headers = {
            "Client-ID": self.client_id,
            "Authorization": f"Bearer {token}",
        }
        
        # Nettoyage titre
        clean_title = self.clean_game_title(game_title)
        
        # Requête IGDB
        if platform_id:
            query = f'''
            search "{clean_title}";
            fields name, cover.image_id;
            where platforms = ({platform_id});
            limit 1;
            '''
        else:
            query = f'''
            search "{clean_title}";
            fields name, cover.image_id;
            limit 1;
            '''
        
        try:
            resp = requests.post(
                "https://api.igdb.com/v4/games",
                headers=headers,
                data=query,
                timeout=10
            )
            
            if resp.status_code == 200 and resp.json():
                game_data = resp.json()[0]
                if "cover" in game_data and "image_id" in game_data["cover"]:
                    image_id = game_data["cover"]["image_id"]
                    return f"https://images.igdb.com/igdb/image/upload/t_cover_big/{image_id}.jpg"
        except:
            pass
        
        return None
    
    @staticmethod
    def clean_game_title(title: str) -> str:
        """Nettoie le titre du jeu pour améliorer le matching IGDB"""
        # Retirer les mentions de copie
        title = re.sub(r'\s*[\(\[]copie\s*\d*[\)\]]', '', title, flags=re.IGNORECASE)
        
        # Retirer les mentions VF/VO
        title = re.sub(r'\s*[\(\[](VF|VO|VOSTFR)[\)\]]', '', title, flags=re.IGNORECASE)
        
        # Retirer les mentions [copie 2], (copie 2), etc.
        title = re.sub(r'\s*[\(\[]copie\s+\d+[\)\]]', '', title, flags=re.IGNORECASE)
        
        # Retirer les caractères spéciaux problématiques
        title = title.replace('"', '').replace("'", "")
        
        # Retirer espaces multiples
        title = re.sub(r'\s+', ' ', title)
        
        return title.strip()

def load_ludov_platform_mapping():
    """Charge le mapping biblio_id -> plateforme depuis Ludov"""
    print("\nChargement mapping plateformes Ludov...")
    
    try:
        # Fetch consoles
        resp_consoles = requests.get(LUDOV_CONSOLES_URL, timeout=30)
        consoles_data = resp_consoles.json()
        console_map = {c["id"]: c["console"] for c in consoles_data}
        
        # Fetch jeux
        resp_jeux = requests.get(LUDOV_JEUX_URL, timeout=30)
        jeux_data = resp_jeux.json()
        
        # Créer mapping biblio_id -> (console_name, igdb_id, koha_console_id)
        platform_mapping = {}
        for jeu in jeux_data:
            biblio_id = jeu.get("id")
            plateforme_id = jeu.get("plateforme", "")
            
            if biblio_id and plateforme_id:
                console_name = console_map.get(plateforme_id)
                if console_name:
                    igdb_id = PLATFORM_NAME_TO_IGDB.get(console_name)
                    platform_mapping[biblio_id] = {
                        "console": console_name,
                        "igdb_id": igdb_id,
                        "koha_console_id": plateforme_id
                    }
        
        print(f">>> {len(platform_mapping)} jeux avec plateforme charges")
        return platform_mapping
        
    except Exception as e:
        print(f"ERREUR chargement Ludov: {e}")
        return {}

def ensure_igdb_columns(conn):
    """S'assure que les colonnes nécessaires pour IGDB existent"""
    cursor = conn.cursor()
    
    print("\nVerification des colonnes IGDB...")
    
    # Vérifie si les colonnes existent
    cursor.execute("""
        SELECT COUNT(*) 
        FROM INFORMATION_SCHEMA.COLUMNS 
        WHERE TABLE_SCHEMA = DATABASE()
        AND TABLE_NAME = 'games' 
        AND COLUMN_NAME = 'platform'
    """)
    
    exists = cursor.fetchone()[0] > 0
    
    if not exists:
        print("Ajout des colonnes platform...")
        try:
            cursor.execute("""
                ALTER TABLE games 
                ADD COLUMN platform VARCHAR(100) NULL AFTER titre,
                ADD COLUMN platform_id INT NULL AFTER platform,
                ADD COLUMN console_koha_id VARCHAR(20) NULL AFTER platform_id
            """)
            conn.commit()
            print(">>> Colonnes ajoutees avec succes")
        except Exception as e:
            print(f"ERREUR ajout colonnes: {e}")
    else:
        print(">>> Colonnes deja presentes")
    
    cursor.close()

# ============================================
# Fonctions principales
# ============================================

def main():
    conn = db.create_connection()
    if conn is None:
        return

    try:
        db.ensure_database(conn)
        db.use_database(conn)
        
        # OPTION: Wipe ou non
        print("\n" + "="*50)
        print("OPTION: Souhaitez-vous vider completement la base de donnees?")
        print("="*50)
        print("y = Oui, vider et reconstruire (SUPPRIME TOUT)")
        print("n = Non, conserver les donnees existantes")
        wipe_choice = input("\nVotre choix (y/n): ").lower().strip()
        
        if wipe_choice == 'y':
            db.preview_wipe(conn)
            input("\nLa BD sera videe. Appuyez sur Entree pour confirmer...")
            db.confirm_and_wipe(conn)
            
            print("\n=== Import du SQL embarque ===")
            db.run_embedded_sql(conn)
            print(">>> Schema importe avec succes")
            
            # S'assurer que les colonnes IGDB existent
            ensure_igdb_columns(conn)
            
            # Charger le mapping des plateformes
            platform_mapping = load_ludov_platform_mapping()
            
            fetch_all_biblios()
            fetch_console(conn)
            seed_games_from_koha_without_covers(conn, platform_mapping)
            fetch_accessoires(conn)
            seed_reservations(conn)
            
            # Après le seed, proposer de fetch les covers
            print("\n" + "="*50)
            print("SEED TERMINE - Les jeux ont ete importes avec plateformes")
            print("="*50)
            print("Souhaitez-vous maintenant fetcher les covers depuis IGDB?")
            fetch_covers_choice = input("Votre choix (y/n): ").lower().strip()
            
            if fetch_covers_choice == 'y':
                update_game_covers(conn, platform_mapping, fetch_all=True)
        else:
            print("\n>>> Conservation des donnees existantes")
            
            # S'assurer que les colonnes IGDB existent
            ensure_igdb_columns(conn)
            
            # Charger le mapping des plateformes
            platform_mapping = load_ludov_platform_mapping()
            
            # OPTION: Fetch toutes les covers ou seulement les manquantes
            print("\n" + "="*50)
            print("OPTION: Quelles covers souhaitez-vous fetcher?")
            print("="*50)
            print("1 = Toutes les covers (remplace les existantes)")
            print("2 = Uniquement les covers manquantes")
            cover_choice = input("\nVotre choix (1/2): ").strip()
            
            fetch_all = (cover_choice == '1')
            
            update_game_covers(conn, platform_mapping, fetch_all)
            
    finally:
        try:
            if conn.is_connected():
                conn.close()
                print("\nConnexion fermee proprement.")
        except NameError:
            pass

def get_toronto_tz():
    if ZoneInfo:
        try:
            return ZoneInfo("America/Toronto")
        except ZoneInfoNotFoundError:
            pass
    return datetime.now().astimezone().tzinfo

TIMEZONE = get_toronto_tz()

def window_for_today_5am_toronto():
    now_local = datetime.now(TIMEZONE)
    today_local = now_local.date()
    start = datetime.combine(today_local - timedelta(days=1), dtime(5, 0), tzinfo=TIMEZONE)
    end   = datetime.combine(today_local, dtime(5, 0), tzinfo=TIMEZONE)
    return start, end

def iso_to_toronto(iso_str: str) -> datetime:
    try:
        dt = datetime.fromisoformat(iso_str.replace("Z", "+00:00"))
    except ValueError:
        try:
            dt = datetime.strptime(iso_str[:19], "%Y-%m-%dT%H:%M:%S")
            dt = dt.replace(tzinfo=ZoneInfo("UTC") if ZoneInfo else None)
        except Exception:
            return datetime.now(TIMEZONE)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=ZoneInfo("UTC") if ZoneInfo else None)
    return dt.astimezone(TIMEZONE)

def fetch_all_biblios():
    global ALL_BIBLIOS
    page = 1
    ALL_BIBLIOS = []
    total = 0
    print("\n=== TELECHARGEMENT DES DONNEES KOHA ===")

    while True:
        try:
            batch = fetch_biblios_page(page)
        except Exception as e:
            print(f"Erreur reseau/API page {page}: {e}")
            break

        if not batch:
            break

        ALL_BIBLIOS.extend(batch)
        total += len(batch)

        if len(batch) < PER_PAGE:
            break
        page += 1

    print(f">>> Donnees Koha telechargees : {len(ALL_BIBLIOS)} enregistrements")
    return ALL_BIBLIOS

def fetch_biblios_page(page: int):
    url = f"{BASE_URL}{ENDPOINT}"
    headers = {
        "Accept": "application/json",
        "Accept-Encoding": "gzip",
        "User-Agent": "LUDOVSeeder/2.0",
    }
    params = {"_page": page, "_per_page": PER_PAGE}
    resp = requests.get(
        url,
        auth=HTTPBasicAuth(USERNAME, PASSWORD),
        headers=headers,
        params=params,
        timeout=60
    )
    resp.raise_for_status()
    return resp.json()

def seed_games_from_koha_without_covers(conn, platform_mapping):
    """Seed initial depuis Koha SANS fetch IGDB mais AVEC infos plateforme"""
    print("\n=== SEED JEUX KOHA (avec plateformes, sans covers): demarrage ===")
    
    type_map = db.get_console_type_id_map(conn)

    to_upsert = []
    stats = {"total": 0, "with_platform": 0, "without_platform": 0, "with_type": 0}

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
        if not titre: 
            continue

        author = (b.get("author") or None)
        available = 1

        # Récupérer les infos de plateforme depuis le mapping Ludov
        platform_name = None
        platform_id = None
        console_koha_id = None
        console_type_id = None
        
        platform_info = platform_mapping.get(str(biblio_id))
        if platform_info:
            stats["with_platform"] += 1
            platform_name = platform_info.get("console")
            platform_id = platform_info.get("igdb_id")
            console_koha_id = platform_info.get("koha_console_id")
            key = (platform_name or "").strip().lower()
            console_type_id = type_map.get(key)
            if console_type_id:
                stats["with_type"] += 1
        else:
            stats["without_platform"] += 1

        to_upsert.append((
            gid,                    # biblio_id
            titre,                  # titre
            author,                 # author
            platform_name,          # platform
            int(platform_id) if platform_id is not None else None,       # platform_id
            int(console_koha_id) if console_koha_id is not None else None, # console_koha_id
            int(console_type_id) if console_type_id is not None else None, # console_type_id
            ts_local.strftime("%Y-%m-%d %H:%M:%S"),                       # createdAt
        ))

        stats["total"] += 1
        
        # Affichage progression simple
        if stats["total"] % 100 == 0:
            print(f"... {stats['total']} jeux traités "
                  f"({stats['with_platform']} avec plateforme, {stats['with_type']} avec type)")
    if not to_upsert:
        print("Aucun jeu a inserer.")
        return 
    
    # Stats finales
    print(f"\n{'='*60}")
    print("STATISTIQUES SEED KOHA")
    print(f"{'='*60}")
    print(f"Total jeux importés       : {stats['total']}")
    print(f"Jeux avec plateforme      : {stats['with_platform']}")
    print(f"Jeux mappés à un type     : {stats['with_type']}")
    print(f"Jeux sans plateforme      : {stats['without_platform']}")
    
    db.insertGameIntoDatabase(conn, to_upsert)

def update_game_covers(conn, platform_mapping, fetch_all=False):
    """Met à jour UNIQUEMENT les covers des jeux existants (ne touche pas aux plateformes)"""
    print("\n=== MISE A JOUR DES COVERS IGDB ===")
    
    # Initialiser client IGDB
    try:
        igdb_client = IGDBClient(TWITCH_CLIENT_ID, TWITCH_CLIENT_SECRET)
        print(">>> Client IGDB initialise")
    except Exception as e:
        print(f"ERREUR: Impossible d'initialiser IGDB: {e}")
        return
    
    cursor = conn.cursor(dictionary=True)
    
    # Sélectionner les jeux à traiter
    if fetch_all:
        query = "SELECT id, titre, biblio_id, platform_id FROM games WHERE platform_id IS NOT NULL"
        print("Mode: Toutes les covers seront fetchees (jeux avec plateforme uniquement)")
    else:
        query = """
            SELECT id, titre, biblio_id, platform_id 
            FROM games 
            WHERE platform_id IS NOT NULL 
            AND (picture IS NULL OR picture = '' OR picture = '/placeholder_games.jpg')
        """
        print("Mode: Uniquement les covers manquantes (jeux avec plateforme uniquement)")
    
    cursor.execute(query)
    games = cursor.fetchall()
    total = len(games)
    
    print(f"\n>>> {total} jeux a traiter")
    
    if total == 0:
        print("Aucun jeu a traiter!")
        cursor.close()
        return
    
    stats = {"processed": 0, "found": 0, "failed": 0}
    failed_games = []
    start_time = time.time()
    
    for idx, game in enumerate(games, 1):
        game_id = game['id']
        titre = game['titre']
        platform_id = game['platform_id']
        
        # Calcul progression
        elapsed = time.time() - start_time
        rate = idx / elapsed if elapsed > 0 else 0
        remaining = total - idx
        eta = remaining / rate if rate > 0 else 0
        
        print(f"\n[{idx}/{total}] {titre[:50]}")
        print(f"    Stats: {stats['found']} trouvees / {stats['failed']} manquees")
        print(f"    Vitesse: {rate:.1f} jeux/sec | ETA: {eta/60:.1f} min")
        
        try:
            cover_url = igdb_client.search_game_cover(titre, platform_id)
            
            if cover_url:
                # Met à jour UNIQUEMENT la cover (pas les infos de plateforme)
                cursor.execute("""
                    UPDATE games 
                    SET picture = %s,
                        lastUpdatedAt = NOW()
                    WHERE id = %s
                """, (cover_url, game_id))
                conn.commit()
                
                stats["found"] += 1
                print(f"    >>> Cover trouvee")
            else:
                stats["failed"] += 1
                failed_games.append({
                    "titre": titre,
                    "biblio_id": game.get('biblio_id')
                })
                print(f"    XXX Pas de cover")
            
            stats["processed"] += 1
            time.sleep(0.26)
            
        except Exception as e:
            stats["failed"] += 1
            failed_games.append({
                "titre": titre,
                "erreur": str(e)
            })
            print(f"    ERREUR: {e}")
    
    cursor.close()
    
    # Stats finales
    total_time = time.time() - start_time
    print(f"\n{'='*60}")
    print("STATISTIQUES FINALES")
    print(f"{'='*60}")
    print(f"Jeux traites: {stats['processed']}")
    print(f"Covers trouvees: {stats['found']}")
    print(f"Covers manquantes: {stats['failed']}")
    print(f"Taux de succes: {stats['found']/stats['processed']*100:.1f}%" if stats['processed'] > 0 else "N/A")
    print(f"Temps total: {total_time/60:.1f} min")
    
    # Afficher les jeux sans cover
    if failed_games:
        print(f"\n{'='*60}")
        print(f"JEUX SANS COVER ({len(failed_games)})")
        print(f"{'='*60}")
        for game in failed_games[:50]:
            raison = game.get('erreur', '')
            print(f"- {game['titre'][:60]}" + (f" - {raison}" if raison else ""))
        
        if len(failed_games) > 50:
            print(f"\n... et {len(failed_games) - 50} autres jeux")

def fetch_console(conn):
    print("\n=== SEED CONSOLES: demarrage ===")
    url = f"{BASE_URL}{ENDPOINT}"
    headers = {
        "Accept": "application/json",
        "Accept-Encoding": "gzip",
        "User-Agent": "LUDOVSeeder/2.0",
    }
    params = {"_per_page": PER_PAGE, "q" : json.dumps({"item_type": "CONSOLE"})}
    resp = requests.get(
        url,
        auth=HTTPBasicAuth(USERNAME, PASSWORD),
        headers=headers,
        params=params,
        timeout=60
    )
    resp.raise_for_status()

    consoles = resp.json()

    db.insert_console(conn, consoles)
    return consoles
    

def seed_console_types(conn, consoles):
    print("\n=== SEED TYPES DE CONSOLES: demarrage ===")

    # Extraire les noms de consoles et éliminer les doublons
    console_names = []
    seen_names = set()
    
    for console in consoles:
        name = console.get("title") or console.get("name", "")
        if name and name not in seen_names:
            console_names.append(name)  # Ajouter directement le nom (string)
            seen_names.add(name)
    
    print(f">>> {len(console_names)} types de consoles uniques trouvés")
    
    if console_names:
        db.insert_console_types(conn, console_names)

def fetch_accessoires(conn): 
    print("\n=== SEED ACCESSOIRES: demarrage ===")
    url = "https://www.ludov.ca/koha/access/catalogue_source_access.json"
    headers = {"Accept": "application/json"}
    resp = requests.get(url, headers=headers, timeout=60)
    resp.raise_for_status()

    accessoires_data = []
    for accessoire in resp.json():
        raw_console = accessoire.get("console")
        consoles = [int(c.strip()) for c in raw_console.split(";") if c.strip().isdigit()] if raw_console else []
        accessoires_data.append({
            "koha_id": int(accessoire.get("id")),
            "name": accessoire.get("accessoire"),
            "console": json.dumps(consoles, ensure_ascii=False)
        })

    db.insert_accessoires(conn, accessoires_data)

def seed_reservations(conn):
    reservations = []

if __name__ == "__main__":
    main()