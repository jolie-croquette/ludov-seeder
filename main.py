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
import marc_in_json_helper as marc
import json

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
            fetch_accessoires(conn)
            fetch_games_from_marc(conn, platform_mapping)
            
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

def fetch_games_from_marc(conn, platform_mapping):
    """
    Importe/maj les JEUX depuis l'API Koha en MARC-in-JSON.
    Utilise en priorité platform_mapping (Ludov), sinon 753$a.
    """
    print("\n=== SEED JEUX (MARC-in-JSON) : démarrage ===")
    url = f"{BASE_URL}{ENDPOINT}"
    page_size = min(PER_PAGE if isinstance(PER_PAGE, int) and PER_PAGE > 0 else 500, 500)
    headers = {
        "Accept": "application/marc-in-json",
        "Accept-Encoding": "gzip",
        "User-Agent": "LUDOVSeeder/2.0",
    }
    params = {"_per_page": page_size, "q": json.dumps({"item_type": "JEU"})}

    type_map = db.get_console_type_id_map(conn)  # {name_lower: id}
    to_upsert = []
    stats = {"total": 0, "mapped_ludov": 0, "mapped_753": 0}

    def resolve_platforms(row):
        """
        Retourne (platform_name, platform_id, console_koha_id, console_type_id).
        Priorité: platform_mapping (Ludov) -> 753$a (première plateforme reconnue).
        """
        biblio_id = str(row["biblio_id"])
        # 1) Mapping Ludov si dispo
        pm = platform_mapping.get(biblio_id)
        if pm:
            name = pm.get("console")
            igdb_id = pm.get("igdb_id")
            koha_console_id = pm.get("koha_console_id")
            ctid = type_map.get((name or "").strip().lower())
            return (name, int(igdb_id) if igdb_id is not None else None,
                    int(koha_console_id) if koha_console_id is not None else None,
                    int(ctid) if ctid is not None else None, True)

        # 2) Sinon 753$a (choisir la première reconnue)
        for candidate in (row.get("platforms") or []):
            name = candidate.strip()
            if not name:
                continue
            igdb_id = PLATFORM_NAME_TO_IGDB.get(name)  # mapping existant
            ctid = type_map.get(name.strip().lower())
            if igdb_id or ctid:
                return (name,
                        int(igdb_id) if igdb_id is not None else None,
                        None,
                        int(ctid) if ctid is not None else None,
                        False)
        return (None, None, None, None, None)

    def iso_005_to_datetime(iso_005):
        # 005 ~ "YYYYMMDDhhmmss.s" -> on tolère, sinon NOW()
        from datetime import datetime
        try:
            core = iso_005.split('.')[0]
            dt = datetime.strptime(core, "%Y%m%d%H%M%S")
            return dt.strftime("%Y-%m-%d %H:%M:%S")
        except Exception:
            # fallback: utiliser fenêtre Toronto déjà définie si tu veux,
            # ici on fait simple:
            from datetime import datetime
            return datetime.now(TIMEZONE).strftime("%Y-%m-%d %H:%M:%S")

    # 1ère page
    resp = requests.get(url, auth=HTTPBasicAuth(USERNAME, PASSWORD), headers=headers, params=params, timeout=60)
    resp.raise_for_status()
    data = resp.json()
    if isinstance(data, dict):
        records = data.get("records") or data.get("items") or data.get("data") or []
        next_url = data.get("next") or (data.get("_links", {}) or {}).get("next")
    else:
        records = data if isinstance(data, list) else []
        next_url = None

    known_acc_ids = db.get_known_accessory_ids(conn)

    def consume(recs):
        added = 0
        for rec in recs:
            row = marc.extract_game_row(rec)
            if not row:
                continue
            platform_name, platform_id, console_koha_id, console_type_id, via_ludov = resolve_platforms(row)
            if via_ludov is True:
                stats["mapped_ludov"] += 1
            elif via_ludov is False:
                stats["mapped_753"] += 1

            req_acc = row.get("required_accessories") or []
            req_acc = [i for i in req_acc if i in known_acc_ids]

            req_acc_json = json.dumps(req_acc) if req_acc else None

            to_upsert.append((
                row["biblio_id"],                # biblio_id
                row["titre"],                    # titre
                row.get("author"),               # author
                platform_name,                   # platform
                platform_id,                     # platform_id
                console_koha_id,                 # console_koha_id
                console_type_id,                 # console_type_id
                req_acc_json,                    # required_accessories
                iso_005_to_datetime(row.get("timestamp") or ""),  # createdAt
            ))
            stats["total"] += 1
        return added

    consume(records)
    page_idx = 1
    while next_url:
        page_idx += 1
        r = requests.get(next_url, auth=HTTPBasicAuth(USERNAME, PASSWORD), headers=headers, timeout=60)
        r.raise_for_status()
        data = r.json()
        if isinstance(data, dict):
            records = data.get("records") or data.get("items") or data.get("data") or []
            next_url = data.get("next") or (data.get("_links", {}) or {}).get("next")
        else:
            records = data if isinstance(data, list) else []
            next_url = None
        consume(records)

    # Fallback pagination _page si next absent
    if not next_url and len(records) == page_size:
        page = 2
        while True:
            params["_page"] = page
            r = requests.get(url, auth=HTTPBasicAuth(USERNAME, PASSWORD), headers=headers, params=params, timeout=60)
            r.raise_for_status()
            data = r.json()
            if isinstance(data, dict):
                page_records = data.get("records") or data.get("items") or data.get("data") or []
            else:
                page_records = data if isinstance(data, list) else []
            if not page_records:
                break
            consume(page_records)
            if len(page_records) < page_size:
                break
            page += 1

    if not to_upsert:
        print("Aucun jeu à insérer (MARC).")
        return

    print(f"\n{'='*60}")
    print("STATISTIQUES SEED JEUX (MARC)")
    print(f"{'='*60}")
    print(f"Total jeux trouvés      : {stats['total']}")
    print(f"Plateforme via Ludov    : {stats['mapped_ludov']}")
    print(f"Plateforme via 753$a    : {stats['mapped_753']}")

    db.insertGameIntoDatabase(conn, to_upsert)
    print("=== SEED JEUX (MARC-in-JSON) : terminé ===")


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

def fetch_accessoires(conn):
    print("\n=== SEED ACCESSOIRES: démarrage ===")
    url = f"{BASE_URL}{ENDPOINT}"

    page_size = min(PER_PAGE if isinstance(PER_PAGE, int) and PER_PAGE > 0 else 500, 500)
    headers = {
        "Accept": "application/marc-in-json",
        "Accept-Encoding": "gzip",
        "User-Agent": "LUDOVSeeder/2.0",
    }
    params = {
        "_per_page": page_size,
        "q": json.dumps({"item_type": "ACCESSOIRE"})
    }

    results, seen_koha = [], set()

    def consume(records_list):
        added = 0
        for rec in records_list:
            row = marc.extract_accessoire_row(rec)
            if not (row.get("name") or row.get("koha_id") or row.get("hidden")):
                continue
            kid = row.get("koha_id")
            if kid:
                try:
                    kid_int = int(kid)
                except Exception:
                    continue
                if kid_int in seen_koha:
                    continue
                seen_koha.add(kid_int)
                row["koha_id"] = kid_int
            results.append(row); added += 1
        return added

    resp = requests.get(url, auth=HTTPBasicAuth(USERNAME, PASSWORD), headers=headers, params=params, timeout=60)
    resp.raise_for_status()
    data = resp.json()
    if isinstance(data, dict):
        records = data.get("records") or data.get("items") or data.get("data") or []
        next_url = data.get("next") or (data.get("_links", {}) or {}).get("next")
    else:
        records = data if isinstance(data, list) else []
        next_url = None

    added = consume(records)
    print(f"Page 1: {len(records)} reçus / {added} ajoutés (cumul: {len(results)})")

    page_idx = 1
    while next_url:
        page_idx += 1
        resp = requests.get(next_url, auth=HTTPBasicAuth(USERNAME, PASSWORD), headers=headers, timeout=60)
        resp.raise_for_status()
        data = resp.json()
        if isinstance(data, dict):
            records = data.get("records") or data.get("items") or data.get("data") or []
            next_url = data.get("next") or (data.get("_links", {}) or {}).get("next")
        else:
            records = data if isinstance(data, list) else []
            next_url = None
        added = consume(records)
        print(f"Page (next) {page_idx}: {len(records)} reçus / {added} ajoutés (cumul: {len(results)})")

    if not next_url and len(records) == page_size:
        page = 2
        while True:
            params["_page"] = page
            resp = requests.get(url, auth=HTTPBasicAuth(USERNAME, PASSWORD), headers=headers, params=params, timeout=60)
            resp.raise_for_status()
            data = resp.json()
            if isinstance(data, dict):
                page_records = data.get("records") or data.get("items") or data.get("data") or []
            else:
                page_records = data if isinstance(data, list) else []
            if not page_records:
                break
            added = consume(page_records)
            print(f"Page {page}: {len(page_records)} reçus / {added} ajoutés (cumul: {len(results)})")
            if len(page_records) < page_size:
                break
            page += 1

    print(f">>> Total accessoires prêts à insérer: {len(results)}")
    if results:
        db.insert_accessoires(conn, results)

    print("=== SEED ACCESSOIRES: terminé ===")
    return results

if __name__ == "__main__":
    main()
