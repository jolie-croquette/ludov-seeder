"""
Script de migration des covers IGDB pour LUDOV
Auteur: Assistant
Version: 1.0

Ce script:
1. Migre la BD pour ajouter le champ 'platform'
2. Parse les titres pour extraire la plateforme
3. Fetch les covers depuis IGDB
4. Met à jour la BD avec les URLs des covers
"""

import requests
import mysql.connector
import time
import re
import json
from typing import Optional, Tuple
from datetime import datetime

# ============================================
# CONFIGURATION
# ============================================

# Credentials IGDB/Twitch
TWITCH_CLIENT_ID = "9a5e4s9f3lz6cgl8qckzqx8yarh6f9"  # À obtenir sur dev.twitch.tv
TWITCH_CLIENT_SECRET = "61urowny486bhlzu6nuh8obmdy8c5z"

# Config DB (depuis votre config.json)
with open("config.json", "r") as f:
    config = json.load(f)

DB_CONFIG = {
    "host": config["DB_HOST"],
    "port": config["DB_PORT"],
    "user": config["DB_USER"],
    "password": config["DB_PASSWORD"],
    "database": config["DB_NAME"],
    "auth_plugin": 'mysql_native_password'
}

# Mapping plateformes : nom détecté -> ID IGDB
PLATFORM_MAP = {
    "playstation 4": 48,
    "ps4": 48,
    "playstation 5": 167,
    "ps5": 167,
    "xbox one": 49,
    "xbox series": 169,
    "nintendo switch": 130,
    "switch": 130,
    "pc": 6,
    "playstation 3": 9,
    "ps3": 9,
    "xbox 360": 12,
    "wii": 5,
    "wii u": 41,
}

# Rate limiting
RATE_LIMIT_DELAY = 0.26  # 4 req/sec = 0.25s entre chaque, on prend 0.26 pour marge

# ============================================
# CLASSES
# ============================================

class IGDBClient:
    """Client pour l'API IGDB"""
    
    def __init__(self, client_id: str, client_secret: str):
        self.client_id = client_id
        self.client_secret = client_secret
        self.access_token = None
        self.token_expiry = 0
        
    def get_access_token(self) -> str:
        """Obtient ou refresh le token OAuth"""
        if self.access_token and time.time() < self.token_expiry:
            return self.access_token
        
        print("🔑 Obtention du token Twitch OAuth...")
        url = "https://id.twitch.tv/oauth2/token"
        params = {
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "grant_type": "client_credentials"
        }
        
        try:
            resp = requests.post(url, params=params, timeout=10)
            resp.raise_for_status()
            data = resp.json()
            
            self.access_token = data["access_token"]
            # Expire 5min avant pour sécurité
            self.token_expiry = time.time() + data["expires_in"] - 300
            
            print(f"✅ Token obtenu, expire dans {data['expires_in']} secondes")
            return self.access_token
            
        except Exception as e:
            raise Exception(f"Erreur obtention token: {e}")
    
    def search_game_cover(self, game_title: str, platform_id: Optional[int] = None) -> Optional[str]:
        """
        Recherche la cover d'un jeu sur IGDB
        
        Args:
            game_title: Nom du jeu
            platform_id: ID de la plateforme IGDB (optionnel)
            
        Returns:
            URL de la cover en haute résolution ou None
        """
        token = self.get_access_token()
        
        headers = {
            "Client-ID": self.client_id,
            "Authorization": f"Bearer {token}",
            "Accept": "application/json"
        }
        
        # Nettoyage du titre pour améliorer le matching
        clean_title = self._clean_title(game_title)
        
        # Construction de la requête IGDB
        if platform_id:
            # Recherche avec plateforme spécifique
            query = f'''
            search "{clean_title}";
            fields name, cover.url, cover.image_id;
            where platforms = ({platform_id});
            limit 1;
            '''
        else:
            # Recherche sans plateforme (prend la plus populaire)
            query = f'''
            search "{clean_title}";
            fields name, cover.url, cover.image_id;
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
                    # URL haute résolution (cover_big = 264x374)
                    cover_url = f"https://images.igdb.com/igdb/image/upload/t_cover_big/{image_id}.jpg"
                    return cover_url
            
            return None
            
        except Exception as e:
            print(f"    ⚠️ Erreur API IGDB: {e}")
            return None
    
    @staticmethod
    def _clean_title(title: str) -> str:
        """Nettoie le titre pour améliorer le matching"""
        # Retire les mentions de plateforme
        title = re.sub(r'\s*-\s*(PlayStation|PS\d|Xbox|Switch|Nintendo|PC).*$', '', title, flags=re.IGNORECASE)
        # Retire les caractères spéciaux problématiques
        title = title.replace('"', '').replace("'", "").strip()
        return title


class PlatformExtractor:
    """Extrait la plateforme depuis le titre du jeu"""
    
    @staticmethod
    def extract_platform(title: str, subtitle: str = "") -> Tuple[Optional[str], Optional[int]]:
        """
        Extrait la plateforme du titre/sous-titre
        
        Returns:
            (nom_plateforme, igdb_id) ou (None, None)
        """
        full_text = f"{title} {subtitle}".lower()
        
        # Patterns de détection
        patterns = [
            (r'\bplaystation\s*5\b|\bps5\b', "PlayStation 5", 167),
            (r'\bplaystation\s*4\b|\bps4\b', "PlayStation 4", 48),
            (r'\bplaystation\s*3\b|\bps3\b', "PlayStation 3", 9),
            (r'\bxbox\s*series\b', "Xbox Series", 169),
            (r'\bxbox\s*one\b', "Xbox One", 49),
            (r'\bxbox\s*360\b', "Xbox 360", 12),
            (r'\bnintendo\s*switch\b|\bswitch\b', "Nintendo Switch", 130),
            (r'\bwii\s*u\b', "Wii U", 41),
            (r'\bwii\b(?!\s*u)', "Wii", 5),
        ]
        
        for pattern, name, igdb_id in patterns:
            if re.search(pattern, full_text):
                return name, igdb_id
        
        return None, None


# ============================================
# MIGRATION BASE DE DONNÉES
# ============================================

def migrate_database(conn):
    """Ajoute le champ 'platform' à la table games si nécessaire"""
    cursor = conn.cursor()
    
    print("\n📊 Vérification du schéma de la base...")
    
    # Vérifie si la colonne existe
    cursor.execute("""
        SELECT COUNT(*) 
        FROM INFORMATION_SCHEMA.COLUMNS 
        WHERE TABLE_SCHEMA = %s 
        AND TABLE_NAME = 'games' 
        AND COLUMN_NAME = 'platform'
    """, (DB_CONFIG["database"],))
    
    exists = cursor.fetchone()[0] > 0
    
    if not exists:
        print("➕ Ajout de la colonne 'platform' à la table games...")
        cursor.execute("""
            ALTER TABLE games 
            ADD COLUMN platform VARCHAR(50) NULL AFTER titre,
            ADD COLUMN platform_id INT NULL AFTER platform
        """)
        conn.commit()
        print("✅ Migration effectuée")
    else:
        print("✅ Colonne 'platform' déjà présente")
    
    cursor.close()


# ============================================
# EXTRACTION DES PLATEFORMES
# ============================================

def extract_platforms_from_titles(conn):
    """Parse les titres existants pour extraire les plateformes"""
    cursor = conn.cursor(dictionary=True)
    extractor = PlatformExtractor()
    
    print("\n🔍 Extraction des plateformes depuis les titres...")
    
    cursor.execute("""
        SELECT id, titre, author 
        FROM games 
        WHERE platform IS NULL
    """)
    
    games = cursor.fetchall()
    updated = 0
    
    for game in games:
        title = game['titre'] or ""
        subtitle = game['author'] or ""  # Parfois le subtitle est dans author
        
        platform_name, platform_id = extractor.extract_platform(title, subtitle)
        
        if platform_name:
            cursor.execute("""
                UPDATE games 
                SET platform = %s, platform_id = %s 
                WHERE id = %s
            """, (platform_name, platform_id, game['id']))
            updated += 1
    
    conn.commit()
    cursor.close()
    
    print(f"✅ {updated} jeux mis à jour avec leur plateforme")
    
    # Statistiques
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM games WHERE platform IS NOT NULL")
    with_platform = cursor.fetchone()[0]
    cursor.execute("SELECT COUNT(*) FROM games")
    total = cursor.fetchone()[0]
    cursor.close()
    
    print(f"📊 Résultat: {with_platform}/{total} jeux ont une plateforme détectée")


# ============================================
# FETCH DES COVERS
# ============================================

def fetch_covers(conn, batch_size: int = 100, max_games: Optional[int] = None):
    """
    Fetch les covers depuis IGDB pour tous les jeux
    
    Args:
        conn: Connexion MySQL
        batch_size: Nombre de jeux à traiter par batch
        max_games: Limite max de jeux (pour tests)
    """
    client = IGDBClient(TWITCH_CLIENT_ID, TWITCH_CLIENT_SECRET)
    cursor = conn.cursor(dictionary=True)
    
    print("\n🎮 Début du fetch des covers IGDB...")
    
    # Sélectionne les jeux sans cover
    query = """
        SELECT id, titre, platform, platform_id
        FROM games 
        WHERE picture IS NULL OR picture = '' OR picture = '/placeholder_games.jpg'
    """
    
    if max_games:
        query += f" LIMIT {max_games}"
    
    cursor.execute(query)
    games = cursor.fetchall()
    total = len(games)
    
    print(f"📦 {total} jeux à traiter")
    
    stats = {
        "processed": 0,
        "found": 0,
        "not_found": 0,
        "errors": 0
    }
    
    start_time = time.time()
    
    for i, game in enumerate(games, 1):
        game_id = game['id']
        titre = game['titre']
        platform_id = game['platform_id']
        
        # Affichage progression
        elapsed = time.time() - start_time
        games_per_sec = i / elapsed if elapsed > 0 else 0
        eta = (total - i) / games_per_sec if games_per_sec > 0 else 0
        
        print(f"\n[{i}/{total}] {titre}")
        print(f"    ⏱️  {games_per_sec:.1f} jeux/sec | ETA: {eta/60:.1f}min")
        
        try:
            # Recherche cover
            cover_url = client.search_game_cover(titre, platform_id)
            
            if cover_url:
                # Update BD
                cursor.execute("""
                    UPDATE games 
                    SET picture = %s, lastUpdatedAt = NOW()
                    WHERE id = %s
                """, (cover_url, game_id))
                conn.commit()
                
                stats["found"] += 1
                print(f"    ✅ Cover trouvée: {cover_url}")
            else:
                stats["not_found"] += 1
                print(f"    ❌ Aucune cover trouvée")
            
            stats["processed"] += 1
            
        except Exception as e:
            stats["errors"] += 1
            print(f"    ⚠️ Erreur: {e}")
        
        # Rate limiting
        time.sleep(RATE_LIMIT_DELAY)
        
        # Checkpoint tous les 50 jeux
        if i % 50 == 0:
            print(f"\n💾 Checkpoint: {stats}")
    
    cursor.close()
    
    # Rapport final
    print("\n" + "="*50)
    print("📊 RAPPORT FINAL")
    print("="*50)
    print(f"Jeux traités:     {stats['processed']}")
    print(f"Covers trouvées:  {stats['found']} ({stats['found']/stats['processed']*100:.1f}%)")
    print(f"Non trouvées:     {stats['not_found']}")
    print(f"Erreurs:          {stats['errors']}")
    print(f"Temps total:      {(time.time() - start_time)/60:.1f} min")
    print("="*50)


# ============================================
# FONCTION PRINCIPALE
# ============================================

def main():
    print("""
    ╔══════════════════════════════════════════╗
    ║   LUDOV - IGDB Cover Fetcher            ║
    ║   Migration des images de jeux          ║
    ╚══════════════════════════════════════════╝
    """)
    
    # Vérification credentials
    if TWITCH_CLIENT_ID == "VOTRE_CLIENT_ID":
        print("❌ ERREUR: Configurez vos credentials IGDB dans le script!")
        print("   1. Allez sur https://dev.twitch.tv/")
        print("   2. Créez une application")
        print("   3. Copiez Client ID et Client Secret dans ce script")
        return
    
    try:
        # Connexion DB
        print("\n🔌 Connexion à la base de données...")
        conn = mysql.connector.connect(**DB_CONFIG)
        print("✅ Connecté")
        
        # Étape 1: Migration
        migrate_database(conn)
        
        # Étape 2: Extraction plateformes
        extract_platforms_from_titles(conn)
        
        # Étape 3: Confirmation utilisateur
        print("\n" + "⚠️ "*20)
        print("ATTENTION: Le fetch complet peut prendre plusieurs heures!")
        print("Estimation: ~7300 jeux × 0.3sec = ~36 minutes")
        print("⚠️ "*20)
        
        choice = input("\nCommencer le fetch? (y/n): ").lower()
        
        if choice != 'y':
            print("❌ Annulé par l'utilisateur")
            return
        
        # Étape 4: Fetch covers
        # Pour test: max_games=50
        # Pour prod: max_games=None
        fetch_covers(conn, batch_size=100, max_games=50)
        
        print("\n✅ Script terminé avec succès!")
        
    except Exception as e:
        print(f"\n❌ Erreur fatale: {e}")
        import traceback
        traceback.print_exc()
        
    finally:
        if 'conn' in locals() and conn.is_connected():
            conn.close()
            print("🔌 Connexion fermée")


if __name__ == "__main__":
    main()