import json
import mysql.connector
from mysql.connector import Error
from typing import Any, Dict

FILE_PATH = "config.json"
REQUIRED_KEYS = ["DB_HOST", "DB_PORT", "DB_USER", "DB_PASSWORD", "DB_NAME"]
SQL_SCHEMA = r"""
SET NAMES utf8mb4;
SET FOREIGN_KEY_CHECKS=0;

CREATE TABLE IF NOT EXISTS `users` (
  `id` INT AUTO_INCREMENT NOT NULL UNIQUE,
  `firstname` VARCHAR(50) NOT NULL,
  `lastname` VARCHAR(100) NOT NULL,
  `email` VARCHAR(255) NOT NULL,
  `password` VARCHAR(255),
  `isAdmin` TINYINT NOT NULL,
  `lastUpdatedAt` DATETIME NOT NULL,
  `createdAt` DATETIME NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS `console_type` ( 
    `id` INT AUTO_INCREMENT NOT NULL UNIQUE,
    `name` VARCHAR(255) NOT NULL UNIQUE,
    `picture` LONGTEXT,
    `description` TEXT,
    PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS `console_stock` (
  `id` INT AUTO_INCREMENT NOT NULL UNIQUE,
  `console_type_id` INT NOT NULL,
  `biblio_id` INT NOT NULL,
  `name` VARCHAR(255) NOT NULL,
  `picture` LONGTEXT,
  `is_active` TINYINT NOT NULL DEFAULT 1,
  `holding` TINYINT NOT NULL DEFAULT 0,
  `createdAt` DATETIME NOT NULL DEFAULT NOW(),
  `lastUpdatedAt` DATETIME NOT NULL DEFAULT NOW() ON UPDATE NOW(),
    PRIMARY KEY (`id`),
    FOREIGN KEY (`console_type_id`) REFERENCES `console_type`(`id`) 
        ON UPDATE CASCADE ON DELETE RESTRICT,
    INDEX `idx_console_type` (`console_type_id`),
    INDEX `idx_active` (`is_active`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS `games` (
  `id` INT AUTO_INCREMENT NOT NULL UNIQUE,
  `titre` TEXT NOT NULL,
  `author` TEXT DEFAULT NULL,
  `platform` VARCHAR(255) DEFAULT NULL,
  `console_type_id` INT NULL,
  `platform_id` INT NULL,
  `biblio_id` INT NOT NULL,
  `console_koha_id` INT DEFAULT NULL,
  `picture` LONGTEXT,
  `holding` TINYINT NOT NULL DEFAULT 0,
  `required_accessories` JSON DEFAULT NULL,
  `createdAt` DATETIME NOT NULL,
  `lastUpdatedAt` DATETIME DEFAULT NOW(),
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS `stations` (
  `id` INT AUTO_INCREMENT NOT NULL UNIQUE,
  `consoles` JSON NOT NULL,
  `lastUpdatedAt` DATETIME NOT NULL,
  `createdAt` DATETIME NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS `reservation` (
  `id` VARCHAR(255) NOT NULL UNIQUE,
  `console_id` INT NOT NULL,
  `console_type_id` INT NOT NULL,
  `user_id` INT NOT NULL,
  `game1_id` INT NOT NULL,
  `game2_id` INT NULL,
  `game3_id` INT NULL,
  `accessory_ids` JSON NULL,
  `cours_id` INT NOT NULL,
  `station` INT NULL,
  `date` DATE NOT NULL,
  `time` TIME NOT NULL,
  `archived` TINYINT NOT NULL DEFAULT 0,
  `reminder_enabled` TINYINT(1) NOT NULL DEFAULT 0,
  `reminder_hours_before` INT NULL,
  `reminder_sent` TINYINT(1) NOT NULL DEFAULT 0,
  `reminder_sent_at` DATETIME NULL,
  `createdAt` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `lastUpdatedAt` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  KEY `ix_res_user` (`user_id`),
  KEY `ix_res_console` (`console_id`),
  KEY `ix_res_console_type` (`console_type_id`),
  KEY `ix_res_game1` (`game1_id`),
  KEY `ix_res_game2` (`game2_id`),
  KEY `ix_res_game3` (`game3_id`),
  KEY `ix_res_cours` (`cours_id`),
  KEY `ix_res_station` (`station`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS `accessoires` (
  `id` INT AUTO_INCREMENT UNIQUE NOT NULL,
  `name` TEXT NOT NULL,
  `consoles` JSON NOT NULL,
  `koha_id` INT NOT NULL,
  `hidden` TINYINT NOT NULL DEFAULT 0,
  `lastUpdatedAt` DATETIME NOT NULL,
  `createdAt` DATETIME NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS `reservation_hold` (
  `id` VARCHAR(255) NOT NULL UNIQUE,
  `user_id` INT NOT NULL,
  `console_id` INT NOT NULL,
  `console_type_id` INT NULL,
  `game1_id` INT NULL,
  `game2_id` INT NULL,
  `game3_id` INT NULL,
  `station_id` INT NULL,
  `accessoirs` JSON NULL,
  `cours` INT NULL,
  `date` DATE NULL,
  `time` TIME NULL,
  `expireAt` TIMESTAMP NOT NULL,
  `createdAt` DATETIME NOT NULL,
  PRIMARY KEY (`id`),
  KEY `ix_hold_user` (`user_id`),
  KEY `ix_hold_console` (`console_id`),
  KEY `ix_hold_game1` (`game1_id`),
  KEY `ix_hold_game2` (`game2_id`),
  KEY `ix_hold_game3` (`game3_id`),
  KEY `ix_hold_station` (`station_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS `otp` (
  `id` INT AUTO_INCREMENT PRIMARY KEY,
  `user_id` INT NOT NULL,
  `otp_code` VARCHAR(6) NOT NULL,
  `created_at` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `expires_at` DATETIME NOT NULL,
  `is_used` BOOLEAN NOT NULL DEFAULT FALSE,
  FOREIGN KEY (`user_id`) REFERENCES users(`id`)
);

CREATE TABLE IF NOT EXISTS `cours` (
  `id` INT AUTO_INCREMENT PRIMARY KEY,
  `code_cours` VARCHAR(7) NOT NULL,
  `nom_cours` VARCHAR(255) NOT NULL
);

CREATE TABLE `weekly_availabilities` (
    `weekly_id` INT AUTO_INCREMENT PRIMARY KEY,
    `start_date` DATE NULL,
    `end_date` DATE NULL,
    `day_of_week` VARCHAR(10) NOT NULL,
    `enabled` BOOLEAN NOT NULL,
    `always_available` TINYINT(1) NOT NULL DEFAULT 0
);

CREATE TABLE `specific_dates` (
    `id` INT AUTO_INCREMENT PRIMARY KEY,
    `date` DATE NOT NULL,
    `start_hour` VARCHAR(2) NOT NULL,
    `start_minute` VARCHAR(2) NOT NULL,
    `end_hour` VARCHAR(2) NOT NULL,
    `end_minute` VARCHAR(2) NOT NULL,
    `is_exception` BOOLEAN NOT NULL
);

CREATE TABLE `hour_ranges` (
    `range_id` INT AUTO_INCREMENT PRIMARY KEY,
    `weekly_id` INT NOT NULL,
    `start_hour` VARCHAR(2) NOT NULL,
    `start_minute` VARCHAR(2) NOT NULL,
    `end_hour` VARCHAR(2) NOT NULL,
    `end_minute` VARCHAR(2) NOT NULL,
    FOREIGN KEY (`weekly_id`) REFERENCES `weekly_availabilities`(`weekly_id`) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS `email_logs` (
  `id` INT AUTO_INCREMENT PRIMARY KEY,
  `reservation_id` VARCHAR(255) NOT NULL,
  `email_type` VARCHAR(50) NOT NULL,
  `recipient` VARCHAR(255),
  `status` ENUM('sent', 'failed') NOT NULL,
  `error_message` TEXT,
  `created_at` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  KEY idx_reservation (reservation_id),
  KEY idx_status (status),
  KEY idx_created (created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ============
-- FOREIGN KEYS
-- ============

-- reservation.user_id -> users.id
ALTER TABLE `reservation`
  ADD CONSTRAINT `reservation_fk1`
  FOREIGN KEY (`user_id`) REFERENCES `users`(`id`)
  ON UPDATE CASCADE ON DELETE RESTRICT;

-- reservation.console_id -> console_stock.id
ALTER TABLE `reservation`
  ADD CONSTRAINT `reservation_fk3`
  FOREIGN KEY (`console_id`) REFERENCES `console_stock`(`id`)
  ON UPDATE CASCADE ON DELETE RESTRICT;

-- reservation.game1_id -> games.id
ALTER TABLE `reservation`
    ADD CONSTRAINT `reservation_fk4`
    FOREIGN KEY (`game1_id`) REFERENCES `games`(`id`)
    ON UPDATE CASCADE ON DELETE RESTRICT;

-- reservation.game2_id -> games.id
ALTER TABLE `reservation`
    ADD CONSTRAINT `reservation_fk5`
    FOREIGN KEY (`game2_id`) REFERENCES `games`(`id`)
    ON UPDATE CASCADE ON DELETE RESTRICT;

-- reservation.game3_id -> games.id
ALTER TABLE `reservation`
    ADD CONSTRAINT `reservation_fk6`
    FOREIGN KEY (`game3_id`) REFERENCES `games`(`id`)
    ON UPDATE CASCADE ON DELETE RESTRICT;

-- reservation.cours_id -> cours.id
ALTER TABLE `reservation`
    ADD CONSTRAINT `reservation_fk7`
    FOREIGN KEY (`cours_id`) REFERENCES `cours`(`id`)
    ON UPDATE CASCADE ON DELETE RESTRICT;

-- reservation_hold.user_id -> users.id
ALTER TABLE `reservation_hold`
  ADD CONSTRAINT `reservation_hold_fk1`
  FOREIGN KEY (`user_id`) REFERENCES `users`(`id`)
  ON UPDATE CASCADE ON DELETE RESTRICT;

-- reservation_hold.console_id -> console_stock.id (CORRIGÉ)
ALTER TABLE `reservation_hold`
  ADD CONSTRAINT `reservation_hold_fk2`
  FOREIGN KEY (`console_id`) REFERENCES `console_stock`(`id`)
  ON UPDATE CASCADE ON DELETE RESTRICT;

-- reservation_hold.game1_id -> games.id
ALTER TABLE `reservation_hold`
  ADD CONSTRAINT `reservation_hold_fk3`
  FOREIGN KEY (`game1_id`) REFERENCES `games`(`id`)
  ON UPDATE CASCADE ON DELETE SET NULL;

-- reservation_hold.game2_id -> games.id
ALTER TABLE `reservation_hold`
  ADD CONSTRAINT `reservation_hold_fk4`
  FOREIGN KEY (`game2_id`) REFERENCES `games`(`id`)
  ON UPDATE CASCADE ON DELETE SET NULL;

-- reservation_hold.game3_id -> games.id
ALTER TABLE `reservation_hold`
  ADD CONSTRAINT `reservation_hold_fk5`
  FOREIGN KEY (`game3_id`) REFERENCES `games`(`id`)
  ON UPDATE CASCADE ON DELETE SET NULL;

-- reservation_hold.station_id -> stations.id
ALTER TABLE `reservation_hold`
  ADD CONSTRAINT `reservation_hold_fk6`
  FOREIGN KEY (`station_id`) REFERENCES `stations`(`id`)
  ON UPDATE CASCADE ON DELETE SET NULL;

ALTER TABLE `games`
  ADD CONSTRAINT `games_fk_console_type`
  FOREIGN KEY (`console_type_id`) REFERENCES `console_type`(`id`)
  ON UPDATE CASCADE ON DELETE SET NULL;

ALTER TABLE `reservation_hold`
    ADD CONSTRAINT `reservation_hold_console_type_id_fk`
    FOREIGN KEY (`console_type_id`) REFERENCES `console_type`(`id`)
    ON UPDATE CASCADE;

ALTER TABLE games
  ADD UNIQUE KEY uq_games_biblio (biblio_id),
  ADD INDEX ix_games_console_type (console_type_id);

CREATE INDEX ix_stock_biblio ON console_stock(biblio_id);

ALTER TABLE `accessoires`
  ADD UNIQUE KEY `uq_accessoires_koha` (`koha_id`);

CREATE INDEX idx_reminder_pending
  ON reservation(reminder_enabled, reminder_sent, date, time);

-- ============
-- VIEWS
-- ============

-- Vue pour afficher les consoles disponibles par type
CREATE OR REPLACE VIEW `console_catalog` AS
SELECT 
    ct.id as console_type_id,
    ct.name,
    ct.picture,
    ct.description,
    COUNT(cs.id) as total_units,
    SUM(CASE WHEN cs.is_active = 1 AND cs.holding = 0 THEN 1 ELSE 0 END) as active_units,
    SUM(CASE WHEN cs.is_active = 0 THEN 1 ELSE 0 END) as inactive_units
FROM console_type ct
LEFT JOIN console_stock cs ON ct.id = cs.console_type_id
GROUP BY ct.id, ct.name, ct.picture, ct.description
ORDER BY ct.name;

SET FOREIGN_KEY_CHECKS=1;
"""

SYSTEM_SCHEMAS = {"mysql", "information_schema", "performance_schema", "sys"}

def get_config(path: str = FILE_PATH) -> Dict[str, Any]:
    """Charge et valide le fichier de configuration JSON."""
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    missing = [k for k in REQUIRED_KEYS if not data.get(k)]
    if missing:
        raise KeyError(f"Missing config: {', '.join(missing)}")
    return data

CONFIG = get_config()

def create_connection() -> mysql.connector.MySQLConnection:
    """Crée une connexion MySQL en utilisant les paramètres du fichier config.json."""
    try:
        conn = mysql.connector.connect(
            host=CONFIG["DB_HOST"],
            port=CONFIG["DB_PORT"],
            user=CONFIG["DB_USER"],
            password=CONFIG["DB_PASSWORD"],
            database=CONFIG["DB_NAME"],
            auth_plugin='mysql_native_password'
        )
        if conn.is_connected():
            return conn
        else:
            raise ConnectionError("❌ Failed to connect to the database.")
    except Error as e:
        raise ConnectionError(f"Database connection error: {e}")

def ensure_database(conn):
    dbname = CONFIG["DB_NAME"]
    if dbname in SYSTEM_SCHEMAS:
        raise RuntimeError(f"Refus: '{dbname}' est un schéma système.")
    cur = conn.cursor()
    try:
        cur.execute(f"CREATE DATABASE IF NOT EXISTS `{dbname}` CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci")
        print(f"✓ Base vérifiée/créée : {dbname}")
    finally:
        cur.close()

def use_database(conn):
    dbname = CONFIG["DB_NAME"]
    cur = conn.cursor()
    try:
        cur.execute(f"USE `{dbname}`")
        print(f"Utilisation de la base `{dbname}`.")
    finally:
        cur.close()

def preview_wipe(conn):
    dbname = CONFIG["DB_NAME"]
    cur = conn.cursor()
    print("Inventaire des objets à supprimer dans la base :", dbname)

    def fetch_list(query, params=None):
        cur.execute(query, params or ())
        return [row[0] for row in cur.fetchall()]

    views = fetch_list("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.VIEWS WHERE TABLE_SCHEMA=%s", (dbname,))
    triggers = fetch_list("SELECT TRIGGER_NAME FROM INFORMATION_SCHEMA.TRIGGERS WHERE TRIGGER_SCHEMA=%s", (dbname,))
    events = fetch_list("SELECT EVENT_NAME FROM INFORMATION_SCHEMA.EVENTS WHERE EVENT_SCHEMA=%s", (dbname,))
    procs = fetch_list("SELECT ROUTINE_NAME FROM INFORMATION_SCHEMA.ROUTINES WHERE ROUTINE_SCHEMA=%s AND ROUTINE_TYPE='PROCEDURE'", (dbname,))
    funcs = fetch_list("SELECT ROUTINE_NAME FROM INFORMATION_SCHEMA.ROUTINES WHERE ROUTINE_SCHEMA=%s AND ROUTINE_TYPE='FUNCTION'", (dbname,))
    tables = fetch_list("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA=%s AND TABLE_TYPE='BASE TABLE'", (dbname,))

    print(f"- Vues       : {len(views)} -> {views}")
    print(f"- Triggers   : {len(triggers)} -> {triggers}")
    print(f"- Events     : {len(events)} -> {events}")
    print(f"- Procédures : {len(procs)} -> {procs}")
    print(f"- Fonctions  : {len(funcs)} -> {funcs}")
    print(f"- Tables     : {len(tables)} -> {tables}")
    cur.close()

def confirm_and_wipe(conn):
    dbname = CONFIG["DB_NAME"]
    cur = conn.cursor()
    try:
        cur.execute("SET FOREIGN_KEY_CHECKS=0")

        cur.execute("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.VIEWS WHERE TABLE_SCHEMA=%s", (dbname,))
        for (vname,) in cur.fetchall(): cur.execute(f"DROP VIEW IF EXISTS `{vname}`")

        cur.execute("SELECT TRIGGER_NAME FROM INFORMATION_SCHEMA.TRIGGERS WHERE TRIGGER_SCHEMA=%s", (dbname,))
        for (tname,) in cur.fetchall():
            try: cur.execute(f"DROP TRIGGER `{tname}`")
            except Error: pass

        cur.execute("SELECT EVENT_NAME FROM INFORMATION_SCHEMA.EVENTS WHERE EVENT_SCHEMA=%s", (dbname,))
        for (ename,) in cur.fetchall():
            try: cur.execute(f"DROP EVENT IF EXISTS `{ename}`")
            except Error: pass

        cur.execute("SELECT ROUTINE_NAME FROM INFORMATION_SCHEMA.ROUTINES WHERE ROUTINE_SCHEMA=%s AND ROUTINE_TYPE='PROCEDURE'", (dbname,))
        for (pname,) in cur.fetchall():
            try: cur.execute(f"DROP PROCEDURE IF EXISTS `{pname}`")
            except Error: pass
        cur.execute("SELECT ROUTINE_NAME FROM INFORMATION_SCHEMA.ROUTINES WHERE ROUTINE_SCHEMA=%s AND ROUTINE_TYPE='FUNCTION'", (dbname,))
        for (fname,) in cur.fetchall():
            try: cur.execute(f"DROP FUNCTION IF EXISTS `{fname}`")
            except Error: pass

        cur.execute("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA=%s AND TABLE_TYPE='BASE TABLE'", (dbname,))
        for (tname,) in cur.fetchall(): cur.execute(f"DROP TABLE IF EXISTS `{tname}`")

        print("✅ Base vidée avec succès.")
    except Error as e:
        print_sql_error("❌ Erreur pendant la suppression", e)
    finally:
        try: cur.execute("SET FOREIGN_KEY_CHECKS=1")
        except Error: pass
        cur.close()

def run_embedded_sql(conn):
    cur = conn.cursor()
    try:
        statements = [s.strip() for s in SQL_SCHEMA.split(";") if s.strip()]
        for stmt in statements:
            try:
                cur.execute(stmt)
            except Error as e:
                print_sql_error(f"Erreur sur la requête : {stmt[:80]}...", e)
        print("✓ SQL embarqué exécuté.")
    finally:
        cur.close()

def insertGameIntoDatabase(conn, games_data):
    """
    games_data attend des tuples de 8 valeurs:
    (biblio_id, titre, author, platform, platform_id, console_koha_id, console_type_id, createdAt)
    """
    sql = """
    INSERT INTO games
        (biblio_id, titre, author, platform, platform_id, console_koha_id, console_type_id, required_accessories, createdAt)
    VALUES
        (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE
        titre = VALUES(titre),
        author = VALUES(author),
        platform = VALUES(platform),
        platform_id = VALUES(platform_id),
        console_koha_id = VALUES(console_koha_id),
        console_type_id = VALUES(console_type_id),
        required_accessories = VALUES(required_accessories),
        lastUpdatedAt = NOW()
    """
    with conn.cursor() as cur:
        cur.executemany(sql, games_data)
    conn.commit()
    print(f">>> {len(games_data)} jeux insérés/mis à jour")


def insert_console(conn, consoles):
    """
    Insère les consoles depuis Koha en créant automatiquement les console_type
    et les console_stock associés
    """
    if not consoles:
        print("⚠️ Aucune console à insérer")
        return
    
    cursor = conn.cursor(dictionary=True)
    
    try:
        stats = {
            "types_created": 0,
            "types_existing": 0,
            "stocks_inserted": 0,
            "stocks_updated": 0,
            "errors": 0
        }
        
        for console in consoles:
            biblio_id = console.get("biblio_id")
            name = console.get("title", "").strip()
            if console.get("subtitle"):
                name += " " + console.get("subtitle", "").strip()
            timestamp = console.get("timestamp")
            
            if not biblio_id or not name:
                stats["errors"] += 1
                continue
            
            try:
                cursor.execute(
                    "SELECT id FROM console_type WHERE name = %s",
                    (name,)
                )
                result = cursor.fetchone()
                
                if result:
                    console_type_id = result['id']
                    stats["types_existing"] += 1
                else:
                    # 2. Créer le console_type s'il n'existe pas
                    cursor.execute("""
                        INSERT INTO console_type (name)
                        VALUES (%s)
                    """, (name,))
                    console_type_id = cursor.lastrowid
                    stats["types_created"] += 1
                    print(f"   ✓ Nouveau type créé: {name} (ID: {console_type_id})")
                
                # 3. Insérer ou mettre à jour le console_stock (CORRIGÉ)
                cursor.execute("""
                    INSERT INTO console_stock
                        (id, console_type_id, biblio_id, name, is_active, createdAt, lastUpdatedAt)
                    VALUES
                        (%s, %s, %s, %s, 1, NOW(), %s)
                    ON DUPLICATE KEY UPDATE
                        console_type_id = VALUES(console_type_id),
                        biblio_id = VALUES(biblio_id),
                        name = VALUES(name),
                        lastUpdatedAt = VALUES(lastUpdatedAt)
                """, (biblio_id, console_type_id, biblio_id, name, timestamp))
                
                if cursor.rowcount == 1:
                    stats["stocks_inserted"] += 1
                else:
                    stats["stocks_updated"] += 1
                
            except mysql.connector.Error as err:
                print(f"   ❌ Erreur pour console {biblio_id} ({name}): {err}")
                stats["errors"] += 1
                continue
        
        conn.commit()
        
        # Affichage des statistiques
        print(f"\n{'='*60}")
        print("STATISTIQUES SEED CONSOLES")
        print(f"{'='*60}")
        print(f"Types de consoles créés: {stats['types_created']}")
        print(f"Types de consoles existants: {stats['types_existing']}")
        print(f"Exemplaires insérés: {stats['stocks_inserted']}")
        print(f"Exemplaires mis à jour: {stats['stocks_updated']}")
        if stats['errors'] > 0:
            print(f"⚠️ Erreurs: {stats['errors']}")
        print(f"{'='*60}\n")
        
    except mysql.connector.Error as err:
        print(f"❌ Erreur globale MySQL: {err}")
        conn.rollback()
    finally:
        cursor.close()
    
    print("=== SEED CONSOLES KOHA: terminé ===\n")

def insert_accessoires(conn, accessoires):
    """
    Upsert des accessoires liés à leurs console_type_id.
    Nécessite:
      - la table console_type déjà remplie
      - un index unique sur accessoires.koha_id
    """
    if not accessoires:
        print("⚠️ Aucun accessoire à insérer")
        return

    type_map = get_console_type_id_map(conn)

    tuples, skipped = [], 0
    for d in accessoires:
        name = (d.get("name") or "").strip()
        koha_id = d.get("koha_id")
        platforms = d.get("platforms") or []
        hidden = d.get("hidden", 0)

        if not name or koha_id in (None, ""):
            skipped += 1
            continue

        try:
            koha_id = int(koha_id)
        except Exception:
            skipped += 1
            continue

        ids = []
        for p in platforms:
            cid = type_map.get(p.strip().lower())
            if cid:
                ids.append(cid)

        console_json = json.dumps(ids) if ids else "null"

        tuples.append((name, console_json, koha_id, hidden))

    if not tuples:
        print("⚠️ Rien d’insérable (skipped: %d)" % skipped)
        return

    sql = """
        INSERT INTO accessoires
            (name, consoles, koha_id, hidden, lastUpdatedAt, createdAt)
        VALUES
            (%s, CAST(%s AS JSON), %s, %s, NOW(), NOW())
        ON DUPLICATE KEY UPDATE
            name = VALUES(name),
            consoles = CAST(VALUES(consoles) AS JSON),
            hidden = VALUES(hidden),
            lastUpdatedAt = NOW()
    """

    BATCH = 500
    affected = 0
    try:
        with conn.cursor() as cur:
            for i in range(0, len(tuples), BATCH):
                cur.executemany(sql, tuples[i:i+BATCH])
                affected += cur.rowcount
        conn.commit()
        print(f"✅ Upsert accessoires: {affected} lignes (skipped: {skipped})")
    except mysql.connector.Error as err:
        print(f"❌ Erreur MySQL pendant l'upsert accessoires : {err}")
        conn.rollback()
    print("=== SEED ACCESSOIRES KOHA: terminé ===\n")


def print_sql_error(prefix, e: Error):
    err_no = getattr(e, "errno", None)
    sqlstate = getattr(e, "sqlstate", None)
    msg = getattr(e, "msg", str(e))
    print(f"{prefix} : [{err_no}/{sqlstate}] {msg}")

def get_console_type_id_map(conn):
    """Retourne un dict {name_lower: id} pour console_type."""
    m = {}
    with conn.cursor(dictionary=True) as cur:
        cur.execute("SELECT id, name FROM console_type")
        for row in cur.fetchall():
            m[row["name"].strip().lower()] = row["id"]
    return m

def get_known_accessory_ids(conn):
    ids = set()
    with conn.cursor() as cur:
        cur.execute("SELECT koha_id FROM accessoires")
        for (kid,) in cur.fetchall():
            if kid is not None:
                ids.add(int(kid))
    return ids
