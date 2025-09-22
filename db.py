import json
import mysql.connector
from mysql.connector import Error, errorcode
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
  `password` VARCHAR(255) NOT NULL,
  `isAdmin` TINYINT NOT NULL,
  `lastUpdatedAt` DATETIME NOT NULL,
  `createdAt` DATETIME NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS `consoles` (
  `id` INT AUTO_INCREMENT NOT NULL UNIQUE,
  `name` VARCHAR(255) NOT NULL UNIQUE,
  `accessoires` JSON,
  `available` TINYINT NOT NULL DEFAULT '1',
  `lastUpdatedAt` DATETIME NOT NULL,
  `createdAt` DATETIME NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS `games` (
  `id` INT AUTO_INCREMENT NOT NULL UNIQUE,
  `titre` TEXT NOT NULL,
  `author` TEXT NOT NULL,
  `biblio_id` INT NOT NULL,
  `picture` LONGTEXT NOT NULL,
  `available` TINYINT NOT NULL DEFAULT '1',
  `createdAt` DATETIME NOT NULL,
  `lastUpdatedAt` DATETIME NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS `stations` (
  `id` INT AUTO_INCREMENT NOT NULL UNIQUE,
  `consoles` JSON NOT NULL,             -- laissé en JSON (pas de FK)
  `lastUpdatedAt` DATETIME NOT NULL,
  `createdAt` DATETIME NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS `reservations` (
  `id` INT AUTO_INCREMENT NOT NULL UNIQUE,
  `etudiant_id` INT NOT NULL,           -- corrigé: INT (avant VARCHAR)
  `games` JSON NOT NULL,                -- JSON (pas de FK)
  `console` INT NOT NULL,
  `station` INT NOT NULL,
  `date` DATETIME NOT NULL,
  `archived` TINYINT NOT NULL DEFAULT '0',
  `createdAt` DATETIME NOT NULL,
  PRIMARY KEY (`id`),
  KEY `ix_res_user` (`etudiant_id`),
  KEY `ix_res_console` (`console`),
  KEY `ix_res_station` (`station`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS `accessoires` (
  `id` INT AUTO_INCREMENT NOT NULL UNIQUE,
  `name` TEXT NOT NULL,
  `description` LONGTEXT NOT NULL,
  `console_id` INT NOT NULL,            -- corrigé: INT (avant VARCHAR)
  `quantity` INT NOT NULL,
  `lastUpdatedAt` DATETIME NOT NULL,
  `createdAt` DATETIME NOT NULL,
  PRIMARY KEY (`id`),
  KEY `ix_accessoires_console` (`console_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS `reservation_hold` (
  `id` INT AUTO_INCREMENT NOT NULL UNIQUE,
  `user_id` INT NOT NULL,               -- corrigé: INT
  `console_id` INT NOT NULL,            -- corrigé: INT
  `game1_id` INT NULL,                  -- corrigé: INT
  `game2_id` INT NULL,                  -- corrigé: INT
  `game3_id` INT NULL,                  -- corrigé: INT
  `station_id` INT NULL,
  `accessoire_id` INT NULL,              -- corrigé: INT
  `createdAt` DATETIME NOT NULL,
  PRIMARY KEY (`id`),
  KEY `ix_hold_user` (`user_id`),
  KEY `ix_hold_console` (`console_id`),
  KEY `ix_hold_game1` (`game1_id`),
  KEY `ix_hold_game2` (`game2_id`),
  KEY `ix_hold_game3` (`game3_id`),
  KEY `ix_hold_station` (`station_id`),
  KEY `ix_hold_accessoire` (`accessoire_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ============
-- FOREIGN KEYS
-- ============

-- reservations.etudiant_id -> users.id
ALTER TABLE `reservations`
  ADD CONSTRAINT `reservations_fk1`
  FOREIGN KEY (`etudiant_id`) REFERENCES `users`(`id`)
  ON UPDATE CASCADE ON DELETE RESTRICT;

-- (supprimé) reservations.games JSON -> games.id  ❌ impossible en FK

-- reservations.console -> consoles.id  (corrigé casse: consoles)
ALTER TABLE `reservations`
  ADD CONSTRAINT `reservations_fk3`
  FOREIGN KEY (`console`) REFERENCES `consoles`(`id`)
  ON UPDATE CASCADE ON DELETE RESTRICT;

-- (supprimé) Consoles.accessoires JSON -> accessoires.id  ❌ impossible en FK

-- reservation_hold.user_id -> users.id
ALTER TABLE `reservation_hold`
  ADD CONSTRAINT `reservation_hold_fk1`
  FOREIGN KEY (`user_id`) REFERENCES `users`(`id`)
  ON UPDATE CASCADE ON DELETE RESTRICT;

-- reservation_hold.console_id -> consoles.id  (corrigé casse: consoles)
ALTER TABLE `reservation_hold`
  ADD CONSTRAINT `reservation_hold_fk2`
  FOREIGN KEY (`console_id`) REFERENCES `consoles`(`id`)
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

-- reservation_hold.accessoire_id -> accessoires.id
ALTER TABLE `reservation_hold`
  ADD CONSTRAINT `reservation_hold_fk7`
  FOREIGN KEY (`accessoire_id`) REFERENCES `accessoires`(`id`)
  ON UPDATE CASCADE ON DELETE SET NULL;

-- (supprimé) stations.consoles JSON -> consoles.id  ❌ impossible en FK

-- accessoires.console_id -> consoles.id  (corrigé casse: consoles)
ALTER TABLE `accessoires`
  ADD CONSTRAINT `accessoires_fk3`
  FOREIGN KEY (`console_id`) REFERENCES `consoles`(`id`)
  ON UPDATE CASCADE ON DELETE RESTRICT;

CREATE VIEW CONSOLE_AVAILABLE AS
    SELECT MIN(id) as id, name, available, createdAt, lastUpdatedAt
    FROM consoles
    GROUP BY name;

CREATE VIEW `GAME_AVAILABLE` AS
    SELECT * FROM games GROUP BY titre;

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
    dbname = CONFIG["DB_NAME"]
    try:
        conn = mysql.connector.connect(
            host=CONFIG["DB_HOST"],
            port=CONFIG["DB_PORT"],
            user=CONFIG["DB_USER"],
            password=CONFIG["DB_PASSWORD"],
            database=dbname,
            auth_plugin='mysql_native_password',
            use_pure=True
        )
        if conn.is_connected():
            return conn
        raise ConnectionError("❌ Failed to connect to the database.")
    except Error as e:
        if getattr(e, "errno", None) == errorcode.ER_BAD_DB_ERROR:
            server_conn = mysql.connector.connect(
                host=CONFIG["DB_HOST"],
                port=CONFIG["DB_PORT"],
                user=CONFIG["DB_USER"],
                password=CONFIG["DB_PASSWORD"],
                auth_plugin='mysql_native_password'
            )
            try:
                ensure_database(server_conn)  # crée la DB
            finally:
                try: server_conn.close()
                except: pass
                            
            # Reconnexion sur la DB désormais existante
            conn = mysql.connector.connect(
                host=CONFIG["DB_HOST"],
                port=CONFIG["DB_PORT"],
                user=CONFIG["DB_USER"],
                password=CONFIG["DB_PASSWORD"],
                database=dbname,
                auth_plugin='mysql_native_password'
            )
            if conn.is_connected():
                return conn
            raise ConnectionError("❌ Failed to connect to the database after creating it.")
        # 3) Autres erreurs : on propage
        raise ConnectionError(f"Database connection error: {e}")

def ensure_database(conn):
    dbname = CONFIG["DB_NAME"]
    if dbname in SYSTEM_SCHEMAS:
        raise RuntimeError(f"Refus: '{dbname}' est un schéma système.")
    cur = conn.cursor()
    try:
        try:
            cur.execute(
                f"CREATE DATABASE IF NOT EXISTS `{dbname}` "
                f"CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci"
            )
        except Error as e:
            # Si la collation n'est pas reconnue (ex. MariaDB), on retente avec une collation universelle
            if getattr(e, "errno", None) == errorcode.ER_UNKNOWN_COLLATION:
                cur.execute(
                    f"CREATE DATABASE IF NOT EXISTS `{dbname}` "
                    f"CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci"
                )
            else:
                raise
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

def insertGameIntoDatabase(conn, games):
    sql = """
        INSERT INTO games
            (id, titre, biblio_id, author, picture, available, lastUpdatedAt, createdAt)
        VALUES
            (%s, %s, %s, %s, %s, %s, %s, NOW())
    """
    try:
        cur = conn.cursor()
        cur.executemany(sql, games)
        conn.commit()
        print(f"✅ Upsert effectué : {cur.rowcount} lignes (insert+update).")
    except mysql.connector.Error as err:
        print(f"Erreur MySQL pendant l’upsert : {err}")
    finally:
        try:
            cur.close()
        except Exception:
            pass
    print("=== SEED JEUX KOHA: terminé ===\n")

def insert_console(conn, consoles):
    sql = """
        INSERT IGNORE INTO consoles
            (name, accessoires, available, lastUpdatedAt, createdAt)
        VALUES
            (%s, NULL, 1, NOW(), NOW())
    """
    try:
        cur = conn.cursor()
        cur.executemany(sql, consoles)
        conn.commit()
        print(f"✅ Upsert effectué : {cur.rowcount} lignes (insert+update).")
    except mysql.connector.Error as err:
        print(f"Erreur MySQL pendant l’upsert : {err}")
    finally:
        try:
            cur.close()
        except Exception:
            pass
    print("=== SEED CONSOLES: terminé ===\n")

def print_sql_error(prefix, e: Error):
    err_no = getattr(e, "errno", None)
    sqlstate = getattr(e, "sqlstate", None)
    msg = getattr(e, "msg", str(e))
    print(f"{prefix} : [{err_no}/{sqlstate}] {msg}")