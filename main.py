import os
import re
import getpass
import mysql.connector
from mysql.connector import Error

print("""
=========================================
   LUDOV SEEDER
   Générateur de données pour LUDOV
   Auteur : jolie-croquette
   Date : 16/09/2025
=========================================
""")

DEFAULT_HOST = "localhost"
DEFAULT_PORT = 3306
DB_NAME = "ludov_dev"

# --- SQL embarqué (corrigé & normalisé) ---
SQL_SCHEMA = r"""
CREATE TABLE IF NOT EXISTS `users` (
	`id` int AUTO_INCREMENT NOT NULL UNIQUE,
	`firstname` varchar(50) NOT NULL,
	`lastname` varchar(100) NOT NULL,
	`email` varchar(255) NOT NULL,
	`password` varchar(255) NOT NULL,
	`isAdmin` tinyint NOT NULL,
	`lastUpdatedAt` datetime NOT NULL,
	`createdAt` datetime NOT NULL,
	PRIMARY KEY (`id`)
);

CREATE TABLE IF NOT EXISTS `reservations` (
	`id` int AUTO_INCREMENT NOT NULL UNIQUE,
	`etudiant_id` varchar(255) NOT NULL,
	`games` json NOT NULL,
	`console` int NOT NULL,
	`station` int NOT NULL,
	`date` datetime NOT NULL,
	`archived` tinyint NOT NULL DEFAULT '0',
	`createdAt` datetime NOT NULL,
	PRIMARY KEY (`id`)
);

CREATE TABLE IF NOT EXISTS `consoles` (
	`id` int AUTO_INCREMENT NOT NULL UNIQUE,
	`accessoirs` json NOT NULL,
	`available` tinyint NOT NULL DEFAULT '1',
	`lastUpdatedAt` datetime NOT NULL,
	`createdAt` datetime NOT NULL,
	PRIMARY KEY (`id`)
);

CREATE TABLE IF NOT EXISTS `games` (
	`id` int AUTO_INCREMENT NOT NULL UNIQUE,
	`titre` text NOT NULL,
	`author` text NOT NULL,
	`biblios_id` int NOT NULL,
	`picture` longtext NOT NULL,
	`available` tinyint NOT NULL DEFAULT '1',
	`createdAt` datetime NOT NULL,
	`lastUpdatedAt` datetime NOT NULL,
	PRIMARY KEY (`id`)
);

CREATE TABLE IF NOT EXISTS `reservation_hold` (
	`id` int AUTO_INCREMENT NOT NULL UNIQUE,
	`user_id` varchar(255) NOT NULL,
	`console_id` varchar(255) NOT NULL,
	`game1_id` varchar(255),
	`game2_id` varchar(255),
	`game3_id` varchar(255),
	`station_id` int,
	`accessoir_id` varchar(255),
	`createdAt` datetime NOT NULL,
	PRIMARY KEY (`id`)
);

CREATE TABLE IF NOT EXISTS `stations` (
	`id` int AUTO_INCREMENT NOT NULL UNIQUE,
	`consoles` json NOT NULL,
	`lastUpdatedAt` datetime NOT NULL,
	`createdAt` datetime NOT NULL,
	PRIMARY KEY (`id`)
);

CREATE TABLE IF NOT EXISTS `accessoirs` (
	`id` int AUTO_INCREMENT NOT NULL UNIQUE,
	`name` text NOT NULL,
	`description` longtext NOT NULL,
	`console_id` varchar(255) NOT NULL,
	`quantity` int NOT NULL,
	`lastUpdatedAt` datetime NOT NULL,
	`createdAt` datetime NOT NULL,
	PRIMARY KEY (`id`)
);


ALTER TABLE `reservations` ADD CONSTRAINT `reservations_fk1` FOREIGN KEY (`etudiant_id`) REFERENCES `users`(`id`);

ALTER TABLE `reservations` ADD CONSTRAINT `reservations_fk2` FOREIGN KEY (`games`) REFERENCES `games`(`id`);

ALTER TABLE `reservations` ADD CONSTRAINT `reservations_fk3` FOREIGN KEY (`console`) REFERENCES `Consoles`(`id`);
ALTER TABLE `Consoles` ADD CONSTRAINT `Consoles_fk1` FOREIGN KEY (`accessoirs`) REFERENCES `accessoirs`(`id`);

ALTER TABLE `reservation_hold` ADD CONSTRAINT `reservation_hold_fk1` FOREIGN KEY (`user_id`) REFERENCES `users`(`id`);

ALTER TABLE `reservation_hold` ADD CONSTRAINT `reservation_hold_fk2` FOREIGN KEY (`console_id`) REFERENCES `Consoles`(`id`);

ALTER TABLE `reservation_hold` ADD CONSTRAINT `reservation_hold_fk3` FOREIGN KEY (`game1_id`) REFERENCES `games`(`id`);

ALTER TABLE `reservation_hold` ADD CONSTRAINT `reservation_hold_fk4` FOREIGN KEY (`game2_id`) REFERENCES `games`(`id`);

ALTER TABLE `reservation_hold` ADD CONSTRAINT `reservation_hold_fk5` FOREIGN KEY (`game3_id`) REFERENCES `games`(`id`);

ALTER TABLE `reservation_hold` ADD CONSTRAINT `reservation_hold_fk6` FOREIGN KEY (`station_id`) REFERENCES `stations`(`id`);

ALTER TABLE `reservation_hold` ADD CONSTRAINT `reservation_hold_fk7` FOREIGN KEY (`accessoir_id`) REFERENCES `accessoirs`(`id`);
ALTER TABLE `stations` ADD CONSTRAINT `stations_fk1` FOREIGN KEY (`consoles`) REFERENCES `Consoles`(`id`);
ALTER TABLE `accessoirs` ADD CONSTRAINT `accessoirs_fk3` FOREIGN KEY (`console_id`) REFERENCES `Consoles`(`id`);
"""

SYSTEM_SCHEMAS = {"mysql", "information_schema", "performance_schema", "sys"}

def main():
    user, password, host, port = collect_connection_info()
    conn = init_server_connection(user, password, host, port)
    if conn is None:
        return

    try:
        ensure_database(conn, DB_NAME)
        use_database(conn, DB_NAME)

        # --- (Optionnel) vider la base avant de réimporter ---
        preview_wipe(conn, DB_NAME)
        if confirm(f"⚠️ Vider complètement la base '{DB_NAME}' avant import ? (o/n) : "):
            confirm_and_wipe(conn, DB_NAME)

        print("\n=== Import du SQL embarqué ===")
        run_embedded_sql(conn, SQL_SCHEMA)
        print("✅ Schéma importé avec succès.")
    finally:
        try:
            if conn.is_connected():
                conn.close()
                print("Connexion fermée proprement.")
        except NameError:
            pass

def collect_connection_info():
    user = prompt("Entrer votre nom d'utilisateur de base de données : ", validator=lambda s: len(s.strip())>0, err="Username invalide.")
    print("=========================================")
    print(f"Votre username est : {user}")

    password = prompt_password()
    host = prompt_host()
    port = DEFAULT_PORT
    return user, password, host, port

def prompt_password():
    while True:
        pw = getpass.getpass("Entrer votre mot de passe de base de données : ").strip()
        if not pw:
            print("Mot de passe invalide. Veuillez réessayer.")
            continue
        print("=========================================")
        return pw

def prompt_host():
    while True:
        if confirm(f"Souhaitez-vous utiliser l'hôte par défaut ({DEFAULT_HOST}) ? (o/n) : "):
            return DEFAULT_HOST
        host = prompt("Entrer l'adresse de l'hôte : ",
                      validator=is_valid_host,
                      err="Hôte invalide. Entrez une IP (ex: 192.168.1.10) ou un nom DNS (ex: db.example.com).")
        print("=========================================")
        print(f"Votre hôte est : {host}")
        if confirm("Est-ce correct ? (o/n) : "):
            return host

def prompt(msg, validator=None, err="Entrée invalide."):
    while True:
        val = input(msg).strip()
        if validator is None or validator(val):
            return val
        print(err)

def confirm(msg):
    return input(msg).strip().lower() in ("o","oui","y","yes")

def is_valid_host(host: str) -> bool:
    ip_ok = re.match(r"^(?:\d{1,3}\.){3}\d{1,3}$", host) is not None
    dns_ok = re.match(r"^(?:(?:[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?\.)+[a-zA-Z]{2,})$", host) is not None
    localhost_ok = host == "localhost"
    return ip_ok or dns_ok or localhost_ok

def init_server_connection(db_user, db_password, host, port):
    print("=========================================")
    print(f"Tentative de connexion MySQL vers {host}:{port} avec l'utilisateur '{db_user}'...")
    try:
        conn = mysql.connector.connect(
            host=host,
            port=port,
            user=db_user,
            password=db_password,
            autocommit=True,
        )
        if conn.is_connected():
            print("Connexion au serveur réussie.")
            return conn
    except Error as e:
        print_sql_error("Erreur lors de la connexion au serveur MySQL", e)
        return None

def ensure_database(conn, dbname):
    if dbname in SYSTEM_SCHEMAS:
        raise RuntimeError(f"Refus: '{dbname}' est un schéma système.")
    cur = conn.cursor()
    try:
        cur.execute(f"CREATE DATABASE IF NOT EXISTS `{dbname}` CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci")
        print(f"✓ Base vérifiée/créée : {dbname}")
    finally:
        cur.close()

def use_database(conn, dbname):
    cur = conn.cursor()
    try:
        cur.execute(f"USE `{dbname}`")
        print(f"Utilisation de la base `{dbname}`.")
    finally:
        cur.close()

def preview_wipe(conn, dbname):
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

def confirm_and_wipe(conn, dbname):
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

def run_embedded_sql(conn, sql_text: str):
    cur = conn.cursor()
    try:
        statements = [s.strip() for s in sql_text.split(";") if s.strip()]
        for stmt in statements:
            try:
                cur.execute(stmt)
            except Error as e:
                print_sql_error(f"Erreur sur la requête : {stmt[:80]}...", e)
        print("✓ SQL embarqué exécuté.")
    finally:
        cur.close()


def print_sql_error(prefix, e: Error):
    err_no = getattr(e, "errno", None)
    sqlstate = getattr(e, "sqlstate", None)
    msg = getattr(e, "msg", str(e))
    print(f"{prefix} : [{err_no}/{sqlstate}] {msg}")

if __name__ == "__main__":
    main()
