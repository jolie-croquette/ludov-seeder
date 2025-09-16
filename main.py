import regex
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

def main():
    user, password, host, port = collect_connection_info()
    init_connection(user, password, host, port)

def collect_connection_info():
    user = prompt_username()
    password = prompt_password()
    host = prompt_host()
    port = DEFAULT_PORT  # ajoute une saisie si tu veux le rendre interactif
    return user, password, host, port

def prompt_username():
    while True:
        username = input("Entrer votre nom d'utilisateur de base de données : ").strip()
        if not username:
            print("Username invalide. Veuillez réessayer.")
            continue

        print("=========================================")
        print(f"Votre username est : {username}")
        confirmation = input("Est-ce correct ? (o/n) : ").strip().lower()
        if confirmation in ("o", "oui"):
            return username

def prompt_password():
    while True:
        password = getpass.getpass("Entrer votre mot de passe de base de données : ")
        if not password.strip():
            print("Mot de passe invalide. Veuillez réessayer.")
            continue

        print("=========================================")
        # On n’affiche pas le mot de passe en clair. On demande juste confirmation.
        confirmation = input("Confirmer le mot de passe saisi ? (o/n) : ").strip().lower()
        if confirmation in ("o", "oui"):
            return password

def prompt_host():
    while True:
        confirmation = input(f"Souhaitez-vous utiliser l'hôte par défaut ({DEFAULT_HOST}) ? (o/n) : ").strip().lower()
        if confirmation in ("o", "oui"):
            return DEFAULT_HOST
        elif confirmation in ("n", "non"):
            host = input("Entrer l'adresse de l'hôte : ").strip()
            if is_valid_host(host):
                print("=========================================")
                print(f"Votre hôte est : {host}")
                confirmation2 = input("Est-ce correct ? (o/n) : ").strip().lower()
                if confirmation2 in ("o", "oui"):
                    return host
            else:
                print("Hôte invalide. Entrez une IP (ex: 192.168.1.10) ou un nom DNS (ex: db.example.com).")

def is_valid_host(host: str) -> bool:
    ip_ok = regex.match(r"^(?:[0-9]{1,3}\.){3}[0-9]{1,3}$", host) is not None
    dns_ok = regex.match(r"^(?:(?:[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?\.)+[a-zA-Z]{2,})$", host) is not None
    localhost_ok = host == "localhost"
    return ip_ok or dns_ok or localhost_ok

def init_connection(db_user, db_password, host, port):
    print("=========================================")
    print(f"Tentative de connexion MySQL vers {host}:{port} avec l'utilisateur '{db_user}'...")
    try:
        conn = mysql.connector.connect(
            host=host,
            port=port,
            user=db_user,
            password=db_password,
        )
        if conn.is_connected():
            print("=========================================")
            print("Connexion à la base de données réussie")
            print("=========================================")
    except Error as e:
        print("Erreur lors de la connexion à MySQL :", e)
    finally:
        try:
            if conn.is_connected():
                conn.close()
                print("Connexion fermée proprement.")
        except NameError:
            pass

if __name__ == "__main__":
    main()
