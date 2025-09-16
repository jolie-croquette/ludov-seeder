@echo off
echo === Compilation du projet en exe ===
python -m PyInstaller --onefile --name LUDOVSeeder main.py
echo === Terminé ! Le .exe est disponible dans le dossier dist/ ===
pause
