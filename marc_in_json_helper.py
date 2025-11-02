# -*- coding: utf-8 -*-
"""
Helpers pour manipuler des notices MARC-in-JSON (MARC-JSON).

Fonctions publiques conservant les mêmes noms que ton code existant :
- iter_fields(record)
- get_control_field(record, tag)
- get_data_fields(record, tag)
- first_subfield(record, tag, code)
- all_subfields(record, tag, code)
- record_to_flat_map(record)
- extract_accessoire_row(record)

Améliorations :
- Typage léger (facultatif) et docstrings.
- Nettoyage/normalisation des plateformes (753$a) :
  - split sur virgules/points-virgules/espaces multiples,
  - trim, déduplication, suppression des vides.
- Retour JSON valide pour "console" (liste de plateformes) ou "null" si aucune.
"""

from __future__ import annotations
from typing import Any, Dict, Iterable, Iterator, List, Optional, Tuple

import json
import re


# ==============================
# MARC primitives (inchangées)
# ==============================

def iter_fields(record: Dict[str, Any]) -> Iterator[Tuple[str, Any]]:
    """Itère (tag, value) sur chaque champ d'une notice MARC-JSON."""
    for item in record.get("fields", []):
        ((tag, value),) = item.items()
        yield tag, value


def get_control_field(record: Dict[str, Any], tag: str) -> Optional[str]:
    """Retourne la valeur chaîne d'un champ de contrôle (ex: 005, 008)."""
    for t, v in iter_fields(record):
        if t == tag and isinstance(v, str):
            return v
    return None


def get_data_fields(record: Dict[str, Any], tag: str) -> List[Dict[str, Any]]:
    """Retourne toutes les occurrences d'un champ de données (avec subfields)."""
    out: List[Dict[str, Any]] = []
    for t, v in iter_fields(record):
        if t == tag and isinstance(v, dict):
            out.append(v)
    return out


def first_subfield(record: Dict[str, Any], tag: str, code: str) -> Optional[str]:
    """Retourne la première sous-zone code (ex: 245 $a) ou None."""
    for field in get_data_fields(record, tag):
        for sf in field.get("subfields", []):
            if code in sf:
                return sf[code]
    return None


def all_subfields(record: Dict[str, Any], tag: str, code: str) -> List[str]:
    """Retourne toutes les valeurs d'une sous-zone donnée (ex: 300 $a multiples)."""
    vals: List[str] = []
    for field in get_data_fields(record, tag):
        for sf in field.get("subfields", []):
            if code in sf:
                vals.append(sf[code])
    return vals


def record_to_flat_map(record: Dict[str, Any]) -> Dict[str, List[Any]]:
    """Transforme fields -> dict {tag: [valeurs...]}."""
    flat: Dict[str, List[Any]] = {}
    for tag, value in iter_fields(record):
        flat.setdefault(tag, []).append(value)
    return flat


# ==============================
# Utilitaires internes
# ==============================

_SPLIT_PLATFORMS_REGEX = re.compile(r"[;,]|\s{2,}")

def _split_platforms(raw):
    if not raw:
        return []
    parts = [p.strip() for p in _SPLIT_PLATFORMS_REGEX.split(raw) if p and p.strip()]
    seen, out = set(), []
    for p in parts:
        if p not in seen:
            seen.add(p)
            out.append(p)
    return out


# ==============================
# Mapping accessoire
# ==============================

def extract_accessoire_row(record):
    """Retourne {'name','platforms','koha_id'} (platforms = liste de noms)."""
    titre = first_subfield(record, "245", "a")
    plateforme_raw = first_subfield(record, "753", "a")
    koha_id = first_subfield(record, "999", "c") or first_subfield(record, "999", "d")

    plateformes = _split_platforms(plateforme_raw)
    return {
        "name": (titre or "").strip(),
        "platforms": plateformes,
        "koha_id": koha_id
    }

# --- à APPEND dans marc_in_json_helper.py ---

def extract_game_row(record):
    """Retourne un dict jeu à partir d'une notice MARC-JSON.

    Sortie:
      {
        "biblio_id": int,
        "titre": str,
        "author": Optional[str],
        "platforms": List[str],  # 753$a, nettoyé/dédoublonné
        "timestamp": str,        # ISO (005), fallback now si absent
      }
    """
    # ID Koha (999$c ou 999$d)
    koha_id = first_subfield(record, "999", "c") or first_subfield(record, "999", "d")
    if not koha_id:
        return {}

    try:
        biblio_id = int(str(koha_id).strip())
    except Exception:
        return {}

    # Titre (245 $a + $b optionnel)
    t_a = first_subfield(record, "245", "a") or ""
    t_b = first_subfield(record, "245", "b") or first_subfield(record, "245", "9") or ""
    raw_title = f"{t_a} {t_b}".strip(" /:;., ").strip()
    if not raw_title:
        return {}

    # Auteur (110 corporate, sinon 100 perso)
    author = first_subfield(record, "110", "a") or first_subfield(record, "100", "a")

    # Plateformes (753 $a, éventuellement multiples)
    plateformes_raw = first_subfield(record, "753", "a")
    platforms = _split_platforms(plateformes_raw)

    # Timestamp (005)
    ts = get_control_field(record, "005") or ""
    return {
        "biblio_id": biblio_id,
        "titre": raw_title,
        "author": author.strip() if author else None,
        "platforms": platforms,
        "timestamp": ts
    }
