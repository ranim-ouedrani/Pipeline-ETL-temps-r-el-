# Pipeline de pointage temps réel : Pointeuse biométrique → Kafka → Oracle

Projet réalisé lors d'un stage de Data Engineering chez **El Fouleth** (juillet - août 2026, Tunisie).

Il remplace un script Python monolithique par deux services indépendants (producer et consumer) reliés par **Apache Kafka**, pour une collecte plus robuste et évolutive des pointages biométriques.

## Architecture

```
[Pointeuse ZKTeco (IP:4370)] → [Producer Python] → [Kafka : topic "pointages"] → [Consumer Python] → [Oracle DB]
```

- **Producer** (`producer_pointage.py`, `producer_live.py`) : se connecte à la pointeuse via son adresse IP (bibliothèque `pyzk`), lit les pointages et les publie en JSON dans Kafka. La version `producer_pointage.py` ne renvoie que les nouveaux pointages (fichier d'état), la version `producer_live.py` écoute la pointeuse en continu.
- **Kafka** : bus de messages qui découple la collecte de l'insertion.
- **Consumer** (`consumer_pointage.py`) : lit le topic et insère dans Oracle. Une contrainte d'unicité `(user_id, timestamp, punch)` évite les doublons.

Format d'un message :
```json
{"user_id": "12", "timestamp": "2026-08-03T09:14:36", "status": 1, "punch": 0}
```

## Technologies
Python, Apache Kafka (`kafka-python`), Oracle Database (`oracledb`), ZKTeco (`pyzk`)

## Installation

1. Installer les dépendances :
   ```
   pip install -r requirements.txt
   ```
2. Copier `.env.example` en `.env` et renseigner l'IP de la pointeuse et les identifiants Oracle.
3. Démarrer Kafka (le broker doit écouter sur `localhost:9092` par défaut) et créer le topic `pointages`.
4. Créer la table Oracle :
   ```
   python create_table.py
   ```

## Utilisation

```
python test_zk.py              # tester la connexion à la pointeuse
python producer_pointage.py    # envoyer les nouveaux pointages vers Kafka
python consumer_pointage.py    # insérer les messages dans Oracle
python verifier_donnees.py     # compter les pointages en base
```

Pour le mode temps réel : `python producer_live.py` puis `python consumer_pointage.py`.

## Sécurité
Les identifiants et l'IP de la pointeuse ne sont pas dans le code : ils sont lus depuis le fichier `.env`, exclu de Git.
