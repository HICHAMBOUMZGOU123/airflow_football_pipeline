from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import pandas as pd
import sqlite3

# ⚠️ Remplace par ta vraie clé API
API_KEY = "cf61f67281118649bd622fb79a86a4f8"

headers = {
    "x-rapidapi-host": "v3.football.api-sports.io",
    "x-rapidapi-key": API_KEY
}

# ===== FONCTIONS DU PIPELINE =====

def extraire_classement():
    print("📥 Extraction du classement Ligue 1...")
    url = "https://v3.football.api-sports.io/standings"
    params = {"league": 61, "season": 2024}
    response = requests.get(url, headers=headers, params=params)
    data = response.json()
    standings = data["response"][0]["league"]["standings"][0]
    rows = []
    for team in standings:
        rows.append({
            "position": team["rank"],
            "equipe": team["team"]["name"],
            "points": team["points"],
            "victoires": team["all"]["win"],
            "defaites": team["all"]["lose"],
            "buts_pour": team["all"]["goals"]["for"],
        })
    df = pd.DataFrame(rows)
    df.to_csv("/opt/airflow/dags/classement.csv", index=False)
    print(f"✅ {len(df)} équipes extraites !")

def transformer_donnees():
    print("🧹 Transformation des données...")
    df = pd.read_csv("/opt/airflow/dags/classement.csv")
    df["ratio_victoires"] = (df["victoires"] / 30 * 100).round(1)
    df["buts_moyenne"] = (df["buts_pour"] / 30).round(2)
    df.to_csv("/opt/airflow/dags/classement_transforme.csv", index=False)
    print("✅ Transformation terminée !")

def charger_donnees():
    print("💾 Chargement dans la base de données...")
    df = pd.read_csv("/opt/airflow/dags/classement_transforme.csv")
    conn = sqlite3.connect("/opt/airflow/dags/football.db")
    df.to_sql("classement_ligue1", conn, if_exists="replace", index=False)
    conn.close()
    print(f"✅ {len(df)} équipes stockées !")

# ===== DAG AIRFLOW =====

default_args = {
    "owner": "hicham",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="football_pipeline",
    default_args=default_args,
    description="Pipeline ETL Football — Ligue 1",
    schedule_interval="0 9 * * *",  # Tous les jours à 9h
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["football", "ETL", "data-engineering"],
) as dag:

    tache_extraction = PythonOperator(
        task_id="extraire_classement",
        python_callable=extraire_classement,
    )

    tache_transformation = PythonOperator(
        task_id="transformer_donnees",
        python_callable=transformer_donnees,
    )

    tache_chargement = PythonOperator(
        task_id="charger_donnees",
        python_callable=charger_donnees,
    )

    # Ordre d'exécution ETL
    tache_extraction >> tache_transformation >> tache_chargement