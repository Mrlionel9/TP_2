import polars as pl
import os
import time
import psutil

def mesurer_performance():
    process = psutil.Process(os.getpid())
    return process.memory_info().rss / (1024 * 1024)

def resoudre_tp_approche2_parallele():
    BASE_DIR = r"C:\Users\lione\Desktop\TP2"
    DATASET_DIR = os.path.join(BASE_DIR, "datasets")
    OUTPUT_FILE = os.path.join(BASE_DIR, "Approche2", "dataset_unifie_polars.csv")
    
    if not os.path.exists(os.path.join(BASE_DIR, "Approche2")):
        os.makedirs(os.path.join(BASE_DIR, "Approche2"))

    print("--- Démarrage de l'Approche 2 : Polars (Parallèle + Lazy Streaming) ---")
    
    mem_initiale = mesurer_performance()
    start_time = time.time()

    try:
        # 1. scan_csv : Crée un plan d'exécution (Lazy) sans charger les données
        lazy_trans = pl.scan_csv(os.path.join(DATASET_DIR, "transactions_data.csv"))
        lazy_users = pl.scan_csv(os.path.join(DATASET_DIR, "users_data.csv"))
        lazy_cards = pl.scan_csv(os.path.join(DATASET_DIR, "cards_data.csv"))
        
        # 2. Définition du plan de jointure (Query Plan)
        # Jointure entre les cartes et les utilisateurs
        lazy_profiles = lazy_cards.join(lazy_users, left_on="client_id", right_on="id")
        
        # Jointure finale avec les transactions
        lazy_final = lazy_trans.join(lazy_profiles, left_on="card_id", right_on="id", how="left")

        # 3. Exécution et écriture via Streaming
        print("Exécution du plan de jointure parallèle...")
        
        lazy_final.sink_csv(OUTPUT_FILE)

        end_time = time.time()
        mem_finale = mesurer_performance()

        print("\n" + "="*30)
        print(f"RESULTATS APPROCHE 2 (POLARS)")
        print(f"Temps écoulé     : {end_time - start_time:.2f} s")
        print(f"Mémoire utilisée : {mem_finale - mem_initiale:.2f} Mo")
        print(f"Mémoire totale   : {mem_finale:.2f} Mo")
        print("="*30)

    except Exception as e:
        print(f"Erreur : {e}")

if __name__ == "__main__":
    resoudre_tp_approche2_parallele()