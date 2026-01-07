import csv
import json
import time
import os
import psutil

def mesurer_performance():
    process = psutil.Process(os.getpid())
    # Mémoire en Mo
    return process.memory_info().rss / (1024 * 1024)

def resoudre_tp_python_streaming():
    BASE_DIR = r"C:\Users\lione\Desktop\TP2"
    DATASET_DIR = os.path.join(BASE_DIR, "datasets")
    OUTPUT_FILE = os.path.join(BASE_DIR, "dataset_unifie_final.csv")
    
    print("--- Démarrage de l'Approche 1 (Version Streaming/Économe) ---")
    
    mem_initiale = mesurer_performance()
    debut_temps = time.time()

    # 1. Chargement des référentiels en RAM
    with open(os.path.join(DATASET_DIR, "mcc_codes.json"), 'r', encoding='utf-8') as f:
        mcc_dict = json.load(f)

    users_dict = {}
    with open(os.path.join(DATASET_DIR, "users_data.csv"), 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            users_dict[row['id']] = row

    cards_profiles = {}
    with open(os.path.join(DATASET_DIR, "cards_data.csv"), 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            u_id = row.get('client_id')
            user_data = users_dict.get(u_id, {})
            row.update(user_data)
            cards_profiles[row['id']] = row

    users_dict = None # Libération RAM

    # 2. Traitement des Transactions en Streaming
    count = 0
    with open(os.path.join(DATASET_DIR, "transactions_data.csv"), 'r', encoding='utf-8') as f_in, \
         open(OUTPUT_FILE, 'w', encoding='utf-8', newline='') as f_out:
        
        reader = csv.DictReader(f_in)
        fieldnames = reader.fieldnames + ['card_brand', 'card_type', 'yearly_income']
        writer = csv.DictWriter(f_out, fieldnames=fieldnames, extrasaction='ignore')
        writer.writeheader()

        for trans in reader:
            c_id = trans.get('card_id')
            profil = cards_profiles.get(c_id, {})
            trans.update(profil) 
            writer.writerow(trans)
            
            count += 1
            if count % 500000 == 0:
                print(f"Progression : {count} transactions...")

    fin_temps = time.time()
    mem_finale = mesurer_performance()

    print("\n" + "="*30)
    print(f"RESULTATS APPROCHE 1 (PYTHON)")
    print(f"Temps écoulé  : {fin_temps - debut_temps:.2f} secondes")
    print(f"Mémoire utilisée : {mem_finale - mem_initiale:.2f} Mo")
    print(f"Mémoire totale pic : {mem_finale:.2f} Mo")
    print("="*30)

if __name__ == "__main__":
    resoudre_tp_python_streaming()