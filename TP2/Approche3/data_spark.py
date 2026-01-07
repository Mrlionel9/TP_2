import os
import time
import psutil
from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast

def mesurer_performance():
    process = psutil.Process(os.getpid())
    return process.memory_info().rss / (1024 * 1024)

def executer_approche3_spark():
    # Chemins
    ROOT_DIR = r"C:\Users\lione\Desktop\TP2"
    DATA_DIR = os.path.join(ROOT_DIR, "datasets")
    OUTPUT_DIR = os.path.join(ROOT_DIR, "Approche3", "resultat_python_spark")

    print("--- Démarrage de l'Approche 3 : PySpark (Distribué) ---")
    
    mem_initiale = mesurer_performance()
    start_time = time.time()

    # 1. Initialisation de la Session Spark
    spark = SparkSession.builder \
        .appName("TP2_Spark_Metrics") \
        .master("local[*]") \
        .config("spark.driver.memory", "4g") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("ERROR")

    try:
        # 2. Chargement des données
        df_trans = spark.read.csv(os.path.join(DATA_DIR, "transactions_data.csv"), header=True, inferSchema=True)
        df_users = spark.read.csv(os.path.join(DATA_DIR, "users_data.csv"), header=True, inferSchema=True)
        df_cards = spark.read.csv(os.path.join(DATA_DIR, "cards_data.csv"), header=True, inferSchema=True)

        # 3. Traitement avec Broadcast Join
        
        print("Exécution des jointures optimisées...")
        df_profiles = df_cards.join(broadcast(df_users), df_cards.client_id == df_users.id).drop(df_users.id)
        df_final = df_trans.join(broadcast(df_profiles), df_trans.card_id == df_profiles.id, "left")

        # 4. Action déclenchante (Écriture)
        df_final.coalesce(1).write.mode("overwrite").option("header", "true").csv(OUTPUT_DIR)

        end_time = time.time()
        # On mesure la mémoire après l'effort de calcul
        mem_finale = mesurer_performance()

        print("\n" + "="*30)
        print(f"RESULTATS APPROCHE 3 (PYSPARK)")
        print(f"Temps total (incluant Spark Init) : {end_time - start_time:.2f} s")
        print(f"Pic Mémoire Processus : {mem_finale:.2f} Mo")
        print("="*30)

    finally:
        spark.stop()

if __name__ == "__main__":
    executer_approche3_spark()