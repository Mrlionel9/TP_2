import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.broadcast;

public class AppSpark {
    public static void main(String[] args) {
        System.setProperty("hadoop.home.dir", "C:/hadoop");

        String dataPath = "C:/Users/lione/Desktop/TP2/datasets/";
        String outputPath = "C:/Users/lione/Desktop/TP2/Approche3/resultat_java_spark";

        System.out.println("--- Démarrage de l'Approche 3 en Java (Spark) ---");

        Runtime runtime = Runtime.getRuntime();
        runtime.gc();
        long startMem = runtime.totalMemory() - runtime.freeMemory();
        long startTime = System.currentTimeMillis();

        SparkSession spark = SparkSession.builder()
                .appName("TP2_JavaSpark_Metrics")
                .master("local[*]")
                .config("spark.driver.host", "127.0.0.1")
                .getOrCreate();

        spark.sparkContext().setLogLevel("ERROR");

        try {
            Dataset<Row> transactions = spark.read().option("header", "true").option("inferSchema", "true")
                    .csv(dataPath + "transactions_data.csv");

            Dataset<Row> users = spark.read().option("header", "true").option("inferSchema", "true")
                    .csv(dataPath + "users_data.csv");

            Dataset<Row> cards = spark.read().option("header", "true").option("inferSchema", "true")
                    .csv(dataPath + "cards_data.csv");

            // --- CORRECTION DU CONFLIT DE COLONNES ---

            // 1. Jointure Cards + Users
            // On supprime la colonne 'id' de users pour ne pas avoir de doublon avec celle de cards
            // ET on supprime 'client_id' de cards juste après la jointure car elle est identique à 'id' de users
            Dataset<Row> profiles = cards.join(
                    broadcast(users),
                    cards.col("client_id").equalTo(users.col("id")),
                    "inner"
            ).drop(users.col("id")); // Supprime l'ID dupliqué

            // 2. Préparation pour la jointure avec Transactions
            // On renomme l'ID de la carte pour éviter le conflit avec l'ID de la transaction
            Dataset<Row> profilesClean = profiles
                    .withColumnRenamed("id", "card_metadata_id")
                    .drop("client_id"); // On supprime client_id car on a déjà les infos user liées

            // 3. Jointure Finale
            Dataset<Row> result = transactions.join(
                    broadcast(profilesClean),
                    transactions.col("card_id").equalTo(profilesClean.col("card_metadata_id")),
                    "left"
            ).drop("card_metadata_id"); // On nettoie la clé de jointure technique

            // 4. Écriture (Action déclenchante)
            System.out.println("Écriture des résultats optimisés...");
            result.coalesce(1).write().mode("overwrite")
                    .option("header", "true")
                    .csv(outputPath);

            long endTime = System.currentTimeMillis();
            long endMem = runtime.totalMemory() - runtime.freeMemory();

            System.out.println("\n==============================");
            System.out.println("RESULTATS APPROCHE 3 (JAVA SPARK)");
            System.out.printf("Temps d'exécution     : %.2f s\n", (endTime - startTime) / 1000.0);
            System.out.printf("Mémoire vive utilisée : %.2f Mo\n", (endMem - startMem) / (1024.0 * 1024.0));
            System.out.println("==============================");

        } catch (Exception e) {
            System.err.println("ERREUR : " + e.getMessage());
            e.printStackTrace();
        } finally {
            spark.stop();
        }
    }
}