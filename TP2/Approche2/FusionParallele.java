import java.io.*;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;

public class FusionParallele {
    public static void main(String[] args) {
        String baseDir = "C:\\Users\\lione\\Desktop\\TP2\\";
        String datasetDir = baseDir + "datasets\\";
        String outputFile = baseDir + "Approche2\\dataset_unifie_java_parallel.csv";

        new File(baseDir + "Approche2").mkdirs();
        System.out.println("--- Dmarrage Approche 2 (Parallel) ---");

        Runtime runtime = Runtime.getRuntime();
        runtime.gc();
        long startTime = System.currentTimeMillis();
        long startMem = runtime.totalMemory() - runtime.freeMemory();

        try {
            // 1. Chargement Parallele des referentiels
            Map<String, String> usersMap = chargerMapParallel(datasetDir + "users_data.csv", "id");
            Map<String, String> profiles = lierCartesParallel(datasetDir + "cards_data.csv", usersMap);
            usersMap = null;

            // 2. Fusion Parallele des Transactions
            Path inPath = Paths.get(datasetDir + "transactions_data.csv");
            Path outPath = Paths.get(outputFile);

            try (BufferedReader reader = Files.newBufferedReader(inPath);
                 BufferedWriter writer = Files.newBufferedWriter(outPath)) {

                String header = reader.readLine();
                writer.write(header + ",profile_data\n");
                int cardIdIdx = Arrays.asList(header.split(",")).indexOf("card_id");

                // .parallel() utilise tous les coeurs de ton processeur
                reader.lines().parallel().forEach(line -> {
                    try {
                        String[] vals = line.split(",");
                        String p = profiles.getOrDefault(vals[cardIdIdx], "N/A");
                        synchronized (writer) { // Obligatoire pour ne pas melanger les lignes
                            writer.write(line + "," + p + "\n");
                        }
                    } catch (IOException e) { e.printStackTrace(); }
                });
            }

            long endTime = System.currentTimeMillis();
            long endMem = runtime.totalMemory() - runtime.freeMemory();

            System.out.println("\n========================================");
            System.out.println("PERFORMANCES APPROCHE 2 (PARALLEL)");
            System.out.println("Temps d'execution     : " + (endTime - startTime) / 1000.0 + " s");
            System.out.println("Memoire vive utilisee : " + (endMem - startMem) / (1024 * 1024) + " Mo");
            System.out.println("========================================");

        } catch (Exception e) { e.printStackTrace(); }
    }

    private static Map<String, String> chargerMapParallel(String path, String keyCol) throws IOException {
        Map<String, String> map = new ConcurrentHashMap<>();
        List<String> lines = Files.readAllLines(Paths.get(path));
        int keyIdx = Arrays.asList(lines.get(0).split(",")).indexOf(keyCol);
        lines.parallelStream().skip(1).forEach(l -> {
            String[] v = l.split(",");
            if (v.length > keyIdx) map.put(v[keyIdx], l);
        });
        return map;
    }

    private static Map<String, String> lierCartesParallel(String path, Map<String, String> users) throws IOException {
        Map<String, String> res = new ConcurrentHashMap<>();
        List<String> lines = Files.readAllLines(Paths.get(path));
        String[] h = lines.get(0).split(",");
        int idIdx = Arrays.asList(h).indexOf("id");
        int clientIdx = Arrays.asList(h).indexOf("client_id");
        lines.parallelStream().skip(1).forEach(l -> {
            String[] v = l.split(",");
            res.put(v[idIdx], l + "," + users.getOrDefault(v[clientIdx], "N/A"));
        });
        return res;
    }
}