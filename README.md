PROJET : ANALYSE ET COMPARAISON DES ARCHITECTURES DE TRAITEMENT DE DONNEES

Auteur : Lionel MAFIKA MBOMBO
Master 1 Ingénierie Logicielle

1. Présentation du projet Ce projet consiste en une étude comparative de trois paradigmes d'ingénierie de données appliqués à l'unification de datasets bancaires volumineux. L'objectif est d'évaluer les performances relatives de Python et Java selon trois axes méthodologiques : le streaming séquentiel, le parallélisme vectorisé et le calcul distribué.

2. Architectures étudiées

Approche 1 : Traitement par flux (Streaming) Lecture et écriture ligne par ligne des transactions afin de minimiser l'empreinte mémoire.

Outils : Module csv (Python), BufferedReader (Java).

Cas d'usage : Systèmes à ressources mémoire critiques.

Approche 2 : Parallélisme et Vectorisation Utilisation du multi-threading et de structures de données optimisées pour le calcul local.

Outils : Polars (Python), Parallel Streams (Java).

Cas d'usage : Optimisation du débit sur stations de travail multi-cœurs.

Approche 3 : Calcul Distribué Simulation d'un environnement de cluster avec optimisation des jointures par diffusion.

Outils : Apache Spark (PySpark et Java Spark).

Technique : Broadcast Join pour l'élimination des transferts réseau (Shuffle).

3. Synthèse des résultats de performance

Les tests ont été réalisés sur un processeur AMD Ryzen 3 3250U avec 16 Go de RAM.
