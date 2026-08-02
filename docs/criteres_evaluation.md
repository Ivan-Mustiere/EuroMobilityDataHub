# Critères d'évaluation — RNCP39586 (Bloc 1 & Bloc 2)

Extrait de `docs/Grille évaluation Ingénieur en science des données.xlsx` (feuilles « Grille Eval
Bloc1 » et « Grille Eval Bloc 2 »), limité aux deux blocs couverts par ce dépôt
(`docs/Bloc_1.docx`, `docs/Bloc_2.docx`). Résultat noté Acquis / Non Acquis par compétence.

## BLOC 1 — Collecter, transformer et sécuriser des données

| Compétence | Description | Livrable attendu | Critères d'évaluation |
|---|---|---|---|
| C1.1.1 | Élaborer une stratégie de collecte de données : données utiles/nécessaires, sources, cadrage. | Une stratégie de collecte de données | Identifie : objectifs de la collecte, données utiles et nécessaires, sources de données, moyens envisagés. |
| C1.1.2 | Mettre en œuvre des techniques de collecte (API externes, bases de données, web crawling, web scraping). | Un exemple de collecte de données | Présente web crawling, web scraping, requêtes SQL, API externes. Pour chaque technique : exhaustivité, exactitude, cadre réglementaire (propriété intellectuelle, droit d'auteur). |
| C1.1.3 | Automatiser la collecte (tâches planifiées et/ou flux temps réel). | Une méthode d'automatisation de collecte | Méthode argumentée (workflows, scripts/librairies, outils d'automation, ordonnanceurs) ; garantit l'actualisation des données. |
| C1.2.1 | Élaborer la stratégie de stockage et un modèle de données adéquat (types de données, usage, volume). | Une stratégie de stockage + un modèle de données | Stratégie répond à l'usage envisagé (disponibilité, analyse, stockage, accessibilité). Modèle de données : organisation, règles d'intégrité, moyens de manipulation. |
| C1.2.2 | Construire une base de données (SQL/NoSQL, SGBD, solution Big Data). | Une base de données + une solution de stockage Big Data | Choix des solutions justifié et adapté à la problématique. Organisation suivant un design pattern adapté. |
| C1.3.1 | Sélectionner les technologies/outils de traitement de données. | Présentation des outils/technologies sélectionnés | Avantages/inconvénients identifiés (ex. Scala, Python, SQL) ; réponse efficace à la problématique. |
| C1.3.2 | Transformer les données (langage de programmation ou outils dédiés type Talend, Spark). | Présentation des données transformées, méthodes et outils | Transformations identifiées : formatage, consolidation, agrégation, profilage, jointure, calcul. Données transformées exploitables. |
| C1.3.3 | Développer un processus ETL (bénéfices de la technologie ETL choisie). | Un processus ETL + solutions d'automatisation/orchestration | Solution ETL justifiée (bénéfices attendus). Processus ETL automatise et orchestre le traitement. |
| C1.4.1 | Définir la politique de sécurisation des données (risques, sensibilité, droits d'accès, RGPD). | Une politique de sécurité des données | Identifie enjeux de sécurité et moyens mis en œuvre (chiffrement, surveillance, sauvegarde, rôles). Garantit l'intégrité des données. |
| C1.4.2 | Concevoir une architecture sécurisée multicouche (contrôles d'accès, chiffrement transit/repos, anonymisation). | Un schéma d'architecture de sécurité | Précise moyens de sécurisation, zones de sécurité + plan d'adressage, flux d'échanges. Assure la protection des données. |

## BLOC 2 — Analyser, organiser et valoriser des données

| Compétence | Description | Livrable attendu | Critères d'évaluation |
|---|---|---|---|
| C2.1.1 | Analyser les besoins métier/enjeux du commanditaire (entretiens exploratoires, cadrage). | Une analyse du besoin | Identifie : enjeux et problématique, contexte, environnement, contraintes (délai, logistique, coût, technique, réglementaire). |
| C2.1.2 | Définir les axes d'analyse et les métriques (traduire la problématique métier en problème numérique). | Une présentation d'un plan d'analyse | Décrit axes et métriques nécessaires ; traduit la problématique client en problème numérique. |
| C2.1.3 | Réaliser des requêtes et calculs (dashboarding, tableurs, SQL, Python). | Présentation des requêtes et résultats sous forme de dashboard | Plusieurs techniques présentées (SQL, notebook, tableur, dashboard) ; résultats justes et rapides au regard de la problématique. |
| C2.1.4 | Élaborer des modèles statistiques et tests d'hypothèses. | Une méthodologie de tests statistiques | Comporte : formulation d'hypothèse(s), test statistique associé, interprétation des résultats. Permet de valider ou réfuter l'hypothèse initiale. |
| C2.2.1 | Représenter les données (choix de modèles et d'outils adaptés). | La visualisation des résultats de l'analyse | Choix des outils/représentations justifié (lisibilité, facilité d'utilisation). Mise en forme claire et juste pour le public ciblé. Prend en compte les personnes en situation de handicap (formes, contrastes, couleurs). |
| C2.2.2 | Présenter des recommandations argumentées. | Une présentation de recommandations | Présentation structurée, synthétique, argumentée. Aide le commanditaire à la prise de décision. |
| C2.3.1 | Former les utilisateurs aux données et aux outils de visualisation. | Un support de formation | Enjeu/sujet présentés. Support adapté, permet la montée en compétences du public visé. |
| C2.3.2 | Rédiger la documentation technique d'utilisation du système d'analyse de données. | Une documentation technique | Décrit : sources de données (origine, périmètre), méthodes de calcul, description technique/fonctionnelle des indicateurs. Assure compréhension, transmission, reproductibilité. |

## Comment ce dépôt répond à chaque critère

- **C1.1.x / C1.2.x / C1.3.x / C1.4.x** : couverts par le texte du Bloc 1 (stratégie/architecture
  cible) — le code de ce dépôt n'implémente que l'équivalent local (voir `claude.md`), il ne
  démontre pas ces critères par lui-même.
- **C2.1.3** (requêtes/calculs) : `analysis/queries.sql`, `analysis/export_results.py`,
  `pipeline/transform.py`.
- **C2.1.4** (tests statistiques) : `analysis/stats_tests.py` (ANOVA H1 + η², Spearman H2, α=0,05).
- **C2.2.1** (visualisation) : `analysis/charts.py` (palette Okabe-Ito, contraste WCAG AA) +
  dashboard Metabase.
- **C2.3.2** (documentation technique) : `README.md` + `docs/Bloc_2.docx` (6.2).

Toute modification de ces fichiers doit rester cohérente avec le critère qu'elle sert à démontrer —
un changement qui casse l'anonymisation RGPD, retire l'η²/Spearman, ou abandonne la palette
accessible dégrade directement une compétence évaluée.
