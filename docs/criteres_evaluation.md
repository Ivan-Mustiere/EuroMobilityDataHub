# Critères d'évaluation — RNCP39586 (Bloc 1, 2 & 3)

Extrait de `docs/Grille évaluation Ingénieur en science des données.xlsx` (feuilles « Grille Eval
Bloc1 », « Grille Eval Bloc 2 » et « Grille Eval Bloc 3 »), limité aux trois blocs couverts par ce
dépôt (`docs/Bloc_1.docx`, `docs/Bloc_2.docx`, `docs/Bloc_3.docx`). Résultat noté Acquis / Non
Acquis par compétence.

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

## BLOC 3 — Élaborer et piloter un projet DATA

| Compétence | Description | Livrable attendu | Critères d'évaluation |
|---|---|---|---|
| C3.1.1 | Définir les objectifs et le périmètre du projet (contraintes techniques/réglementaires, contexte, enjeux). | Le cadrage du projet | Identifie : problématique, objectifs et livrables, cadre réglementaire, contraintes et points de vigilance, enjeux RSE le cas échéant. |
| C3.1.2 | Dimensionner le projet (charge de travail, ressources humaines/matérielles, délai/budget). | Le dimensionnement du projet | Comporte : ressources humaines, ressources matérielles/logistiques, chiffrage (coût/délai), analyse de faisabilité. Permet d'atteindre les objectifs qualité/coût/délai du commanditaire. |
| C3.1.3 | Rédiger la documentation projet (parties prenantes, caractéristiques du projet). | La documentation projet | En adéquation avec le cadrage (ex. cahier des charges, spécifications techniques/fonctionnelles). Vocabulaire compréhensible par les parties prenantes. |
| C3.2.1 | Planifier l'exécution (répartition/ordonnancement, planning prévisionnel, personnes en situation de handicap). | Le planning projet | Méthodologie de gestion de projet justifiée (ex. Kanban, Scrum, Lean). Outil de planification compatible (Gantt, rétroplanning). Planning découpé en phases/tâches/lots. Tâches assignées selon compétences (RACI/RASCI), handicap pris en compte. Points de vigilance soulignés (chemin critique, compétences rares). |
| C3.2.2 | Suivre l'avancement (outil de suivi, indicateurs, reporting) afin d'anticiper les aléas. | Un outil de suivi de projet + un tableau de bord | Outil de suivi adapté à la méthodologie choisie. Indicateurs qualitatifs/quantitatifs argumentés. Permettent de suivre avancement, délais, maîtrise des coûts. |
| C3.3.1 | Évaluer les besoins en compétences de l'équipe, plan de développement des compétences. | Un plan de développement des compétences | Compétences à mobiliser identifiées. Grille compétences actuelles/à acquérir commentée. Plan de développement détaillé, formations préconisées, modalités adaptées au handicap. |
| C3.3.2 | Piloter l'équipe projet (affectation des missions, communication, animation managériale, contexte multiculturel). | Les outils de communication et managériaux utilisés | Charge de travail répartie équitablement. Outils collaboratifs et routines managériales détaillés et justifiés. Prise en compte des personnes en situation de handicap. |
| C3.3.3 | Procéder aux arbitrages/réajustements (écarts prévisionnel/réel, outils d'aide à la décision). | La présentation d'un cas d'arbitrage rencontré au cours du projet | Problématique exposée avec conséquences potentielles. Options possibles détaillées. Décision d'arbitrage argumentée, résout la problématique. |
| C3.4.1 | Mettre en place une veille technologique et réglementaire (science des données, IA). | Une méthodologie de veille | Méthodologie de recueil argumentée (bénéfices attendus). Résultat d'une action de veille présenté : impact sur les pratiques métier, avantages/inconvénients de l'évolution. |
| C3.4.2 | Intégrer les enjeux de données responsables (RSE, sécurité, éthique, confidentialité) dans ses pratiques. | Un plan d'actions RSE, sécurité, éthique et confidentialité | Enjeux RSE détaillés. Arbitrages de priorisation précisés/justifiés. Plan d'actions : sujet, action, délai, coût estimé, résultats attendus. |

## Comment ce dépôt répond à chaque critère

- **C1.1.1/C1.2.1/C1.3.1** (stratégies, choix technos) : couverts par le texte du Bloc 1
  (architecture cible).
- **C1.1.2** (techniques de collecte) : `apps/pipeline/ingest.py`, `analysis/queries.sql`.
- **C1.1.3** (automatisation) : cron hebdomadaire réel (`.github/workflows/preprod.yml`) + DAG
  Airflow (`infra/cloud/airflow/dags/pipeline_dag.py`) sur l'EC2 Applicative + flux temps réel Kafka
  (`apps/streaming/producer.py`, poll GTFS-RT SNCF toutes les 2 min).
- **C1.2.2** (base de données + solution Big Data) : DuckDB (réel, local) + Snowflake STAGING/MART
  et bucket S3 bronze réellement déployés et peuplés (`apps/pipeline/load_cloud.py`, voir
  `infra/terraform/snowflake.tf`/`s3.tf`) + RDS PostgreSQL (`fact_realtime`, alimentée en continu
  par `apps/streaming/consumer.py`, voir `infra/terraform/rds.tf`).
- **C1.3.2** (transformation) : `apps/pipeline/transform.py` (local) + `dbt/models/marts/` (Gold sur
  Snowflake — mêmes règles d'harmonisation TGV/TER/Intercités, 5 tests dbt réels).
- **C1.3.3** (ETL + orchestration) : `apps/pipeline/run.py` + `apps/pipeline/load_cloud.py` + `dbt run`,
  orchestrés par le DAG Airflow ci-dessus (`ingest -> transform -> load_cloud -> dbt`) —
  orchestration réelle et vérifiée de bout en bout via le scheduler, pas un pseudo-code d'annexe.
- **C1.4.1** (politique de sécurité) : anonymisation RGPD + clé API + quotas (`apps/api/main.py`), IAM
  least-privilege (`infra/terraform/iam.tf`), secrets réellement stockés et consommés
  (`infra/terraform/secrets.tf`, `rds.tf`) — plus une politique déclarée, un mécanisme qui tourne.
- **C1.4.2** (architecture sécurisée multicouche) : zonage réseau + security groups
  (`infra/terraform/vpc.tf`), chiffrement au repos S3/RDS, chiffrement en transit local
  (`ops/caddy/Caddyfile`, TLS).
- **C2.1.3** (requêtes/calculs) : `analysis/queries.sql`, `analysis/export_results.py`,
  `apps/pipeline/transform.py`.
- **C2.1.4** (tests statistiques) : `analysis/stats_tests.py` (ANOVA H1 + η², Spearman H2, α=0,05).
- **C2.2.1** (visualisation) : `analysis/charts.py` (palette Okabe-Ito, contraste WCAG AA) +
  dashboard Metabase.
- **C2.3.2** (documentation technique) : `README.md` + `infra/README.md` + `docs/Bloc_2.docx` (6.2).
- **C3.2.1/C3.2.2** (planning/suivi) : historique Git réel (dates de commits/PR) + gouvernance de
  branches (`preprod`/`prod` protégées, CI `build-and-smoke-test`) comme outil de suivi, `pytest`
  (52 tests) comme indicateur de stabilité.
- **C3.3.3** (arbitrage) : restriction du périmètre à la SNCF (Bloc 2, 2.2) — décision réelle et
  documentée, prise plutôt que de compléter par des données fictives.
- **C3.4.1** (veille) : Dependabot (dépôt GitHub) + Google Alertes ; résultat concret illustré par
  le choix de DuckDB (Bloc 2, 3.1).
- **C3.4.2** (RSE/sécurité/éthique) : ouverture de l'API à des tiers + anonymisation RGPD
  (`apps/api/main.py`) + accessibilité (`analysis/charts.py`).

Toute modification de ces fichiers doit rester cohérente avec le critère qu'elle sert à démontrer —
un changement qui casse l'anonymisation RGPD, retire l'η²/Spearman, ou abandonne la palette
accessible dégrade directement une compétence évaluée.
