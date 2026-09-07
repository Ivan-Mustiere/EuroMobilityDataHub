{#
  Reproduit exactement _normalize_station_name_expr() de pipeline/transform.py, pour que le
  rapprochement de noms de gares reste identique entre la version locale (DuckDB) et Snowflake
  (Gold, cf. dim_stations.sql) : majuscules, accents retirés, tirets -> espaces, "ST" -> "SAINT".
  Limite assumée et documentée côté transform.py : gares combinées, gares étrangères ou variantes
  trop éloignées ne sont jamais rapprochées (pas de fuzzy matching).
#}
{% macro normalize_station_name(column) %}
    regexp_replace(
        regexp_replace(
            upper(trim(
                replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(
                    {{ column }},
                    'É', 'E'), 'È', 'E'), 'Ê', 'E'), 'Ë', 'E'), 'À', 'A'), 'Â', 'A'), 'Î', 'I'), 'Ï', 'I'), 'Ô', 'O'), 'Û', 'U')
            )),
            '-', ' '
        ),
        '\\bST\\b', 'SAINT'
    )
{% endmacro %}
