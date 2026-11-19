# Evidencias

Carpeta para las capturas y exports que demuestran el funcionamiento del
pipeline en Airflow, según lo pedido en clase:

- `ejecucion_exitosa/` — captura del Grid/Graph view del DAG con todas las
  tareas en verde, y el CSV final de `dags/data/gold/`.
- `ejecucion_fallida/` — captura de una corrida con
  `co2_simular_fallo=true`, mostrando la tarea en rojo y el log del error.
- `logs/` — export del log de Airflow de ambas corridas (botón "Log" de la
  tarea, o `airflow tasks logs`).
- `historial/` — captura del historial de corridas del DAG (éxitos y
  fallas) desde la vista Grid.
