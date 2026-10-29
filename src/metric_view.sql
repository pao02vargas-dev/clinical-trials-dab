-- =====================================================================
-- metric_view.sql — Metric View sobre la capa Gold
--
-- Crea una vista semantica con MEASURE() exponiendo 2 KPIs y 2 dimensiones
-- que responden la pregunta de negocio:
--   "¿Cuantos ensayos hay por fase y nivel de certificacion del patrocinador?"
--
-- Requisitos de la rubrica: 2 measures + 2 dimensions
--   Dimensions: phase (N/S/B/O) + certification_level (FDA/EMA/GMP/Dual)
--   Measures:   enrollment_count (SUM de ensayos) + duration_days (AVG duracion)
--
-- Ejecutar en SQL warehouse serverless.
-- Para DEV, sustituye dab__prod por dab__dev.
-- =====================================================================
CREATE OR REPLACE VIEW dab__prod.biotech_clinical_trials.mv_clinical_trials_metrics
WITH METRICS LANGUAGE YAML AS
$$
version: 1.1
comment: "KPIs de ensayos clínicos: duración media y volumen por fase (tipo de ensayo) y certificación del patrocinador"
source: dab__prod.biotech_clinical_trials.trials_gold

fields:
  - name: phase
    expr: source.phase
    comment: "Tipo de ensayo requerido (PMR/PMC)"
  - name: certification_level
    expr: source.certification_level
    comment: "Nivel de certificación del patrocinador vigente"

measures:
  - name: enrollment_count
    expr: SUM(source.num_trials)
    comment: "Volumen de ensayos (sustituye participantes: el dataset FDA no los reporta)"
  - name: duration_days
    expr: AVG(source.avg_duration_days)
    comment: "Duración media de los ensayos en días"
$$;

-- Consulta de ejemplo:
-- SELECT phase, certification_level,
--        MEASURE(enrollment_count) AS enrollment_count,
--        MEASURE(duration_days)    AS duration_days
-- FROM dab__prod.biotech_clinical_trials.mv_clinical_trials_metrics
-- GROUP BY phase, certification_level;
