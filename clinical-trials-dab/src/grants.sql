-- Compartir la Metric View de producción (solo lectura).
-- Nota: el principal va con backticks (los emails con caracteres especiales
-- fallan como string literal en este warehouse).
GRANT SELECT ON TABLE dab__prod.biotech_clinical_trials.mv_clinical_trials_metrics
  TO `juan.morice@ulatina.net`;
