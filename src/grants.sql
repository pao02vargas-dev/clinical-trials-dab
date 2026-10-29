-- =====================================================================
-- grants.sql — Gobernanza de Unity Catalog (produccion)
--
-- Otorga permisos de solo lectura (SELECT) sobre la Metric View de
-- produccion al usuario evaluador.
--
-- Principio: minimo privilegio.
--   - Solo SELECT en produccion (no dev)
--   - Solo sobre la Metric View (no todas las tablas)
--   - El entorno de desarrollo (dab__dev) no recibe grants externos
--
-- Nota: el principal va con backticks porque los emails con caracteres
-- especiales fallan como string literal en este warehouse.
-- =====================================================================
GRANT SELECT ON TABLE dab__prod.biotech_clinical_trials.mv_clinical_trials_metrics
  TO `juan.morice@ulatina.net`;
