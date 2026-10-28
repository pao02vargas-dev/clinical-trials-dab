-- Permisos a nivel de SCHEMA (no tabla) para demostrar la jerarquia de Unity Catalog.
-- GRANT SELECT ON SCHEMA hereda a todas las tablas, views y metric views del schema,
-- incluyendo trials_gold, mv_clinical_trials_metrics y futuras tablas que se agreguen.
-- Nota: el principal va con backticks (los emails con caracteres especiales
-- fallan como string literal en este warehouse).

GRANT USE SCHEMA ON SCHEMA dab__prod.biotech_clinical_trials
  TO `juan.morice@ulatina.net`;

GRANT SELECT ON SCHEMA dab__prod.biotech_clinical_trials
  TO `juan.morice@ulatina.net`;
