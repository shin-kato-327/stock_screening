# notebooks/

Prototype notebooks from the original (~2y old) project. Kept for
reference only; **not** the runtime path. Production logic now lives
in `src/stock_screening/` and runs from the Airflow DAGs.

## archived/

Logic was already duplicated in DAGs and is now consolidated in `src/`.
Don't run these — they reference the deleted `parameters.json` and
the legacy schema.

- `0_PostgresDB_DDL.ipynb` — superseded by `alembic/versions/`. ⚠ contained
  a leaked password; rotate it independently of this refactor and
  scrub from git history.
- `1_JQUANTS_YahooFinance.ipynb`, `1_jquants.ipynb` — JQuants prototype;
  superseded by `src/stock_screening/jquants/client.py`.
- `2_get_document_list_from_EDINET.ipynb`,
  `3_a_ingestDailyDocumentList.ipynb`,
  `3_b_EDINET_Stocklist.ipynb.ipynb` — EDINET doclist prototype;
  superseded by `src/stock_screening/edinet/client.py` and the
  `edinet_doclist_dag` DAG.

## active/

Reference notebooks that contain unique logic now ported to `src/`.
Useful for one-off exploration; should not be the production path.

- `4_get_documents_from_EDINET.ipynb` — EDINET zip download flow.
- `5_XBRL_Parser.ipynb` — Arelle parsing prototype.
  ⚠ The new parser also captures `concept_id` (XBRL concept QName) and
  accepts `shares` units in addition to JPY.
- `6_Stock_Selection.ipynb` — net-cash-ratio screen prototype.
  ⚠ Two bugs in this notebook were fixed in the refactor: (1) it used
  `負債` (total liabilities) instead of `有利子負債` (interest-bearing
  debt); (2) `itemName LIKE '流動資産%'` double-counted the rollup.
  The new screen reads typed columns from `t_financials_annual`.
