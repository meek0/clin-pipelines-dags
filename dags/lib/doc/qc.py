etl_qc = '''
# Tests non critiques

L'échec d'un de ces tests ne bloque pas l'exécution du DAG.

---
Pour plus de détails sur chaque test, voir "Task Instance Details".
'''

no_dup_snv = '''
### Documentation
- Test : Non duplication - Table normalized_snv
- Objectif : Les variants doivent être unique par patient_id dans la table normalized_snv
'''
