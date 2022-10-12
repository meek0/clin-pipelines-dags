etl_qa = '''
# Tests critiques

L'échec d'un de ces tests bloque l'exécution du DAG.

## Série de tests sur la non duplication des variants dans les tables

### Fonctionnement des tests
- Regrouper les variants selon la clé (chromosome, start, reference, alternate) et un critère supplémentaire selon le cas
- Afficher les variants dupliqués

### Différents tests
- Table normalized_snv
- Table normalized_variants
- Table variants
- Table variant_centric

## Série de tests comparant la liste des variants entre les tables

### Fonctionnement des tests
- Lister les variants distincts selon la clé (chromosome, start, reference, alternate)
- Comparer la liste de deux tables

### Différents tests
- Entre les tables normalized_snv et normalized_variants
- Entre les tables normalized_snv et variants
- Entre les tables variants et variant_centric

---
Pour plus de détails sur chaque test, voir "Task Instance Details".
'''

no_dup_snv = '''
### Documentation
- Test : Non duplication - Table normalized_snv
- Objectif : Les variants doivent être unique par patient_id dans la table normalized_snv
'''

no_dup_nor_variants = '''
### Documentation
- Test : Non duplication - Table normalized_variants
- Objectif : Les variants doivent être unique par batch_id dans la table normalized_variants
'''

no_dup_variants = '''
### Documentation
- Test : Non duplication - Table variants
- Objectif : Les variants doivent être unique dans la table variants
'''

no_dup_variant_centric = '''
### Documentation
- Test : Non duplication - Table variant_centric
- Objectif : Les variants doivent être unique dans la table variant_centric
'''

no_dup_varsome = '''
### Documentation
- Test : ...
- Objectif : ...
'''

same_list_snv_nor_variants = '''
### Documentation
- Test : Liste des variants - Entre les tables normalized_snv et normalized_variants
- Objectif : La liste des variants ayant ad_alt >= 3 dans la table normalized_snv est incluse dans celle de la table normalized_variants
'''

same_list_snv_variants = '''
### Documentation
- Test : Liste des variants - Entre les tables normalized_snv et variants
- Objectif : La liste des variants ayant ad_alt >= 3 dans la table normalized_snv est incluse dans celle de la table variants
'''

same_list_variants_variant_centric = '''
### Documentation
- Test : Liste des variants - Entre les tables variants et variant_centric
- Objectif : La liste des variants dans la table variants est la même que dans la table variant_centric
'''