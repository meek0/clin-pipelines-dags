etl_qc = '''
# Tests non critiques

L'échec d'un de ces tests ne bloque pas l'exécution du DAG.

## Série de tests validant les filtres sur les variants

### Différents filtres appliqués
- Table normalized_snv : Les variants du fichier VCF sont filtrés pour ne garder que les variants dont alternate != *
- Table normalized_variants : Les fréquences sont calculées uniquement si gq >= 20, filter contient PASS, ad_alt >= 3 et alternate != *

### Fonctionnement des tests
- Filtrer les variants selon le filtre applicable
- Vérifier qu’aucun variant indésirable n'est dans les tables
- Vérifier qu’aucun variant n’a été oublié ou est en trop pour le calcul des fréquences selon gq, filters, ad_alt et alternate

### Différents tests
- Filtre alternate sur la table normalized_snv
- Filtre gq, filters, ad_alt et alternate sur la table normalized_variants (en trop)
- Filtre gq, filters, ad_alt et alternate sur la table normalized_variants (oubliés)

## Série de tests sur la diversité des colonnes des tables centric

### Fonctionnement des tests
- Se positionner dans le schéma de la table
- Parcourir la “white-list” ou exclure la “black-list” des colonnes accessibles directement sous le niveau où on est positionné
- Pour chaque colonne parcourrue, filtrer les données selon le test
- Vérifier que les colonnes respectent le test

### Différents tests
- Les colonnes de données sans null
  - Table variant_centric
  - Table gene_centric
  - Table cnv_centric
- Les colonnes de données entièrement null
  - Table variant_centric
  - Table gene_centric
  - Table cnv_centric
- Les colonnes de données à valeur unique
  - Table variant_centric
  - Table gene_centric
  - Table cnv_centric

## Séries de tests sur l’annotation Varsome

### Différentes règles
- Annoter seulement les variants
  - Appartenant à un panel de gènes
  - Référence <= 200pb et Alternante <= 200pb
  - Rares (freq <=0.01 dans gnomAD) **** TODO ****
- Ne pas ré-annoter les variants déjà annotés, sauf si l'annotation date de plus de 7 jours

### Fonctionnement des tests
- Filtrer les variants appartenant ou non à un panel de gènes ainsi que la longueur de Référence et Alternate à partir de la table variants
- Comparer avec les variants annotés de la table varsome
- Vérifier au besoin les dates de création et/ou d'annotation

### Différents tests
- Les variants annotés
- Les variants non annotés
- Mise-à-jour de l'annotation après 7 jours
- Mise-à-jour de l'annotation pas avant 7 jours

---
Pour plus de détails sur chaque test, voir "Task Instance Details".
'''

filters_snv = '''
### Documentation
- Test : Filtre alternate sur la table normalized_snv
- Objectif : Aucun variant dans la table normalized_snv n’a alternate = *
'''

filters_frequency_extra = '''
### Documentation
- Test : Filtre gq, filters, ad_alt et alternate sur la table normalized_variants (en trop)
- Objectif : Tous les variants qui ne satisfont pas gq >= 20, filters = PASS, ad_alt >= 3 et alternate = * n’ont pas de calcul des fréquences
'''

filters_frequency_missed = '''
### Documentation
- Test : Filtre gq, filters, ad_alt et alternate sur la table normalized_variants (oubliés)
- Objectif : Tous les variants qui satisfont gq >= 20, filters = PASS, ad_alt >= 3 et alternate = * ont un calcul des fréquences
'''

no_null_variant_centric = '''
### Documentation
- Test : Table variant_centric - Les colonnes de données sans null
- Objectif : Les données dans les colonnes spécifiées ne contiennent pas de null
'''

no_null_gene_centric = '''
### Documentation
- Test : Table gene_centric - Les colonnes de données sans null
- Objectif : Les données dans les colonnes spécifiées ne contiennent pas de null
'''

no_null_cnv_centric = '''
### Documentation
- Test : Table cnv_centric - Les colonnes de données sans null
- Objectif : Les données dans les colonnes spécifiées ne contiennent pas de null
'''

only_null_variant_centric = '''
### Documentation
- Test : Table variant_centric - Les colonnes de données entièrement null
- Objectif : Les données dans les colonnes (sauf celles spécifiées) ne sont pas toutes null
'''

only_null_gene_centric = '''
### Documentation
- Test : Table gene_centric - Les colonnes de données entièrement null
- Objectif : Les données dans les colonnes (sauf celles spécifiées) ne sont pas toutes null
'''

only_null_cnv_centric = '''
### Documentation
- Test : Table cnv_centric - Les colonnes de données entièrement null
- Objectif : Les données dans les colonnes (sauf celles spécifiées) ne sont pas toutes null
'''

same_value_variant_centric = '''
### Documentation
- Test : Table variant_centric - Les colonnes de données à valeur unique
- Objectif : Les données dans les colonnes (sauf celles spécifiées) ne sont pas toutes de la même valeur
'''

same_value_gene_centric = '''
### Documentation
- Test : Table gene_centric - Les colonnes de données à valeur unique
- Objectif : Les données dans les colonnes (sauf celles spécifiées) ne sont pas toutes de la même valeur
'''

same_value_cnv_centric = '''
### Documentation
- Test : Table cnv_centric - Les colonnes de données à valeur unique
- Objectif : Les données dans les colonnes (sauf celles spécifiées) ne sont pas toutes de la même valeur
'''

variants_should_be_annotated = '''
### Documentation
- Test : Les variants annotés
- Objectif : La liste des variants dans la table variants appartenant à un panel de gènes dont la longueur de reference ou alternate n'est pas plus grande que 200 est présente dans la table varsome
'''

variants_should_not_be_annotated = '''
### Documentation
- Test : Les variants non annotés
- Objectif : La liste des variants dans la table variants n'appartenant pas à un panel de gènes ou dont la longueur de reference ou alternate est plus grande que 200 n'ont pas d'annotation
'''

variants_should_be_reannotated = '''
### Documentation
- Test : Mise-à-jour de l'annotation après 7 jours
- Objectif : Les variants de la dernière batch (appartenant à un panel de gènes) n'ont pas un updated_on plus vieux que 7 jours dans la table varsome
'''

variants_should_not_be_reannotated = '''
### Documentation
- Test : Mise-à-jour de l'annotation pas avant 7 jours
- Objectif : Les variants de la dernière batch (appartenant à un panel de gènes) n'ont pas été ré-annotés à l'intérieur de 7 jours depuis leur création (created_on = updated_on)
'''