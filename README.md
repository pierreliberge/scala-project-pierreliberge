# Scala Template

2 types de fichiers acceptés : 
csv et parquet

## Rapports générés par le traitement Spark

### 🔹 `rapportParClasseDePassager`
**Groupement par** : `Pclass` (classe du passager)  
**Variables créées** :
- `nombreDePassagers` → nombre total de passagers par classe  
- `ageMoyen` → âge moyen des passagers par classe

---

### 🔹 `rapportParSexeEtClasse`
**Groupement par** : `Sex` (sexe) et `Pclass` (classe)  
**Variables créées** :
- `nombreDePassagers` → nombre total de passagers pour chaque combinaison sexe/classe  
- `ageMoyen` → âge moyen pour chaque groupe sexe/classe



