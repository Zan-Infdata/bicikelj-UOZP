# Bicikelj-UOZP
Implementacija napovedi stanja na postajah Bicikelj žez eno in dve uri. 
Rešitev je bila razvita v sklopu predmeta Uvod v odkrivanje znanj iz podatkov na Uni FRI v študijskem letu 2022/23.

## Uporaba modela
Priprava podatkovn učenje modelov in napovedovanje se požene s poganjanjem skripte `modelV1.py`. Model uporabi podatke iz direktorija `data`, kjer so prisotni učni in testni podatki ter podatki o vremenu iz [arhiva ARSO](https://meteo.arso.gov.si/met/sl/archive/).

Rezultati oziroma napovedi se nahajajo v direktoriju `result`.


## Delovanje

Model s pomočjo razreda `DataPreparation`, ki se nahaja v datoteki `dataPreparation_cls.py`, pripravi podatke in zmodelira nove značilke. Nato za vsako postajo nauči dva modela Linearne regresije, ki napoveta stanje koles na postaji čez eno in čez dve uri.