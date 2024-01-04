# **Téma: 2. Analýza Vytíženosti Zastávek a Tras**
**Zadání**: Identifikujte a analyzujte nejvytíženější trasy a zastávky v Praze.
Použijte data o frekvenci polohových záznamů pro určení místa s největším provozem.
Porovnejte vytíženost v různých časech dne a zobrazte výsledky v přehledných grafech.

## Workflow
Během jednoho dne byl spuštěn Kafka stream, a od 7:00-23:00 byly sbírány data o PID v Praze. 
Python skript _PID_Batch.py_ byl spouštěn každé 4 hodiny podle časových oken (7:00-11:00, 11:00-15:00, 15:00-19:00, 19:00-23:00). 
Toho se docílilo díky definování **jobu** v **databricksech**, který se spouštěl každé 4 hodiny od 11:00. 
Data byly zpracovávány podle časového okna od posledního spuštění skriptu do aktuálního času.

Byla využita medailonová architektura, kde se prvně ukládaly data do bronze tabulky. Data se zpracovala, očistila a byla uložena do silver tabulky. 
Následně se agregace uložily do gold tabulek. Následně je z gold tabulek vytvořen dashboard s grafy Top 20 vytížených zastávek/tras. Vizualizace je zde přiložena i jako HTML soubor (_PID_Visualization.html_), který by měl grafy zobrazit.
