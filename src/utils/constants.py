# File paths
PAMPHILUS_LATINUS = "data/latin/texts/pamphilus/pamphLat.xml"
LATIN_STOP_WORDS = "data/latin/latinStops.txt"
LATIN_CORPUS_FILES = "data/latin/texts/"
OLD_NORSE_CORPUS_FILES = "data/norseMat/texts/"
ON_STOPS = "data/norseMat/stopwords.txt"
EXCELS = "data/ingest/"
LEVEN_DB = "data/similarities/levenshtein/lev-mem.db"
STYLO_FOLDER = "data/similarities/stylo/"

PSDG47 = "data/norseMat/texts/DG-4at7-Pamph-para.xml"
VERSEORDER = "data/latin/texts/pamphilus/verseorder.xlsx"
N2V_PARAMETER_PATH = "data/n2v/model-parameters.csv"
N2V_MODELS_PATH = "data/n2v/models/"
N2V_PLOT_PARAMETERS_PATH = "data/n2v/plot-parameters.csv"
N2V_PLOTS_BASE_PATH = "data/n2v/plots/"
ONP_DATABASE_PATH = "data/n2v/onp_data.db"
N2V_TABLES_PATH ="data/n2v/tables/"

WORD_COOCURRENCES_PATH = "data/coocurrences"

# For ONP
BASE_URL = "https://onp.ku.dk/onp/onp.php"
TXTLOOKUPDICT = {'B1': 'rx46', 
                'P3': 'rx9', 
                'To': 'rx51', 
                'W1': 'rx24', 
                'Elegia': 'vx10',
                'De contemptu mundi': 'vx24',
                'Amores': 'vx5',
                'Ars Amatoria': 'vx4',
                'Epistulae (vel Heroides)': 'vx11',
                'Ex Ponto': 'vx36',
                'Remedia amoris': 'vx071',
                'Alexandreis': 'vx109',
                'Medicamina faciei femineae': 'vx110',
                'Fasti': 'vx111',
                'Ibis': 'vx112',
                'Metamorphoses': 'vx113',
                'Tristia': 'vx114',
                'Aeneid': 'vx115',
                'Eclogues': 'vx116',
                'Georgicon': 'vx117',
                'De excidio Troiae historia': 'v520'}


# Onto ONO
TXTWITDES = "ZZ02_TxtWit" # NodeLabel
MSSDESC = "E22_Human_Made_Object" # NodeLabel
INFAMDESC = "ZZX" # Rel label
FAMDESC = "ZZ_FAM" # Node Label
FAMLIT = 'Family' # Node Property
AUTHREL = 'XX_AUTHORED' # Rel label
PERSONLABEL = 'XX_HUMAN' # NodeLabel
INTERTEXTREL = 'ZZ_INTERTEXT' # Rel label
NATLANG = 'XX_Language' #Node label
METRE = 'XX_METRIC' # Node label
INLANG = 'XX_HAS_LANUAGE' # Rel label
HASMETRE = 'XX_HAS_METRE' # Rel label
FROMWHERE = 'XX_ISFROM' #Rel label


# Yet another manual lookup snafu
PAMPHFAMID = 'r9559'
ELISFAMID = 'r6971'
STRENGFAMID = 'r10468'
ALEXSAGA519 = 'r002'


# More lookup snafus

LOCLOOK = {'Bergen': 'https://d-nb.info/gnd/1027742-0', 'Norway': 'https://d-nb.info/gnd/4042640-3', 'Iceland': 'https://d-nb.info/gnd/4027754-9', 'N/A': 'Uncertain', "Vadstena, Sweden": "https://d-nb.info/gnd/4389764-2", "Vadstena kloster": "https://d-nb.info/gnd/4389764-2"}


# n2v config

DATE_RANGES = [(1200, 1325), (1, 1325), (1, 1536)]
STOPWORD_PATH = "data/ingest/stopwords.txt"
EXCLUDE_LEGAL = ["Landslǫg", "Lǫgfrǿði", "Hirðskrá", "Kristinn réttr", "Statuta Vilhjalms kardinála", "Réttarbǿtr", "Grágás", "Jónsbók"]
EXCLUDE_DIPLOMAS = ["?v95", "?v91", "?v261", "?v375"]

# neo4j default config
NEO4J_URL_LOCAL = "neo4j://localhost:7687"
NEO4J_CREDENTIALS = ('neo4j', '12')


# setup config
ONP_DB_URL = "https://drive.switch.ch/index.php/s/QOG86EdRFjOTya7"