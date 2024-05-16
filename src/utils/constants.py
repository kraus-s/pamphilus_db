# File paths
PAMPHILUS_LATINUS = "data/latin/texts/pamphilus/pamphLat.xml"
LATIN_STOP_WORDS = "data/latin/latinStops.txt"
LATIN_CULL_WORDS = "data/latin/latin_culls.txt"
LATIN_CORPUS_FILES = "data/latin/texts/"
OLD_NORSE_CORPUS_FILES = "data/norseMat/texts/"
ON_STOPS = "data/norseMat/stopwords.txt"
ON_CULLS_LEMMA = "data/cache/on_culls_lemma.p"
ON_CULLS_NORMALIZED = "data/cache/on_culls_normalized.p"
EXCELS = "data/ingest/"
LEVEN_DB = "data/similarities/levenshtein/lev-mem.db"
LEVEN_DB_ON = "data/similarities/levenshtein/lev-on.db"
LEVEN_FOLDER = "data/similarities/levenshtein/"
STYLO_FOLDER = "data/similarities/stylo/"
HANDRIT_MS_DATA = "data/ingest/handrit_msitems.csv"
MIMIR_DATABASE = "data/ingest/mimir_data.db"

PSDG47 = "data/norseMat/texts/DG-4at7-Pamph-para.xml"
VERSEORDER = "data/latin/texts/pamphilus/verseorder.xlsx"
N2V_PARAMETER_PATH = "data/n2v/model-parameters.csv"
N2V_MODELS_PATH = "data/n2v/models/"
N2V_PLOT_PARAMETERS_PATH = "data/n2v/plot-parameters.csv"
N2V_PLOTS_BASE_PATH = "data/n2v/plots/"
ONP_DATABASE_PATH = "data/n2v/onp_data.db"
N2V_TABLES_PATH ="data/n2v/tables/"

WORD_COOCURRENCES_PATH = "data/coocurrences"

STYLE_MARKERS_PATH = "data/style_markers/"
MENOTA_COMPLETE_PICKLE = "data/cache/menota_complete.p"
PAMPHILUS_SAGA_PICKLE = "data/cache/pamph_saga.p"

LATIN_PICKLE = "data/cache/latin_pickle.p"
OLD_NORSE_PICKLE = "data/cache/old_norse_pickle.p"

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
VERSETYPE = "ZZ1_verse"
VERSEINMS = "ZZ3_VersinMS"


# Yet another manual lookup snafu
PAMPHFAMID = 'r9559'
ELISFAMID = 'r6971'
STRENGFAMID = 'r10468'
ALEXSAGA519 = 'r002'


# More lookup snafus

LOCLOOK = {'Bergen': 'https://d-nb.info/gnd/1027742-0',
           'Norway': 'https://d-nb.info/gnd/4042640-3',
           'Iceland': 'https://d-nb.info/gnd/4027754-9',
           'N/A': 'Uncertain',
           "Vadstena, Sweden": "https://d-nb.info/gnd/4389764-2",
           "Vadstena kloster": "https://d-nb.info/gnd/4389764-2"}


# n2v config

DATE_RANGES = [(1200, 1325), (1, 1325), (1, 1536)]
STOPWORD_PATH = "data/ingest/stopwords.txt"
EXCLUDE_LEGAL = [
    "Landslǫg",
    "Lǫgfrǿði",
    "Hirðskrá",
    "Kristinn réttr",
    "Statuta Vilhjalms kardinála",
    "Réttarbǿtr",
    "Grágás",
    "Jónsbók",
    "The Provincial Law of Jutland",
    "Jyske lov",
    "Jyske lovs fortale",
    "Preface",
    "Trolddomskapitlet",
    "Chapter on Sorcery",
    "Valdemars Sjællandske lov",
    "Sjællandske kirkelov",
    "Borgarþingslǫg: Kristinn réttr hinn forni",
    "Borgartings ældste kristenret",
    "Kong Magnus lagabøters Norske landslov med retterbøder",
    "Réttarbǫtr Hákonar Magnússonar",
    "Réttarbǫtr Hákonar Hákonarsonar",
    "Réttarbǫtr Eríks Magnússonar",
    "Réttarbǫtr Magnús Hákonarsonar",
    "Bjarkeyjarréttr",
    "Den nyere bylov",
    "Det færøske saudabrev",
    "Gulaþingslǫg",
    "Valdemar's Provincial Law of Sjælland", 
    "Valdemars Sjællandske lov"
]
EXCLUDE_DIPLOMAS = ["?v95", "?v91", "?v261", "?v375"]

# neo4j default config
NEO4J_URL_LOCAL = "neo4j://localhost:7687"
NEO4J_CREDENTIALS = ('neo4j', '12')


# setup config
ONP_DB_URL = "https://drive.switch.ch/index.php/s/QOG86EdRFjOTya7/download"
LEVENSHTEIN_DB_URL = "https://drive.switch.ch/index.php/s/c3VyEkTut3n7d2j/download"