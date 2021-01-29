from pathlib import Path
from cltk.corpus.utils.importer import CorpusImporter

safeHouse = Path('.')
latMat = Path(__file__).parent / "..\\latMat"
norseMat = Path(__file__).parent / "..\\norseMat"

corpus_importer = CorpusImporter('latin')
corpus_importer.import_corpus('latin_text_latin_library')
corpus_importer.list_corpora

print("Working yet?")