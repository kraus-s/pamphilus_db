from utils.util import load_data
import pickle
from utils.constants import ON_CULLS_LEMMA, ON_CULLS_NORMALIZED

msa_exclude = ["xPE", "xNP", "xNA", "xNO"]
def culler():
    data = load_data()
    normalized_culling_list: set[str] = set()
    lemmatized_culling_list: set[str] = set()
    for i in data:
        if i.msa:
            for ii in i.tokens:
                try:
                    if ii.msa.split()[0] in msa_exclude:
                        normalized_culling_list.add(ii.normalized)
                        lemmatized_culling_list.add(ii.lemma)
                except IndexError:
                    continue

    normalized_culling_list.add("latin.expressjon")
    lemmatized_culling_list.add("latin.expressjon")

    with open(ON_CULLS_NORMALIZED, "wb") as f:
        pickle.dump(normalized_culling_list, f)
    with open(ON_CULLS_LEMMA, "wb") as f:
        pickle.dump(lemmatized_culling_list, f)

if __name__ == "__main__":
    print("Call via setup!")