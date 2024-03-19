from bs4 import BeautifulSoup
import bs4
import glob
import matplotlib.pyplot as plt
from utils import menota_parser
from utils.constants import *


f_list = glob.glob("data/norseMat/PROIEL/*.htm")

soups = []
for i in f_list:
    print("Processing", i)
    with open(i, 'r', encoding="utf-8") as f:
        soup = BeautifulSoup(f, 'html', from_encoding="utf-8")
        soups.append(soup)

all_tokens = []

for i in soups:
    toks = i.find_all(class_="token")
    for tok in toks:
        all_tokens.append(tok)

sent_tok_dict: dict[str, list] = {}
for tok in all_tokens:
    sent_href = tok.get("href")
    sent_id = sent_href.split("/")[-1]
    if sent_id not in sent_tok_dict:
        sent_tok_dict[sent_id] = []
    sent_tok_dict[sent_id].append(tok.get_text())

sent_str_dict: dict[int, str] = {int(k): " ".join(v) for k, v in sent_tok_dict.items()}
ordered_sents = dict(sorted(sent_str_dict.items()))

with open("data/export/ordered_sents.txt", 'w', encoding="utf-8") as f:
    for k, v in ordered_sents.items():
        f.write(f"{v}. ")

lens = [len(v.split()) for k, v in ordered_sents.items()]
print("Max length:", max(lens))
print("Min length:", min(lens))
print("Mean length:", sum(lens) / len(lens))



# plt.hist(lens, bins=10)
# plt.xlabel('Length')
# plt.ylabel('Frequency')
# plt.title('Histogram of Sentence Lengths')
# plt.show()

# plt.plot(lens)
# plt.xlabel('Index')
# plt.ylabel('Length')
# plt.title('Graph of Sentence Lengths')
# plt.show()

pamph = menota_parser.get_parallelized_text(PSDG47)
all_toks = [tok for verse in pamph.verses for tok in verse.tokens]

pos_count = 0
proiel_menota_stuff: list[tuple[str, str]] = []

for k, v in ordered_sents.items():
    for tok in v.split():
        if tok != ".":
            try:
                if all_toks[pos_count].lemma == "því at" and tok == "þui":
                    proiel_menota_stuff.append(("þui at", all_toks[pos_count].lemma))
                    continue
                if all_toks[pos_count].lemma == "því at" and tok == "at":
                    pos_count += 1
                    continue
                if all_toks[pos_count].lemma == "einkaván" and tok == "ENGA":
                    proiel_menota_stuff.append(("ENGA vón", all_toks[pos_count].lemma))
                    continue
                if all_toks[pos_count].lemma == "einkaván" and tok == "vón":
                    pos_count += 1
                    continue
                if all_toks[pos_count].lemma == "konungaveldi" and tok == "konunga":
                    proiel_menota_stuff.append(("konunga vellde", all_toks[pos_count].lemma))
                    continue
                if all_toks[pos_count].lemma == "konungaveldi" and tok == "vellde":
                    pos_count += 1
                    continue
                if all_toks[pos_count].lemma == "nauðsyn":
                    if all_toks[pos_count - 1].lemma == "minn":
                        for ii in "Heldr ger þú þat er ek beiðumsk".split():
                            proiel_menota_stuff.append((ii, all_toks[pos_count].lemma))
                            pos_count += 1
                    continue
                else:
                    proiel_menota_stuff.append((tok, all_toks[pos_count].lemma))
                    pos_count += 1
            except IndexError:
                print("IndexError at", tok)
                break

print(proiel_menota_stuff)
import pdb; pdb.set_trace()