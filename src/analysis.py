from utils import stylo
from utils import msclustering
from utils import culler
import setup
from utils import onpnode2vec


def main():
    print("You can run stylo, node2vec and clustering from here. Warning! Node2vec takes several days!")
    print("Checking for requirements.")
    setup.download_levenshtein_data()
    setup.download_onp_data()
    print("Requirements checked.")
    run_stylometry = input("Run stylometry? (y/n): ")
    if run_stylometry == "y":
        culler.culler()
        stylo.run()
    elif run_stylometry == "n":
        print("Skipping stylometry.")
    run_node2vec = input("Run node2vec? (y/n): ")
    if run_node2vec == "y":
        onpnode2vec.run()
    run_clustering = input("Run clustering? (y/n): ")
    if run_clustering == "y":
        msclustering.main()
    if input("Run word coocurrence? (y/n): ") == "y":
        from utils import cw2v
        cw2v.count_results()
    if input("Run old norse style marker analysis? (y/n): ") == "y":
        from utils import stylalyzer
        stylalyzer.main()



if __name__ == "__main__":
    main()