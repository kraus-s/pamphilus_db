# Data and WebApp of my PhD

This repository has all the scripts and data that I used and/or created for my PhD.
The WebApp is built in streamlit and has two functions: It is a help for (my) research
and it enables other researchers to easily access the data in the background.

# Installation

The installation requires a few steps. If you want to make use of the neo4j backend you will have
to install an instance of neo4j yourself. There is a free community/desktop version that can be
downloaded directly from the neo4j homepage. Follow the instructions to set up a database and make
sure to either use the credentials stored in the src/utils/constants file as NEO4J_CREDENTIALS or update 
the credentials stored there. Normally you will only need to change the password.

The python side of things can be set up fairly easily using pipenv or pip using `pipenv install` or 
`pip install -r requirements.txt`. Using pipenv is highly recommended.

## Preparing the webapp
If you used pipenv you can simply run `pipenv run setup` to prepare all the data.
Otherwise you will need to run src/utils/setup.py manually.
In either case, make sure to start the neo4j database beforehand.
You can use the webapp without the neo4j backend, too. To do so, simply run streamlit with `python -m streamlit run src/pamphalazyer.py` or with `pipenv run run` if you used pipenv to set up a virtual envirnonment.