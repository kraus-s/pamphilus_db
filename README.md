# Data and WebApp of my PhD

This repository has all the scripts and data that I used and/or created for my PhD.
The WebApp is built in streamlit and has two functions: It is a help for (my) research
and it enables other researchers to easily access the data in the background.

# Installation

I recommend using pipenv `pipenv install` to create a virtual environment for the app and get all dependencies. Tested using python 3.11.5.

## Preparing the webapp
If you used pipenv you can simply run `pipenv run setup` to prepare all the data.
Otherwise you will need to run src/setup.py manually.
Then start the webapp with the command `python -m streamlit run src/pamphalazyer.py` or 
with `pipenv run run` if you used pipenv to set up a virtual envirnonment (remember to activate the shell first).