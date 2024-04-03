# Extract and parse jobs in real time with Bytewax

This repository has a simple script to extract job listings from the job search API and parse the job descriptions to extract the required skills in real time. 

## Set up

1. Create a virtual environment and install the requirements:

```bash
conda create -n bytewax-env python=3.10
conda activate bytewax-env
pip install -r requirements.txt
```

2. Create your job search api key 

Visit https://rapidapi.com/letscrape-6bRBa3QguO5/api/jsearch and select the free plan. You will need to create an account to get the api key.

3. Create a `source.env` file in the root directory of the project and add the following:

```bash
api_key = "YOUR_API_KEY"
```

Alternatively, you can set the environment variable `api_key` in your terminal.

## Run the data flow

```bash
python dataflow.py
```