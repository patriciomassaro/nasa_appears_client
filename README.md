# NASA APPEARS Client

This project is a Python client for interacting with the [NASA APPEARS API](https://appeears.earthdatacloud.nasa.gov/api/).

## Getting Started

Install dependencies

```sh
pip install -r requirements.txt
```

Add your credentials to the [JSON](credentials.json).



## Use Cases

See some examples of how to use the client: 

- [Submiting tasks based on a list of locations](nasa_appears_client/use_cases/submit_tasks.py)
- [Deleting submitted tasks](nasa_appears_client/use_cases/delete_tasks.py)
- [Downloading Tiffs from completed submitted tasks](nasa_appears_client/use_cases/download_files.py)
