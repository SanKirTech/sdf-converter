# Steps to run script

1. Install google-cloud-storage Python library with
   
```bash
pip install -r requirements.txt
```

2. Create a `config.json` containing following three fields:
   
```json
{
    "bucket_name": "sankir-1705",
    "input_path": "retail/input/2010-12-01.csv",
    "output_path": "retail/processed"
}
```

3. Set `GOOGLE_APPLICATION_CREDENTIALS` environment variable
```bash
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/creds.json
```

4. Run the script with

```bash
python batch.py /path/to/config.json
```
