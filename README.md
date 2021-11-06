# Steps to run (GCP)

1. Install sankir-sdf
   
```bash
pip3 install sankir-sdf
```

2. Create a `config.json` containing following fields:

```json
{
    "cloud": "AWS|GCP|AZURE",
    "bucket_name":"sankir-storage-prospark",
    "input_path":"data/retail-data/q1",
    "output_path":"processed-retail-data/",
    "reconciliation": "recon.reconciliation"
}
```

3. Set `GOOGLE_APPLICATION_CREDENTIALS` environment variable
```bash
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/creds.json
```

4. Put sankir-sdf in path 

```bash
export PATH=$PATH:$HOME/.local/bin
```
5. Run the program with:

```bash
sankir-sdf /path/to/config.json
```
