<a name="readme-top"></a>

## Usage

Prepare the configuration on root directory so that the file structure will be:
```markdown
├─ config
│  ├── main.json
├─ dml-executor  <- the executable binary
```
Below are the sample configuration.
```json
{  
  "database": {  
    "host": "127.0.0.1:5432",  
    "db_name": "postgres-db",  
    "type": "postgres"  
  },  
  "job_batch_limit": 2,
  "job_interval": 1,
  "table_name": "sample_table",
  "column_name": "sample_column",
  "job_type": "set_empty"
}
```

You need to change:
- database host
- db_name
- batch limit (rows)
- job interval (in minutes)

Currently, we're supporting for set empty on varchar column.

<p align="right">(<a href="#readme-top">back to top</a>)</p>  