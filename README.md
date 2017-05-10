# bota
BOToArchiver is a simple tool for managing objects in S3 storage, based on boto3 library.
It is meant to be used with 3rd party S3 compliant storage providers.

# Requirements

- python2.7
- ``` pip install boto3 ```

# Configuration

For S3 access credentials configuration, please refer to [Boto3 docs]

# Usage
```
bota.py [-h] S3_HOSTNAME {lsb,ls,put,get} ...

positional arguments:
  S3_HOSTNAME       S3 endpoint hostname

optional arguments:
  -h, --help        show this help message and exit

S3 commands:
  {lsb,ls,put,get}
    lsb             List all available buckets
    ls              List objects in a bucket
    put             upload a file
    get             download a file
```

[Boto3 docs]: http://boto3.readthedocs.io/en/latest/guide/configuration.html
