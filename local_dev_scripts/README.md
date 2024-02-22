# Local Development Scripts

This section of the repository will contain scripts to aid with development in `pgbelt`.

## generate_large_test_data.py

This script simply generates chunks of INSERTs to the integration test `users` table. It will return a large string.

For easy use, simply redirect output to a file, then load it into your database yourself via PSQL.

```
python3 generate_large_test_data.py > extra_data.sql
```

NOTE: The existing parameters in the script generate a 5GB SQL file and 10000MB of on-disk data to use. This could overwhelm your laptop's Docker engine (you might need to bump your Docker engine allocated memory).
