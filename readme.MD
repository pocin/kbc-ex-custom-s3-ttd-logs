# config

Logs have path `s3://in-bucket/reds5/date=yyyy-mm-dd/hour=hh/<category>_timestamp.log.gz`
```javascript
{
    "#secret_key": "foobar",
    "#access_key": "barbar",
    "bucket_name": "bucket-name",
    "prefix": "path/within/bucket",
    "categories": ["clicks", "conversions", "impressions", "videoevents"]
    "remember_downloaded": true,
    "chunks_combining_threshold": 2000,
    "wipe_statefile": false
}
```

This would download all logs (the actual files) with prefixes `["clicks", "conversions", "impressions", "videoevents"]`
and remmeber the downloaded files in statefile for the next run 


The option `    "chunks_combining_threshold": 2000,` controls how many logs are con`cat`enated into one (the logs are typically <3kB in size and there are thousands of them!)

if you want to clean the statefile, just run the config with `"wipe_statefile": true` parameter, it will ignore remaining parameters, clean statefile and exit

Columns are automatically added to the log columns `col_{0..X}`

Each job overwrites the previous in storage
