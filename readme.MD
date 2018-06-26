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


The (optional) option "chunks_combining_threshold": 10000,` controls how many logs are con`cat`enated into one (the logs are typically <3kB in size and there are thousands of them!) 10000 is the default value and a good value

if you want to clean the statefile, just run the config with `"wipe_statefile": true` parameter, it will ignore remaining parameters, clean statefile and exit


If you want to run daily jobs use this config
```
{
    "#access_key": "key",
    "#secret_key": "secret",
    "bucket_name": "thetradedesk-useast-partner-datafeed",
    "custom_date": "yesterday UTC",
    "prefix": "amnet aud/redf5/date=$custom_date$",
    "categories": ["clicks", "conversions", "impressions", "videoevents"]
}
```

Tables are loaded incrementally (columns are set automatically)
