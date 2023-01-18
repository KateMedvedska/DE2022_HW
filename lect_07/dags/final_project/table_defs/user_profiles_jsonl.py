user_profiles_jsonl = {
    "autodetect": False,
    "schema": {
        "fields": [
            {
                "name": "email",
                "type": "STRING",
                "mode": "REQUIRED"
            },
            {
                "name": "full_name",
                "type": "STRING",
                "mode": "REQUIRED"
            },
            {
                "name": "state",
                "type": "STRING",
                "mode": "REQUIRED"
            },
            {
                "name": "birth_date",
                "type": "DATE",
                "mode": "REQUIRED"
            },
            {
                "name": "phone_number",
                "type": "STRING",
                "mode": "REQUIRED"
            },
        ]
    },
    "maxBadRecords": 0,
    "sourceFormat": "NEWLINE_DELIMITED_JSON",
    "sourceUris": [
        (
            "gs://{{ params.data_lake_raw_bucket }}"
            "/user_profiles"
            "/*.jsonl"
        )
    ]
}
