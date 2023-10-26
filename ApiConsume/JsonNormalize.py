import pandas as pd
data = [
    {
        "company": "Google",
        "tagline": "Dont be evil",
        "management": {"CEO": "Sundar Pichai"},
        "department": [
            {"name": "Gmail", "revenue (bn)": 123},
            {"name": "GCP", "revenue (bn)": 400},
            {"name": "Google drive", "revenue (bn)": 600},
        ],
    },
    {
        "company": "Microsoft",
        "tagline": "Be What's Next",
        "management": {"CEO": "Satya Nadella"},
        "department": [
            {"name": "Onedrive", "revenue (bn)": 13},
            {"name": "Azure", "revenue (bn)": 300},
            {"name": "Microsoft 365", "revenue (bn)": 300},
        ],
    },

]
result = pd.json_normalize(
    data, "department", ["company", "tagline", ["management", "CEO"]]
)
print(result)
