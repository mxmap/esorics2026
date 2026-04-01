# Paper: municipality and emails

## Domains

Each country will have its own file:

```bash
domains/at.json
domains/ch.json
domains/de.json
```

In each file, the following structure will be used:

```json
{
  "generated": "2026-04-01T13:31:57.930979Z",
  "total": 2115,
  "municipalities": [
    {
      "code": "1",
      "name": "Gemeinde A",
      "region": "Kanton Zürich",
      "website": "domain-used-for-website.ch",
      "emails": [
        "domain-used-for-emails.ch"
      ]
    },
    {
      "code": "2",
      "name": "Gemeinde B",
      "region": "Kanton Zürich",
      "website": "domain-used-for-website.ch",
      "emails": [
        "domain-used-for-emails.ch"
      ]
    }
    ]
}
```

## Adding overrides

If you find a municipality that has an incorrect domain, you can add an override for it. 
These will then have the highest priority when generating the final list of domains.

```bash
data/ch/overrides.json
data/at/overrides.json
data/de/overrides.json
```

