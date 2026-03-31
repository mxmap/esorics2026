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
  "generated": "2026-03-30T20:46:12Z",
  "total": 2,
  "municipalities": {
    "10101": {
      "code": "10101",
      "name": "Example municipality",
      "region": "Example region or canton",
      "website": "domain-used-for-website.com",
      "email": "domain-used-for-email.com"
    },
    "10102": {
      "code": "10102",
      "name": "Example municipality 2",
      "region": "Example region or canton",
      "website": "domain-used-for-website.com",
      "email": "domain-used-for-email.com"
    }
  }
}
```


