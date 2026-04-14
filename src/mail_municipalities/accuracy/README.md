# Bounce-Probe Accuracy Validation

Validates the provider classifier by sending a single email to a provably
non-existent address at each sampled municipality domain, collecting the NDR
(bounce), and parsing its headers to identify the actual backend MTA.

## Prerequisites

1. **Gmail account** with an App Password (not your regular password - 2FA must be enabled):
   - Go to https://myaccount.google.com/apppasswords
   - Create an app password for "Mail"
   - Enable IMAP: Gmail Settings > Forwarding and POP/IMAP > Enable IMAP

2. **Provider classification output** must exist:
   ```
   output/providers/providers_ch.json
   output/providers/providers_de.json
   output/providers/providers_at.json
   ```

3. **Dependencies** installed:
   ```bash
   uv sync
   ```

## Step 1: Configure credentials

Create a `.env` file in the **project root** (it is already gitignored):

```bash
cp .env.example .env   # or create from scratch
```

Put your Gmail address and the App Password you just generated:

```env
ACCURACY_SMTP_USER=your-account@gmail.com
ACCURACY_SMTP_PASSWORD=abcd efgh ijkl mnop
ACCURACY_IMAP_USER=your-account@gmail.com
ACCURACY_IMAP_PASSWORD=abcd efgh ijkl mnop
```

The SMTP and IMAP passwords are the same App Password (the 16-character code
Google gave you, with or without spaces). Both SMTP (sending) and IMAP
(collecting bounces) use the same Gmail account.

All settings use the `ACCURACY_` prefix. See `config.py` for the full list.
Key defaults:

| Variable                          | Default           | Description                    |
|-----------------------------------|-------------------|--------------------------------|
| `ACCURACY_SMTP_HOST`              | `smtp.gmail.com`  | SMTP server                    |
| `ACCURACY_SMTP_PORT`              | `587`             | SMTP port (STARTTLS)           |
| `ACCURACY_IMAP_HOST`              | `imap.gmail.com`  | IMAP server                    |
| `ACCURACY_IMAP_PORT`              | `993`             | IMAP port (SSL)                |
| `ACCURACY_SEND_RATE_PER_SECOND`   | `1.0`             | Max emails/sec                 |
| `ACCURACY_SEND_BATCH_SIZE`        | `25`              | Emails per batch               |
| `ACCURACY_SEND_BATCH_PAUSE_SECONDS` | `30.0`          | Pause between batches (sec)    |
| `ACCURACY_MAX_PROBES_PER_RUN`     | `100`             | Hard cap per invocation        |
| `ACCURACY_NDR_MAX_WAIT_HOURS`     | `24.0`            | Max time to wait for bounces   |

## Quick smoke test (1-2 domains)

Before running a real sample, verify the full send/collect cycle works with
just 2 probes:

```bash
# 1. Sample only 2 municipalities from CH
uv run accuracy sample ch --size 2 --min-per-class 1

# 2. Check what would be sent
uv run accuracy send --dry-run

# 3. Actually send (only 2 emails)
uv run accuracy send --no-dry-run --max-probes 2

# 4. Wait 1-2 minutes for bounces to arrive, then collect
uv run accuracy collect --poll-once

# 5. Check status — you should see ndr_received > 0
uv run accuracy status

# 6. If NDRs were collected, view the report
uv run accuracy report
```

If `status` still shows `sent: 2, ndr_received: 0` after a few minutes, wait
a bit longer and run `collect --poll-once` again. Some mail servers take
5-10 minutes to generate the bounce.

Once this works, reset the state to start a real run:

```bash
rm output/accuracy/state.db
```

## Step 2: Create a sample

Pick a country and sample size. This creates probe records in a local SQLite
database (`output/accuracy/state.db`) but sends nothing.

```bash
# Sample 50 Swiss municipalities (stratified by provider class)
uv run accuracy sample ch --size 50

# Sample across all three countries
uv run accuracy sample --all --size 200 --min-per-class 10

# Adjust minimum per provider class (default 5)
uv run accuracy sample ch --size 100 --min-per-class 8
```

Review the summary table. Re-running `sample` for the same country is safe --
domains already in the database are skipped.

## Step 3: Verify with a dry run

By default, `send` is in dry-run mode. It prints what would be sent without
touching the network:

```bash
uv run accuracy send --dry-run
```

Check the probe list, domains, and recipient addresses.

## Step 4: Send probes

Disable dry-run explicitly. The tool will ask you to type `YES` before
sending:

```bash
# Interactive confirmation (recommended for first run)
uv run accuracy send --no-dry-run

# Skip the interactive prompt (e.g. on a remote VM)
uv run accuracy send --no-dry-run --confirm

# Limit to 10 probes for an initial test
uv run accuracy send --no-dry-run --max-probes 10

# Override rate limit (default 1/sec)
uv run accuracy send --no-dry-run --rate 0.5
```

The tool rate-limits at 1 email/second and pauses 30 seconds every 25 emails.
Gmail's own limit (~500/day on a free account) acts as an additional backstop.

Each probe sends to `validation-probe-<random>@municipality-domain` -- a
provably non-existent address. The email includes `Auto-Submitted:
auto-generated` (RFC 3834) to prevent auto-reply loops.

## Step 5: Check status

Monitor the probe lifecycle at any time:

```bash
uv run accuracy status
```

Shows counts per status (pending / sent / ndr_received / no_ndr / failed) and
per country.

## Step 6: Collect NDRs

Wait some time for bounces to arrive (minutes to hours), then poll your
Gmail inbox for NDR messages:

```bash
# Single check, then exit
uv run accuracy collect --poll-once

# Keep polling every 5 minutes for up to 12 hours
uv run accuracy collect --wait-hours 12

# Custom poll interval (seconds)
uv run accuracy collect --poll-interval 600 --wait-hours 24
```

The collector matches NDRs to probes via Message-ID, In-Reply-To, and the
UUID embedded in the probe recipient address. Unmatched NDRs are skipped.
Probes without a bounce after `--wait-hours` are marked `no_ndr`.

## Step 7: Generate the accuracy report

```bash
# Console report with confusion matrix and per-class metrics
uv run accuracy report

# Also export a LaTeX table for the paper
uv run accuracy report --latex
```

Output files are written to `output/accuracy/`:
- `accuracy_report.json` -- full metrics as JSON
- `accuracy_report.tex` -- LaTeX table (with `--latex`)

The report shows:
- Overall accuracy
- Response rate (NDRs received / probes sent)
- Per-class precision, recall, F1, support
- Confusion matrix (predicted vs actual)

## Production run protocol

A clean, reproducible validation run.  Gmail free accounts allow ~500 emails
per day, so a 400-probe run fits in a single day.  Target: 400 probes, ~300
responses (~75% response rate), giving +/-5% margin of error at 95% CI for
the combined population of 15,331 municipalities.

### Prerequisites

- Fresh Gmail account with App Password and IMAP enabled
- `.env` configured with the account credentials
- No prior state: `rm -f output/accuracy/state.db`

### Run steps

```bash
# 0. Clean slate
rm -f output/accuracy/*

# 1. Create stratified sample (400 probes across all countries)
#    min-per-class=22 ensures census of all AWS (22) and Google (12)
#    municipalities; rest is proportional (domestic ~259, microsoft ~77)
#    Target: ~300 responses at 75% response rate -> +/-5.6% margin at 95% CI
#    Fits within Gmail free account daily limit of 500.
uv run accuracy sample --all --size 400 --min-per-class 22

# 2. Verify sample distribution
uv run accuracy status

# 3. Dry run — review probe list
uv run accuracy send --dry-run

# 4. Send all 400 probes (4 batches of 100)
uv run accuracy send --no-dry-run --max-probes 100 --confirm
uv run accuracy send --no-dry-run --max-probes 100 --confirm
uv run accuracy send --no-dry-run --max-probes 100 --confirm
uv run accuracy send --no-dry-run --max-probes 100 --confirm

# 5. Verify all sent (pending should be 0, failed should be 0)
uv run accuracy status

# 6. Wait 10-15 minutes for bounces to arrive

# 7. IMPORTANT: Gmail may route some NDRs to Spam.
#    Open Gmail in a browser, go to Spam folder, select all NDR messages
#    (from "Mail Delivery Subsystem" / "mailer-daemon"), and click
#    "Not spam".  Move them to Inbox.  The IMAP collector only searches
#    INBOX by default.

# 8. Collect NDRs (first pass)
uv run accuracy collect --poll-once

# 9. Check response rate
uv run accuracy status

# 10. Wait another 10-15 minutes, check Spam again, collect stragglers
uv run accuracy collect --poll-once

# 11. Final report
uv run accuracy report --latex
```

### Verifying sample validity

After collection, check that the response distribution matches the sample
stratification.  If the Gmail daily limit was hit mid-run, some strata will
have unsent probes (`pending` or `failed` status), invalidating the sample.

```bash
# Check for unsent/failed probes — must all be 0
sqlite3 output/accuracy/state.db \
  "SELECT predicted_provider, status, COUNT(*) FROM probes GROUP BY predicted_provider, status"
```

If any probes are `failed`, reset them and send from another account:

```bash
sqlite3 output/accuracy/state.db "UPDATE probes SET status='pending' WHERE status='failed';"
vim .env   # switch to second Gmail account
uv run accuracy send --no-dry-run --confirm
# Then collect from this account:
uv run accuracy collect --poll-once
# Switch .env back to original account and collect its NDRs too:
vim .env   # switch back
uv run accuracy collect --poll-once
```

## Resumability

The SQLite state database tracks every probe's lifecycle. If the process is
interrupted:
- `sample` skips domains already in the database
- `send` picks up probes still in `pending` status
- `collect` only processes new IMAP messages and skips already-matched probes

You can safely re-run any command.

## Known limitations

### AWS SES inbound relay

Municipalities classified as AWS typically use `inbound-smtp.eu-west-1.amazonaws.com`
as their MX.  The classifier correctly identifies AWS from DNS (MX, ASN, TXT
verification).  However, bounce probes reveal that these domains use AWS SES
purely as an **inbound relay** that forwards to on-premises Exchange (e.g.
`rzpex0x.public.hi-ag.ch` at Hosting Informatik AG).  The NDR is generated by
the backend Exchange server, not by AWS.

This architecture layering -- cloud relay in front of on-prem mailbox hosting --
is invisible to DNS-based classification.  It is a methodological boundary of
the bounce-probe approach rather than a classifier error.  AWS accounts for
~1% of municipalities, so the impact on overall accuracy is small.

When reporting accuracy for the paper, exclude AWS and report per-class metrics
for the three dominant classes (Microsoft, self-hosted, Google) which cover
97% of all municipalities.

## Safety controls

| Control                        | Default | Override                          |
|--------------------------------|---------|-----------------------------------|
| Dry run                        | On      | `--no-dry-run`                    |
| Interactive confirmation       | On      | `--confirm`                       |
| Max probes per run             | 100     | `--max-probes N`                  |
| Rate limit                     | 1/sec   | `--rate N`                        |
| Batch pause                    | 30s/25  | `ACCURACY_SEND_BATCH_*` env vars  |
| Gmail daily limit              | ~500    | (external, not overridable)       |
| RFC 3834 Auto-Submitted header | Always  | (not overridable)                 |
