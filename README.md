# orebot poller (GitHub Actions)

Polls the UEX marketplace for Star Citizen minerals every ~25 min and writes to
Supabase Postgres. Runs as a scheduled GitHub Actions workflow — no server needed.

Retention/rollup is **not** here: it runs natively on Supabase as a `pg_cron` job
(`orebot_retention()`, daily 03:30 UTC).

## One-time setup

1. **Create the repo and push** (from this folder):
   ```bash
   git init && git add -A && git commit -m "orebot poller"
   gh repo create orebot --public --source=. --push      # needs: gh auth login
   # …or create the repo in the GitHub UI and: git remote add origin <url> && git push -u origin main
   ```
   **Use a PUBLIC repo** so Actions minutes are free. The code contains **no
   secrets** (the DB host/username are present but the password is not). If you'd
   rather keep it private, change the cron in `.github/workflows/poll.yml` to
   `"*/30 * * * *"` to stay under the 2,000 free private-repo minutes/month.

2. **Add the two secrets** (Settings → Secrets and variables → Actions → New secret),
   matching the values in your local `~/.config/orebot.env`:
   - `SUPABASE_DB_PASSWORD`
   - `UEX_API_TOKEN`

   Or via CLI:
   ```bash
   gh secret set SUPABASE_DB_PASSWORD
   gh secret set UEX_API_TOKEN
   ```

3. **Trigger a test run:** Actions tab → "poll" → "Run workflow" (or `gh workflow run poll.yml`).
   Then confirm in the dashboard that a new `poll_run` row appeared.

## Notes

- **Schedule drift:** GitHub can delay scheduled runs by several minutes under load.
  Fill detection compares to the previous observation in `listing_state`, so wider
  gaps just mean coarser timing — quantities stay correct.
- **60-day auto-disable:** GitHub disables scheduled workflows after 60 days with no
  repo activity. If you go quiet that long, re-enable it in the Actions tab (or push
  any commit). For a hands-off fix, add a keepalive workflow.
- **Don't commit secrets.** `.gitignore` already excludes `*.env`.
