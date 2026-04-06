# ============================================================
# .github/workflows/send-email.yml  —  v2.0 (PER-PAGE EMAILS)
#
# Fires the email digest worker at:
#   8:00 AM Dhaka  = 02:00 UTC
#   8:00 PM Dhaka  = 14:00 UTC
#
# NEW in v2:
#   Each run now sends 6 separate emails per user (one per page):
#     1. অগ্রিম পরিশোধ
#     2. ব্যবসা পরিসংখ্যান
#     3. অনুদান
#     4. সহায়তা (pending tasks)
#     5. সমস্যা ও সমাধান
#     6. প্রিমিয়াম জমা
#
#   Users can opt out of individual page emails via the
#   email widget (per-page toggles in email-ui.js v2).
#
# HOW TO SET UP:
#   Go to your GitHub repo → Settings → Secrets and variables → Actions
#   Add these Repository Secrets:
#     - FIREBASE_PROJECT_ID
#     - FIREBASE_CLIENT_EMAIL
#     - FIREBASE_PRIVATE_KEY
#     - GMAIL_USER            ← your Gmail address
#     - GMAIL_APP_PASSWORD    ← 16-character App Password from Google
# ============================================================

name: Email Digest

on:
  schedule:
    - cron: '0 2 * * *'    # 8:00 AM Dhaka (UTC+6)
    - cron: '0 14 * * *'   # 8:00 PM Dhaka (UTC+6)

  # Run manually from the Actions tab for testing
  workflow_dispatch:

jobs:
  send-email:
    runs-on: ubuntu-latest
    timeout-minutes: 15   # raised slightly — 6 emails × N users takes longer

    steps:
      - name: Checkout repository
        uses: actions/checkout@v4

      - name: Set up Node.js
        uses: actions/setup-node@v4
        with:
          node-version: '20'

      - name: Install dependencies
        run: |
          cd email-worker
          npm install

      - name: Run email worker
        env:
          FIREBASE_PROJECT_ID:   ${{ secrets.FIREBASE_PROJECT_ID }}
          FIREBASE_CLIENT_EMAIL: ${{ secrets.FIREBASE_CLIENT_EMAIL }}
          FIREBASE_PRIVATE_KEY:  ${{ secrets.FIREBASE_PRIVATE_KEY }}
          GMAIL_USER:            ${{ secrets.GMAIL_USER }}
          GMAIL_APP_PASSWORD:    ${{ secrets.GMAIL_APP_PASSWORD }}
        run: node email-worker/send-email.js
