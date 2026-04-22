# API Basic

Copy-paste example:

```bash
python3 shop-ops-agent.py --debug 'We sold 100 electronics today - 15 TVs, 50 fridges, 35 washing machines - and we sold-out ALL microwaves. Update stock, give me monthly statistics - and plan the next bulk-buy electronics from our Samsung partner.'
```

This updates `shop.db`, writes the chronological tool log to `db-fetch-log.md`, and saves the final report to `final-report.md`.
