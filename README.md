# API Basic

Copy-paste example:

```bash
python3 shop-ops-agent.py --debug 'We sold 100 electronics today - 15 TVs, 50 fridges, 35 washing machines - and we sold-out ALL microwaves. Update stock, give me monthly statistics - and plan the next bulk-buy electronics from our NovaTech partner.'
```

This updates `shop.db`, writes the chronological tool log to `db-fetch-log.md`, and saves the final report to `final-report.md`.

The included sample data is fictional and for local demo purposes only.

Generic tool-call experiment without Python prompt parsing:

```bash
python3 tool-call-experiment.py --debug 'We sold 100 electronics today - 15 TVs, 50 fridges, 35 washing machines - and we sold-out ALL microwaves. Update stock, give me monthly statistics - and plan the next bulk-buy electronics from our NovaTech partner.'
```

This copies `examples/shop-example.db` into a timestamped `runs/` folder, then lets the model drive generic tools without regex-based argument correction.
