from glutils import Enricher
import dateparser

with Enricher(["ark", "date"], keep_old_header=True) as enricher:
    for row in enricher:
        iso = dateparser.parse(row["numero"]).date()
        enricher.writerow({
            "ark": row["ark"],
            "date": iso
        })
