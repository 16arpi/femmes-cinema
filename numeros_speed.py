from xml.etree import ElementTree
from urllib.parse import urlencode
from minet.executors import HTTPThreadPoolExecutor
from minet.cli.loading_bar import LoadingBar
from minet.cli.utils import with_loading_bar
from collections import deque
from glutils import Enricher, log

import csv, sys, requests, time

EXPORT_HEADER = ["parent_notice", "ark_periodique", "ark", "numero", "gallica_url", "text_brut"]


@with_loading_bar(
    title="Scraping",
    unit="requests",
    stats=[
        {"name": "queued", "style": "info"},
        {"name": "done", "style": "success"}
    ]
)
def scrape(cli_args, defer, loading_bar: LoadingBar, *args, **kwargs):
    with HTTPThreadPoolExecutor(retry=True) as executor:
        with Enricher(EXPORT_HEADER) as enricher, loading_bar.step():

            # Préparation des URLs pour les périodiques
            urls_periodique = {}
            for row in enricher:
                parent_notice = row["notice"]
                ark_periodique = row["ark"].replace("https://gallica.bnf.fr/", "")
                url_year = "https://gallica.bnf.fr/services/Issues?%s" % urlencode({
                    "ark": ark_periodique
                })

                urls_periodique[url_year] = {
                    "parent_notice": parent_notice,
                    "ark_periodique": ark_periodique
                }

                loading_bar.inc_stat("queued")



            # Récupérations URLs numéros
            urls_numeros = {}
            for res in executor.request(urls_periodique.keys(), throttle=0.7):

                loading_bar.inc_stat("done")
                p_data = urls_periodique[res.url]

                if res.response.status != 200:
                    log("Error", p_data["parent_notice"], res.url, res.error)
                    continue

                xml = ElementTree.fromstring(res.response.text())

                for it in xml:
                    url_year_detailed = "https://gallica.bnf.fr/services/Issues?%s" % urlencode({
                        "ark": p_data["ark_periodique"],
                        "date": it.text
                    })
                    urls_numeros[url_year_detailed] = p_data

                    loading_bar.inc_stat("queued")

            # Récupération des numéros
            for res in executor.request(urls_numeros.keys(), throttle=0.7):
                loading_bar.inc_stat("done")
                p_data = urls_numeros[res.url]

                if res.response.status != 200:
                    log("Error", p_data["parent_notice"], res.url, res.error)
                    continue

                xml = ElementTree.fromstring(res.response.text())

                child_parent_ark = xml.attrib["parentArk"]

                for it in xml:
                    child_ark = p_data["ark_periodique"].replace(child_parent_ark, it.attrib["ark"])
                    enricher.writerow({
                        "parent_notice": row["notice"],
                        "ark_periodique": p_data["ark_periodique"],
                        "ark": child_ark,
                        "numero": it.text,
                        "gallica_url": "https://gallica.bnf.fr/" + child_ark,
                        "text_brut": "https://gallica.bnf.fr/" + child_ark + ".texteBrut"
                    })


scrape({}, None)


