from oca_metrics.utils.normalization import stz_title


def extract_journal_issns(article):
    if not article or not article.journal:
        return []

    journal = article.journal
    issns = set()
    for attr in ["any_issn", "scielo_issn", "electronic_issn", "print_issn"]:
        try:
            issn_value = getattr(journal, attr)
            if issn_value:
                issns.add(issn_value.strip())

        except Exception:
            continue

    return sorted(issns)

def extract_titles(article):
    if not article:
        return []

    titles = set()
    try:
        original_title = article.original_title()
        if original_title:
            titles.add(stz_title(original_title))

    except Exception:
        pass

    try:
        translated_titles = article.translated_titles()
        if translated_titles:
            for lang, title in translated_titles.items():
                if title:
                    titles.add(stz_title(title))

    except Exception:
        pass

    return sorted(titles)

def extract_document_type(article):
    if not article or not article.document_type:
        return ""

    return str(article.document_type).strip()

def extract_journal_title(article):
    if not article or not article.journal:
        return ""

    try:
        return article.journal.title.strip()
    except Exception:
        return ""
