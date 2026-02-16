import unicodedata


def stz_doi(value):
    if not value:
        return ""

    doi = str(value).strip().lower()
    doi = doi.replace("https://doi.org/", "").replace("http://doi.org/", "")
    doi = doi.replace("doi:", "").strip()

    return doi if doi else ""

def stz_title(value):
    if not value:
        return ""

    # Remove accents
    normalized = unicodedata.normalize("NFKD", str(value))
    without_accents = "".join(
        c for c in normalized if not unicodedata.combining(c)
    )

    # Remove spaces, transform to lowercase and strip leading/trailing whitespace
    title = without_accents.replace(" ", "").lower().strip()

    return title if title else ""

def extract_year(value):
    if not value:
        return None

    try:
        year = int(str(value).strip())
        return year
    except ValueError:
        pass

    return None
