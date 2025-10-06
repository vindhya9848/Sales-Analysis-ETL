from thefuzz import process

def is_valid_country(name: str, valid_countries: set, threshold: int = 85) -> bool:
    """
    Checks if a country name matches a valid country name using fuzzy matching, since Thaiwan and Taiwan are the same country.
    """
    match, score = process.extractOne(name.lower(), valid_countries)
    if score >=threshold:
        return True
    return False
