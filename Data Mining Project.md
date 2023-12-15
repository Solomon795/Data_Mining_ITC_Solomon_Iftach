# Data Mining Project

Parsing Research-Gate publications on Energy Market subject
For now we scraped info from search pages only not going to page of every individual
publication (didn't use grequests yet, planning to use it).

## Authors
Solomon Iashuvaev, Iftach Nevo

## Packages required
BS4 (Beautiful Soup) \
requests (with lxml, separate installation) \
selenium \
*grequests (work in progress)

## Goal of the project
Analyze Research-Gate publications on Energy Market subject for score, number of reads, citations, and most popular sub-topics to understand what researchers and readers might be interested in? Are conventional resources are still in trend? What downsides to renewable energy sources? Let's find out

## Project Status
We reached the first milestone, where we able to print some key data for every publication

## Sneak peek at the code (main function)
```python
def main():
    """Costructor function"""
    # Initializing our container for parsed info of publications
    data = []
    # Launching chrome and signing in
    browser, url = get_url()
    sign_in(url, browser)
    # Looping through pages (with finding all pubs on each page)
    for p in range(1, 10):
        print("Page proccessing: ", p)
        url_page = f"https://www.researchgate.net/search/publication?q=Energy-Market&page={p}"
        browser.get(url_page)
        sleep(3)
        pubs = find_all_pubs_on_page(browser)

        # Scraping relevant info of individual publication from given search page
        # and appending it to our list container as a dictionary
        for pub in pubs:
            material = parse_single_pub_material(pub)
            title = parse_single_pub_title(pub)
            site = parse_single_pub_site(pub)
            journal = parse_single_pub_journal(pub)
            authors = parse_single_pub_authors(pub)
            monthyear = parse_single_pub_monthyear(pub)
            reads = parse_single_pub_reads(pub)
            citations = parse_single_pub_citations(pub)
            data.append({"material": material, "title": title, "site": site, "journal": journal,
                         "authors": authors, "month - year": monthyear, "reads": reads,
                         "citations": citations})
        print("Total publications parsed: ", len(data))
    print(*data, sep="\n")
    return data

# returns 'words'
foobar.pluralize('word')

# returns 'geese'
foobar.pluralize('goose')

# returns 'phenomenon'
foobar.singularize('phenomena')
```

## Contributing

Pull requests are welcome. For major changes, please open an issue first
to discuss what you would like to change.

## License

[Israel Tech Challenge (c)](https://www.itc.tech/)