url = 'https://www.carrefourksa.com/api/v1/menu?latitude=24.7136&longitude=46.6753&lang=en&displayCurr=SAR'

cat_id = response.json()[0]['children']
categories = []
for cat in cat_id:
    categories.append(cat['id'])

prods = response.json()['products']
print('https://www.carrefourksa.com/' + link['links']['productUrl']['href'])

data_json['props']['initialProps']['pageProps']['initialData']['products'][0]['offers'][0]['stores'][0]['price']['discount']['information']['amount']


data = response.css('script[id="__NEXT_DATA__"]').get().replace('<script id="__NEXT_DATA__" type="application/json">', '').replace('</script>', '')

## Pagination
current_page = json_data['props']['initialState']['search']['query']['?currentPage']
json_data['props']['initialState']['search']['numOfPages']

link_url.split("=")[2]

product_listings = response.css('div.css-1itwyrf ::attr(href)').extract()
for links in listings:
    print("https://www.carrefourksa.com/" + links)

with open('carrefour-ksa-items.csv', 'r') as source:
    raw_reader = csv.reader(source)
    header = next(raw_reader, None)
    sorted_data = sorted(raw_reader, key=operator.itemgetter(1))
    with open("output_1.csv", "w") as result:
        writer = csv.writer(result)
        if header:
            writer.writerow(header)
            for r in sort_data:
                writer.writerow((r[0], r[1], r[2], r[4], r[5]))

with open('carrefour-ksa-items.csv', 'r', newline='') as f_input:
    csv_input = csv.DictReader(f_input)
    data = sorted(csv_input, key=lambda row: (row['catalog_uuid']))

    with open('output_1.csv', 'w', newline='') as f_output:    
        csv_output = csv.DictWriter(f_output, fieldnames=csv_input.fieldnames)
        csv_output.writeheader()
        csv_output.writerows(data)

params = {
    'filter': '',
    'sortBy': 'relevance',
    'currentPage': '0',
    'pageSize': '60',
    'maxPrice': '',
    'minPrice': '',
    'areaCode': 'Granada - Riyadh',
    'lang': 'en',
    'displayCurr': 'SAR',
    'latitude': '24.7136',
    'longitude': '46.6753',
    'nextOffset': '0',
    'requireSponsProducts': 'true',
    'responseWithCatTree': 'true',
    'depth': '3',
}

data = (
            response.css('script[id="__NEXT_DATA__"]')
            .get()
            .replace('<script id="__NEXT_DATA__" type="application/json">', "")
            .replace("</script>", "")
        )
        json_data = json.loads(data)
        # current_page = json_data["props"]["initialState"]["search"]["query"][
        #     "?currentPage"
        # ]
        num_of_pages = json_data["props"]["initialState"]["search"]["numOfPages"]
        product_listings = response.css("div.css-1itwyrf ::attr(href)").extract()

        lang = response.meta.get("language")
        cat = response.meta.get("category")
        print(lang, cat)

json_data['props']['initialState']['search']['products']
json_data['props']['initialState']['search']['query']['currentPage']
json_data['props']['initialProps']['pageProps']['query']['?currentPage']
json_data['props']['initialState']['search']['numOfPages']

#####
NFKSA4000000 Electronics & Appliances
NFKSA1200000 Smartphones, Tablets & Wearables
NFKSA2000000 Beauty & Personal Care
NFKSA2300000 Automotive
FKSA1000000 Baby Products
NFKSA3000000 Cleaning & Household
NFKSA5000000 Fashion, Accessories & Luggage
NFKSA8000000 Home & Garden
NFKSA7000000 Health & Fitness
NFKSA1400000 Toys & Outdoor
#####