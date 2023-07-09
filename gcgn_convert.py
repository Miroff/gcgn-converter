#/usr/bin/env python

import argparse
import pandas as pd
import camelot
from pypdf import PdfReader

import re
from geojson import FeatureCollection, Feature, Point, dumps
from tqdm import tqdm


"""
Convert degrees and minutes to decimal degrees
"""
def toDecimal(coord):
    match = re.match("(\d+)°([\d\.]+)", coord)
    deg = float(match.group(1))
    min = float(match.group(2))
    return deg + (min / 60)

"""
Each object can be split to multiple rows in DataDrame. This method combines rows ans return list of objects
"""
def combineRows(df):
    ref = None
    name = None
    objType = None
    admRef = None
    geoRef = None
    lat = None
    lon = None
    for index, row in df.iterrows():
        if row[0]:
            if ref:
                yield [ref, name, objType, admRef, geoRef, lat, lon]
            ref = row[0]
            name = row[1]
            objType = row[2]
            admRef = row[3]
            geoRef = None
            (lat, lon) = row[4].split("\n")
            lat = toDecimal(lat)
            lon = toDecimal(lon)
        else:
            if row[3]:
                admRef += " " + row[3]
            if geoRef:
                geoRef += row[4]
            else:
                geoRef = row[4]
    if ref:
        yield [ref, name, objType, admRef, geoRef, lat, lon]    

"""
Convert list of objects to GeoJSON
"""
def convertToGeojson(objects):
    for ref, name, objType, admLoc, geoLoc, lat, lon in objects:
        feature = Feature(geometry=Point((lon, lat)), properties={"ref": ref, "name": name, "type": objType, "administrative_location": admLoc, "geo_location": geoLoc})
        yield feature        
    
def parse(filename):
    reader = PdfReader(filename)
    number_of_pages = len(reader.pages)

    text = reader.pages[0].extract_text()
    text = re.sub("\s+", ' ', text )
    match = re.match(".*Количество записей - (\d+).*", text)
    if match:
        expected_count = int(match.group(1))

    #Read first page because it has different structure
    tables = camelot.read_pdf(filename, flavor='stream', pages='1', table_areas=['0,395,810,27'], columns=['115,320,438,603,752'])
    items = tables[0].df

    #Read other pages
    tables = camelot.read_pdf(filename, flavor='stream', pages="2-%d" % (number_of_pages), table_areas=['0,550,810,27'], columns=['115,320,438,603,752'])
    items = pd.concat([items, pd.concat(map(lambda t: t.df, tables))])

    objects = list(combineRows(items))

    if expected_count != len(objects):
        print("WARN: anounced %d objects but found %d" % (expected_count, len(objects)))

    return objects

if __name__ == '__main__':
    parser = argparse.ArgumentParser(prog='gpgn_convert', description='Converts GCGN PDF to GeoJSON')

    parser.add_argument('filenames', nargs='+', help='Input pdf files') 
    parser.add_argument('-o', '--output', help='Output GeoJSON file name', required=True)

    args = parser.parse_args()
    objects = []
    for filename in tqdm(args.filenames):
        objects += parse(filename)

    fc = FeatureCollection(list(convertToGeojson(objects)))
    with open(args.output, 'w') as fh:
        fh.write(dumps(fc))
