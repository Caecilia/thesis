import requests
from geopy.distance import geodesic
from flask import jsonify
def getQuery(type,lat,lon,radius):
    query="""query{
                       poi(
                           size:%d,
                           filters:[
                               { isLocatedAt: {schema_geo: { _geo_distance: {lng: "%f" , lat: "%f" , distance: "%d" } }}},
                               {rdf_type: {_eq: "https://www.datatourisme.gouv.fr/ontology/core#%s"}}
                           ]
                       ){
                           total
                           results{
                                rdf_type

                               
                               _uri
                               rdfs_label{
                                   value
                                   lang
                               }
                               rdfs_comment{
                                    value
                                    lang
                                }
                                 hasContact{
                                    foaf_homepage
                                    schema_legalName
                                    schema_logo
                                    schema_email
                                    schema_telephone
                                    schema_givenName
                                    schema_familyName

                                }
                               takesPlaceAt{
                                   startTime
                                   endDate
                                   endTime
                                   startDate
                               }
                                hasBeenCreatedBy{
                                    foaf_homepage
                                    schema_legalName
                                    schema_logo
                                    schema_email
                                    schema_telephone
                                    schema_givenName
                                    schema_familyName
                                }
                                
                                isLocatedAt{
                                    schema_address{
                                        schema_addressLocality
                                        schema_postalCode
                                        schema_streetAddress

                                    }
                                    schema_geo{
                                        schema_longitude
                                        schema_latitude

                                    }
                                }
                                
                                
                           }
                       }

                      }""" %(10,float(lon),float(lat),int(radius),type) 
    return query


def getPlacePOI(lat,lon,radius,keyword,type,key):
    
    url="https://maps.googleapis.com/maps/api/place/nearbysearch/json?location={},{}&radius={}&keyword={}&type={}&key={}"
    url=url.format(lat,lon,radius,keyword,type,key)
    response=requests.get(url)
    if response.status_code == 200:
        responseJson=response.json()
        return responseJson
    else:
        return {'error':'Connection Error {}'.format(response.status_code)}


def getDistance(origin , poiLocation):

    ori = (origin['lat'] , origin['lon'])
    poi = (poiLocation['lat'] , poiLocation['lon'])
    return geodesic(ori,poi).km


def getAddress(location):
    url='https://maps.googleapis.com/maps/api/geocode/json?latlng={},{}&language=fr&result_type=locality|plus_code&key=google_key'
    url=url.format(location['lat'],location['lon'])
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()['results'][0]['address_components'][0]['short_name']
    else:
        return jsonify({'error':'function getAddress()'})


def getID(address):
    url='https://geodatamine.fr/boundaries/search?text={}'
    url=url.format(address)
    response=requests.get(url)
    if response.status_code == 200:
        ID=[]
        for obj in response.json():
            ID.append(obj['id'])
        return ID
    else:
        return jsonify({'error':'getID function '})



def getPointOfInterest(type,ID):
    poi=[]
    for id in ID:
        url='https://geodatamine.fr/data/{}/{}?format=geojson&aspoint=true'
        url=url.format(type,id)
        response=requests.get(url)
        if response.status_code == 200:
            poi.append(response.json())
        else:
            return jsonify({'error':'getPointOfInterest'})
    return poi

def exist(id,POIs):
    for obj in POIs:
        if obj['properties']['osm_id'] == id:
            return True
    return False







#https://geodatamine.fr/boundaries/search?text=la%20rochellle
#https://geodatamine.fr/themes
#https://www.gps-coordinates.net/api