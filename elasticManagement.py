import logging
from elasticsearch import Elasticsearch
import json


class Manage_data(object):

    def __init__(self, index_name, map_name):
        
        self.elastic_object = Elasticsearch([{'host':'localhost', 'port':9200}])

        if self.elastic_object.ping():
            print("Elastic Search Connected")
        else :
            print("Couldn't Connect to Elastic Search")
        
        self.create_index(index_name, map_name)

    '''
        Checks if an Index exists previously or not..
        If it does not existed the Index with index_name and a map with map_name with 
        the following setting is created
    '''
    def create_index(self,index_name, map_name):

        settings = json.dumps({
            "settings" : {
                "number_of_shards":1,
                "number_of_replicas":0
            }, 
            "mappings" : {
                map_name:{
                    "properties":{
                        "subcity" : {
                            "type":"text",
                            "fielddata": "true",
                            "fields": {
                                "keyword": { 
                                "type": "keyword"
                                }
                            }
                        },
                        "subspecialties":{
                            "type":"text",
                            "fielddata": "true",
                            "fields": {
                                "keyword": { 
                                "type": "keyword"
                                }
                            }
                        },
                        "name":{
                            "type":"text",
                            "fielddata": "true",
                            "fields": {
                                "keyword": { 
                                "type": "keyword"
                                }
                            }
                        },
                        "language":{
                            "type":"text"
                        } ,
                        "certification": {
                            "type":"text"
                        },
                        "overview": {
                            "type":"text"
                        },
                        "officeLocation": {
                            "type" : "nested",
                            "properties":{
                                "cityname": {
                                "type":"text"
                                },
                                "zip": {
                                    "type":"text",
                                    "fielddata": "true",
                                    "fields": {
                                        "keyword": { 
                                        "type": "keyword"
                                        }
                                }
                                } ,
                                "address1": {
                                "type":"text"   
                                } , 
                                "address2":{
                                "type":"text"
                                } , 
                                "longitude":{
                                "type":"integer"
                                } ,
                                "state":{
                                "type":"text"
                                } ,
                                "latitude":{
                                "type":"integer"
                                } 
                            }
                        }, 
                        "hospital_affiliation": {
                            "type":"text"
                        } , 
                        "specialities":{
                            "type":"text",
                            "fielddata": "true",
                            "fields": {
                                "keyword": { 
                                "type": "keyword"
                                }
                            }
                        } , 
                        "licenses": {
                            "type":"text"
                        } ,
                        "years_in_practice": {
                            "type":"text",
                            "fielddata": "true",
                            "fields": {
                                "keyword": { 
                                "type": "keyword"
                                }
                            }
                        } ,
                        "education":{
                            "type":"text"
                        }       
                    }
                }
            }
        })

        try:
            if not self.elastic_object.indices.exists(index_name):
                self.elastic_object.indices.create(index = index_name ,body = settings)
                print("Created Index")
            else:
                print("Index already exists")
        except Exception as ex:
            print(str(ex))
    
    # This method is used to send the information to the ELASTIC SEARCH API
    def store_record(self, index_name, record):
        try:
            outcome = self.elastic_object.index(index = index_name,doc_type= "DoctorInfo", body = record)
        except Exception as ex:
            print("Error in indexing data")
            print(ex)
        
    # This method can be used to get records in a particular index
    def get_record(self,index_name):
        try:
            query = json.dumps({
                "size" : "10000",
                "query":{
                    
                    "match_all":{}
                }
            })

            res = self.elastic_object.search(index=index_name ,body = query)
            return res['hits']['hits']

        except Exception as ex:
            print("Unable to Fetch data")
            print(ex)

    '''
        Year filter will fetch the records lying in a particular range of years of practice
        and return the number of records in that particular range
    '''
    def year_filter(self,index_name,lower_limit, upperlimit):

        try:
            query = json.dumps(
                {"query":{
                "bool": {
                "must": { "match_all": {} },
                "filter": {
                    "range": {
                    "years_in_practice": {
                        "gte": lower_limit,
                        "lte": upperlimit
                    }
                    }
                }
                }
            },
            "_source":["name", "years_in_practice"]})
            res = self.elastic_object.search(index=index_name,  body = query)
            return {"lower_limit_practice":lower_limit, "upperlimit_practice":upperlimit},res["hits"]["total"]

        except:
            pass

    def group_by_city(self,index_name):
        try:
            query = json.dumps({
                "size" : 0,
                "aggs": {
                    "group_by_city": {
                        "terms": {
                            "field": "subcity.keyword"
                        }
                    }
                }
            })

            res = self.elastic_object.search(index = index_name, body = query)
            for x in res["aggregations"]["group_by_city"]["buckets"]:
                print("City : ", str(x["key"]), "Count : ", x["doc_count"])

            return res["aggregations"]["group_by_city"]["buckets"]

        except Exception as ex:
            print(ex)

    def group_by_speciality(self,index_name):
        try:
            query = json.dumps({
                "size" : 0,
                "aggs": {
                    "group_by_speciality": {
                        "terms": {
                            "field": "specialities.keyword"
                        }
                    }
                }
            })

            res = self.elastic_object.search(index = index_name,  body = query)
            for x in res["aggregations"]["group_by_speciality"]["buckets"]:
                print("Speciality : ", str(x["key"]), "Count : ", x["doc_count"])

            return res["aggregations"]["group_by_speciality"]["buckets"]

        except Exception as ex:
            print(ex)

    def group_by_zip(self, index_name):
        try:
            query = json.dumps({
                "size" : 0,
                "aggs": {
                    "group_by_zip": {
                        "terms": {
                            "field": "officeLocation.zip.keyword"
                        }
                    }
                }
            })

            res = self.elastic_object.search(index = index_name, body = query)
            for x in res["aggregations"]["group_by_zip"]["buckets"]:
                print("ZIP : ", str(x["key"]), "Count : ", x["doc_count"])

            return res["aggregations"]["group_by_zip"]["buckets"]

        except Exception as ex:
            print(ex)

if __name__ == "__main__":

    logging.basicConfig(level = logging.ERROR)
    index= "newjerseynewhha"
    elastic_object = Manage_data("newjerseynewhha", "DoctorInfo")
     # Total number of doctors by city
    print("Total number of doctors by city")
    elastic_object.group_by_city(index)

    # Total number of doctors by specialty
    print("Total number of doctors by specialty")
    elastic_object.group_by_speciality(index)  
    
    # Total number of doctors based on their experience range
    print("Total number of doctors based on their experience range")
    print(elastic_object.year_filter(index, 0,4))
    print(elastic_object.year_filter(index, 5,10))
    print(elastic_object.year_filter(index, 11,16))
    print(elastic_object.year_filter(index, 17,20))
    print(elastic_object.year_filter(index, 21,100))
    
    # Total number of doctors by zipcode
    print("Total number of doctors by zipcode")
    elastic_object.group_by_zip(index)

