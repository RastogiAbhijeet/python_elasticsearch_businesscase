import requests
from bs4 import BeautifulSoup
from user_agent import generate_user_agent
import json
from elasticManagement import Manage_data

'''
    This class is used to handle the information of the doctor
    - A doctor object is created by used the scraped information from the website
    - The object so created is sent of Elastic Search REST API
'''

class Doctor(object):

    def __init__(self, url):

        headers = {'User-Agent': generate_user_agent(device_type="desktop", os=('mac', 'linux'))}
        url = requests.get(url,timeout = 5,headers = headers)
        self.html = BeautifulSoup(url.text, "lxml")
        self.doctor = {}
        self.elastic_object = Manage_data("newjerseyindex", "DoctorInfo")
    
    def buildDoctor(self, subcity):
        script = self.html.find_all("script")
        for x in script:

            if(str(x).__contains__("summon:doctors:maps")):

                y = str(x)
                y = y[y.index("('summon:doctors:maps', function(maps) {") + len("summon:doctors:maps', function(maps) {") :]
                y = y[0:y.index(");")] 
                y = y[y.index("maps(") + len("maps("):]
                
                js = json.loads(y)
                
                self.doctor["name"] = js["full_name"]

                self.doctor["overview"] = js["overview_blurb"]

                self.doctor["education"] = []
                for i in js["education"]["training"]:
                    self.doctor["education"].append(i["institution_long_name"])
                
                self.doctor["certification"] = []
                for i in js["certifications"]:
                    self.doctor["certification"].append(i["name"])

                self.doctor["licenses"] = []
                for i in js["licenses"]:
                    self.doctor["licenses"].append(i["state"] + " Medical License")

                self.doctor["officeLocation"] = js["location"]

                self.doctor["years_in_practice"] = js["years_in_practice"][0]
                
                self.doctor["specialities"] = js["specialty"]["name"]

                self.doctor["subspecialties"] = []
                for i in js["specialty"]["subspecialties"]:
                    self.doctor["subspecialties"].append(i)

                self.doctor["language"] = []
                if(js["languages"] == []):
                    self.doctor["language"].append("English")
                else:
                    for lang in js["languages"]:
                        self.doctor["language"].append("English")


                self.doctor["hospital_affiliation"] = []
                for hospital in js["hospitals"]:
                    self.doctor["hospital_affiliation"].append(hospital["name"])
                
                self.doctor["subcity"] = subcity


                self.elastic_object.store_record("newjerseyindex",self.doctor)
                break
        
