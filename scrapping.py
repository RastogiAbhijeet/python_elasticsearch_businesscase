from bs4 import BeautifulSoup
import urllib2
import requests
from user_agent import generate_user_agent
import threading
import urllib
import time
from doctor import Doctor


'''
    ScraphDoctorInfo(CLASS):
    used to scrap the information for the target website.
    A 3-Layer process is followed to scrap the entire data

    Layer 1 - Fetch the cities in New Jersey
    Layer 2 - Fetch Specialization per City
    Layer 3 - Fetch Doctors per Specilization per City

    In Layer 3 the following doctor's data is sent to the 
    Elastic Search using Elasticsearch Python Api version 6.2.4
'''
class ScrapDoctorInfo(object):
    
    city_specialization_map = {}  

    def __init__(self, city):

        self.parent_domain = "https://health.usnews.com"
        self.url =  self.parent_domain + "/doctors/city-index/" + city
        self.city = city
    

    # Scrapes the urls of the cities in New Jersey
    def scrap_cities(self):        

        # Declaring a user-agent header in order to get permission to access the site 
    
        headers = {'User-Agent': generate_user_agent(device_type="desktop", os=('mac', 'linux'))}
        citypageresponse = requests.get(self.url, timeout = 5, headers = headers)
        print(citypageresponse.status_code)

        # Creating a Html Parsed Soup in order to access html tags
        soup = BeautifulSoup(citypageresponse.text, 'lxml')

        # Parsing the URLs for next level of Scrapping 
        city_urls = []
        for link in soup.find_all('a'):

            if str(link.get("href")).__contains__("/doctors/specialists-index"):
                city_urls.append(self.parent_domain + link.get("href"))
                self.city_specialization_map[link.get("href").split("/")[-1]] = []
                
        return city_urls              
           
    # A multi-threaded module that divides the cities into factors of 4 and 
    # Perform a divide and conquer strategy to explore the depths of the website
    def multithreadScrap(self,urls,depth,targetFunction):
        threading_frequency = 1

        for i in range(0,depth - 3):
            
            t1 = threading.Thread(target = targetFunction, name = "t1", args = (urls[i],))
            t2 = threading.Thread(target = targetFunction, name = "t2", args = (urls[i+1],))
            t3 = threading.Thread(target = targetFunction, name = "t3", args = (urls[i+2],))
            t4 = threading.Thread(target = targetFunction, name = "t4", args = (urls[i+3],))
            
            t1.start()
            t2.start()
            t3.start()
            t4.start()
            
            t1.join()
            t2.join()
            t3.join()
            t4.join()
            time.sleep(threading_frequency)
            
    # Initialized for multi-threading and url division
    def fetchDoctorInfo(self, Map, depth = 13):
        print("here")
        for key in Map.keys():
            if(Map[key] != []):
                self.multithreadScrap(Map[key],depth,self.doctorSoup)

        return self.city_specialization_map
    
    #Here we visit the page with list of doctors - Screenshot 4
    def doctorSoup(self, url):
        
        print("Starting a Thread ", threading.current_thread().name)
        headers = {'User-Agent': generate_user_agent(device_type="desktop", os=('mac', 'linux'))}
        
        # FORM A URL
    
        doctor_pageresponse = requests.get(self.parent_domain + url, timeout = 5, headers = headers)
        print(doctor_pageresponse.status_code)
        soup = BeautifulSoup(doctor_pageresponse.text,"lxml")

        temp_city = url.split("/")[-1]
        for a in soup.find_all("li", {"data-view":"dr-search-card"}):
            
            doc_dom = []
            for x in a.find_all("a"):
                if(x.get("href") not in doc_dom):
                    doc_dom.append(x.get("href"))
            
            doc_url = self.parent_domain + doc_dom[0]
            doctor = Doctor(doc_url)
            doctor.buildDoctor(temp_city)
            

    def scrap_doctor_specialization(self, urls, depth = 13):
        self.multithreadScrap(urls,depth,self.specializationSoup)
        return self.city_specialization_map
      
    def specializationSoup(self, url):
    
        print("Starting a Thread ", threading.current_thread().name)

        headers = {'User-Agent': generate_user_agent(device_type="desktop", os=('mac', 'linux'))}
        specialization_pageresponse = requests.get(url, timeout = 5, headers = headers)
        soup = BeautifulSoup(specialization_pageresponse.text,"lxml")
        
        for link in soup.find_all("a"):
            if (str(link.get("href")).__contains__("/" + self.city + "/" + url.split("/")[-1])):
                if((link.get("href") in self.city_specialization_map[url.split("/")[-1]])):
                    pass
                else :
                    self.city_specialization_map[url.split("/")[-1]].append(link.get("href")) # create a map cities and the next level of URLS






    



