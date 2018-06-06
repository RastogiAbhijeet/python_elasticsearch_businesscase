from scrapping import ScrapDoctorInfo
from elasticManagement import Manage_data


'''
    Main.py
    - This class is used to control the flow of the overall application
    1. Data is Scraped 
    2. Report is generated
'''

if __name__ == "__main__":

    index = "newjerseyindex"
    map = "DoctorInfo"

    # Initializing the Scraping module
    scrap_obj = ScrapDoctorInfo("new-jersey")

    # SCRAPPING CITIES LAYER - Cities in New-Jersey
    city_urls = scrap_obj.scrap_cities()

    '''
        SCRAPING THE SPECIALIZATION OF DOCTORS PER CITY
        - Generating a map of the cities and the urls of the specialization available in the region 
    '''
    city_specialization_map = scrap_obj.scrap_doctor_specialization(city_urls)
    
    # SCRAPPING THE DOCTOR PAGE AND STORING THE DATA ON THE ELASTIC SEARCH
    scrap_obj.fetchDoctorInfo(city_specialization_map)

    elastic_object = Manage_data(index, map)

    # Summary Report

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
