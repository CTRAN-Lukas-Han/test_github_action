import requests
from dagster import ConfigurableResource, get_dagster_logger
import json

class mSET_Conn:
    def __init__(self,username, password):
        self.login_credentials = { "username": username,"password": password }
        self.session = requests.session()
        self.url = "https://msetctran.luminatorusa.com/"
        self.login_endpoint = "api/login"
        self.logout_endpoint = "api/logout"
        self.status_endpoint = "api/status?startIndex=0&results=250&sort=last_connected%20desc&where="
        self.cliprequest_endpoint = "api/cliprequest"
        self.clipstatus_endpoint = "api/clipstatus/"
        self.clipslist_endpoint = 'api/cliplog'
        self.autlog_endpoint = 'api/auteventlog'

    def openConnection(self):
        return self.session.post(self.url + self.login_endpoint, data=json.dumps(self.login_credentials))

    def getSiteStatus(self):
        return self.session.get(self.url + self.status_endpoint)
        
    def getAutLog(self, startIndex, resultsCount):
        params={'startIndex':startIndex,'results':resultsCount,'sort':'event_start desc','where':'event_start >= 1699603200','preparingFactor':0}
        return self.session.get(self.url + self.autlog_endpoint, params=params)
        
    def getClipList(self,clipsList):
        l = ",".join(str(x) for x in clipsList)
        params={'startIndex':0,'results':-1,'orders':'clip_id desc','where': f'event_log_id in ({l})'.format(l) }
        return self.session.get(self.url + self.clipslist_endpoint, params=params)

    def postClipRequest(self,clipRequestOptions):
        """
        clipRequestOptions definition:
         SiteNameList: A list of sites to request a clip
         CameraList: A list of up to 16 boolean values; those set to true will be retrieved from each site
         Duration: Length of the clip(s) in seconds
         EventLabel: A label applied to the clip(s) in mSET
         Notes: Detailed notes specific to the clip(s)
         NotifyUser: If true, and if email is configured in mSET, the user logged in will receive an email upon download of each clip requested
         ClipQuality: True = HQ; False=LT;
         StartTime: The beginning time of the clip(s) in Unix Epoch format
         SegmentLength: For longer clips, this specifies the time for each clip segment in minutes
        """
        logger=get_dagster_logger()
        #logger.info("executing mSET call with json options: " + json.dumps(clipRequestOptions).replace(" ",""))
        req_body = json.dumps(clipRequestOptions).replace(" ","").replace('"false"',"false").replace('"true"',"true").replace('"[',"[").replace(']"',"]")
        r=self.session.post(self.url + self.cliprequest_endpoint, data=req_body,headers={'Content-Type':'application/json'})
        logger.info(r.request.headers)
        logger.info(r.request.body)
        return r
    def getClipStatus(self,clipsList):
        """Supplies a list of clip statuses given a list of clipsList[] clip ids"""
        return self.session.get(self.url + self.clipstatus_endpoint + clipsList.join(","))

    def closeConnection(self):
        return self.session.get(self.url + self.logout_endpoint)
    

class mSETResource(ConfigurableResource):
    username: str
    password: str

    def getSiteStatus(self):
        logger = get_dagster_logger()
        conn = mSET_Conn(username=self.username, password=self.password)
        logger.info("opening connection to mSET")
        conn.openConnection()
        r = conn.getSiteStatus()
        conn.closeConnection()
        logger.info("executed mSET call: response code " + str(r.status_code))
        return r.json()
    
    def postClipRequest(self, clipRequestOptions):
        logger = get_dagster_logger()
        #logger.info("executing mSET call with json options: " + json.dumps(clipRequestOptions))
        conn = mSET_Conn(username=self.username, password=self.password)
        conn.openConnection()
        r = conn.postClipRequest(clipRequestOptions)
        conn.closeConnection()
        logger.info("executed mSET call: response code " + str(r.status_code))
        logger.info("executed mSET call: response body " + r.text)
        return r.status_code
    
    def getClipStatus(self, clipsList):
        conn = mSET_Conn(username=self.username, password=self.password)
        conn.openConnection()
        r = conn.getClipStatus(clipsList)
        conn.closeConnection()
        return r.status_code
    
    def getAutLog(self, startIndex, resultsCount):
        conn = mSET_Conn(username=self.username, password=self.password)
        conn.openConnection()
        r = conn.getAutLog(startIndex, resultsCount)
        conn.closeConnection()
        return r

    def getClipList(self, clipsList):
        conn = mSET_Conn(username=self.username, password=self.password)
        conn.openConnection()
        r = conn.getClipList(clipsList)
        conn.closeConnection()
        return r