"""
Class to handle authentication
"""

import datetime, jwt, requests, json, zoneinfo
from uuid import uuid4

class EpicApiAuth(requests.auth.AuthBase):
  def __init__(self, 
               client_id, 
               private_key, 
               kid,
               algo,
               auth_location = "https://fhir.epic.com/interconnect-fhir-oauth/oauth2/token"):
    self.__client_id = client_id
    self.__private_key = private_key
    self.__kid = kid
    self.__algo = algo
    self.auth_location = auth_location
    self.__token = None
    self.token_expiry = None

  def get_token(self,
                now = None,
                expiration = None,
                timeout=30):
    now = (now if now is not None else datetime.datetime.now(zoneinfo.ZoneInfo("America/New_York")))
    expiration = (expiration if expiration is not None else datetime.datetime.now(zoneinfo.ZoneInfo("America/New_York")) + datetime.timedelta(minutes=5))
    if self.__token is None or now >= self.token_expiry:
      t = self.generate_token(now, expiration, timeout)
      t.raise_for_status()
      self.__token = json.loads(t.text)
      self.token_expiry = expiration
    return self.__token
  
  def __call__(self, r):
    r.headers['Authorization'] = 'Bearer %s' % self.get_token()['access_token']
    r.headers['Accept'] = 'application/json'
    r.headers['Content-Type'] = 'application/json'
    return r

  """
    Provide authentication to EPIC on FHIR OAuth2 and return valid token
      @param expiration = the datetime when the token expires, default 5 minutes
      @param timeout = seconds to timeout request, default 30 
  """
  def generate_token(self,
                     now = None,
                     expiration = None,
                     timeout=30):
    now = (now if now is not None else datetime.datetime.now(zoneinfo.ZoneInfo("America/New_York")))
    expiration = (expiration if expiration is not None else datetime.datetime.now(zoneinfo.ZoneInfo("America/New_York")) + datetime.timedelta(minutes=5))
    return requests.post(self.auth_location, 
        data= {
        'grant_type': 'client_credentials',
        'client_assertion_type': 'urn:ietf:params:oauth:client-assertion-type:jwt-bearer',
        'client_assertion': jwt.encode(
           {
              'iss': self.__client_id,
              'sub': self.__client_id,
              'aud': self.auth_location,
              'exp': int(expiration.timestamp()),
              'iat': int(now.timestamp()),
              'jti': uuid4().hex,
          },
          self.__private_key,
          algorithm=self.__algo,
          headers={
            'kid': self.__kid,
            'alg': self.__algo,
            'typ': 'JWT',
          })
      }, timeout=timeout)
    
  def can_connect(self):
    return (self.generate_token().status_code == 200)