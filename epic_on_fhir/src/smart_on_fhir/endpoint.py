"""
 Class for making requests to the Epic API
    resource: The EPIC on FHIR resource to perform an action on.
    action: The action to perform on the resource.
    data: The data to send with the request.

    Returns a dictionary with the request and response information.
    Note that the resource and action together are defined as the "scope" of the request.
    For example, a GET request to the "Patient" resource with the "12345" action would be defined as:
        scope = "Patient/12345
"""
import requests

class EpicApiRequest:
    def __init__(self, auth, base_url):
        self.auth = auth
        self.base_url = base_url

    def make_request(self, http_method, resource, action, data=None):
        response = getattr(requests, http_method)(f"{self.base_url}{resource}/{action}", data=data, auth=self.auth)
        
        return {'request':
                {
                    'http_method': http_method,
                    'url': f"{self.base_url}{resource}/{action}",
                    'data': ('' if data is None else data)
                },
                'response': {
                    'response_status_code': response.status_code, 
                    'response_time_seconds': (response.elapsed.microseconds / 1000000),
                    'response_headers': response.headers,
                    'response_text': response.text,
                    'response_url': response.url
                }
            }