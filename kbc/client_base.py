'''
Created on 5. 10. 2018

@author: esner
'''
import requests
import json


class HttpClientBase:
    """
    Base class for implementing a single endpoint related to a single entities

    Attributes:
        base_url (str): The base URL for this endpoint.
    """

    def __init__(self, base_url, default_http_header=[]):
        """
        Create an endpoint.

        Args
            root_url (str): Root url of API.

        """
        if not base_url:
            raise ValueError("Base URL is required.")
        self.base_url = base_url
        
        self._auth_header = default_http_header



    def _get_raw(self, url, params=None, **kwargs):
        """
        Construct a requests GET call with args and kwargs and process the
        results.


        Args:
            url (str): requested url
            params (dict): additional url params to be passed to the underlying
                requests.get
            **kwargs: Key word arguments to pass to the get requests.get

        Returns:
            r (requests.Response): object

        Raises:
            requests.HTTPError: If the API request fails.
        """
        headers = kwargs.pop('headers', {})
        headers.update(self._auth_header)

        r = requests.get(url, params, headers=headers, **kwargs)
        try:
            r.raise_for_status()
        except requests.HTTPError:
            # Handle different error codes
            raise Exception('Request failed with code: {}, message: {}'.format(r.status_code, r.text))
        else:
            return r
    
    def get(self, url, params =None, **kwargs):
        r = self._get_raw(url, params, **kwargs)
        return r.json()

    def _post_raw(self, *args, **kwargs):
        """
        Construct a requests POST call with args and kwargs and process the
        results.

        Args:
            *args: Positional arguments to pass to the post request.
            **kwargs: Key word arguments to pass to the post request.

        Returns:
            body: 

        Raises:
            requests.HTTPError: If the API request fails.
        """
        headers = kwargs.pop('headers', {})
        headers.update(self._auth_header)
        r = requests.post(headers=headers, *args, **kwargs)
        try:
            r.raise_for_status()
        except requests.HTTPError:
            # Handle different error codes
            raise
        else:
            return r

    def post(self, *args, **kwargs):
        """
        Construct a requests POST call with args and kwargs and process the
        results.

        Args:
            *args: Positional arguments to pass to the post request.
            **kwargs: Key word arguments to pass to the post request.

        Returns:
            body: json reposonse

        Raises:
            requests.HTTPError: If the API request fails.
        """
        headers = kwargs.pop('headers', {})
        headers.update(self._auth_header)
        r = requests.post(headers=headers, *args, **kwargs)
        try:
            r.raise_for_status()
        except requests.HTTPError:
            # Handle different error codes
            raise
        else:
            return r.json()

    def _patch(self, *args, **kwargs):
        """
        Construct a requests POST call with args and kwargs and process the
        results.

        Args:
            *args: Positional arguments to pass to the post request.
            **kwargs: Key word arguments to pass to the post request.

        Returns:
            body: Response body parsed from json.

        Raises:
            requests.HTTPError: If the API request fails.
        """
        headers = kwargs.pop('headers', {})
        headers.update(self._auth_header)
        r = requests.patch(headers=headers, *args, **kwargs)
        try:
            r.raise_for_status()
        except requests.HTTPError:
            # Handle different error codes
            raise
        else:
            return r
