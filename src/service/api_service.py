import requests

class ApiService:
    def __init__(self, base_url):
        """
        Initialize the API service with a base URL.
        :param base_url: The base URL for the API.
        """
        self.base_url = base_url

    def _build_url(self, endpoint):
        """
        Build the full URL by appending the endpoint to the base URL.
        :param endpoint: The specific API endpoint.
        :return: Full URL as a string.
        """
        return f"{self.base_url.rstrip('/')}/{endpoint}"

    def get(self, endpoint, params=None, headers=None):
        """
        Perform a GET request.
        :param endpoint: API endpoint to hit.
        :param params: Dictionary of URL parameters to be sent with the request.
        :param headers: Dictionary of HTTP headers.
        :return: Response object.
        """
        url = self._build_url(endpoint)
        response = requests.get(url, params=params, headers=headers)
        return self._handle_response(response)

    def post(self, endpoint, data=None, json=None, headers=None):
        """
        Perform a POST request.
        :param endpoint: API endpoint to hit.
        :param data: Form data to send in the body of the request.
        :param json: JSON data to send in the body of the request.
        :param headers: Dictionary of HTTP headers.
        :return: Response object.
        """
        url = self._build_url(endpoint)
        response = requests.post(url, data=data, json=json, headers=headers)
        return self._handle_response(response)

    def put(self, endpoint, data=None, json=None, headers=None):
        """
        Perform a PUT request.
        :param endpoint: API endpoint to hit.
        :param data: Form data to send in the body of the request.
        :param json: JSON data to send in the body of the request.
        :param headers: Dictionary of HTTP headers.
        :return: Response object.
        """
        url = self._build_url(endpoint)
        response = requests.put(url, data=data, json=json, headers=headers)
        return self._handle_response(response)

    def patch(self, endpoint, data=None, json=None, headers=None):
        """
        Perform a PATCH request.
        :param endpoint: API endpoint to hit.
        :param data: Form data to send in the body of the request.
        :param json: JSON data to send in the body of the request.
        :param headers: Dictionary of HTTP headers.
        :return: Response object.
        """
        url = self._build_url(endpoint)
        response = requests.patch(url, data=data, json=json, headers=headers)
        return self._handle_response(response)

    def delete(self, endpoint, headers=None):
        """
        Perform a DELETE request.
        :param endpoint: API endpoint to hit.
        :param headers: Dictionary of HTTP headers.
        :return: Response object.
        """
        url = self._build_url(endpoint)
        response = requests.delete(url, headers=headers)
        return self._handle_response(response)

    def _handle_response(self, response):
        """
        Handle the HTTP response.
        :param response: Response object.
        :return: Parsed response in JSON or the raw response content.
        :raise: Exception if the response status code indicates an error.
        """
        if response.status_code in range(200, 300):
            try:
                return response.json()
            except ValueError:
                return response.text
        elif response.status_code == 404:
            return None
        else:
            response.raise_for_status()

# # Example usage:
# api_service = ApiService("https://jsonplaceholder.typicode.com")
# response = api_service.get("posts")
# print(response)