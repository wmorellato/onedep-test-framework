import asyncio
import logging
import mimetypes
import os
from typing import List, Union, Dict, Optional
import aiohttp
from aiofiles import open as aio_open

from onedep_deposition.models import (
    DepositStatus, Experiment, Deposit, Depositor, 
    DepositedFile, DepositedFilesSet, DepositError, Response
)
from onedep_deposition.enum import Country, EMSubType, FileType
from onedep_deposition.exceptions import DepositApiException, InvalidDepositSiteException
from onedep_deposition.decorators import handle_invalid_deposit_site


class AsyncRestAdapter:
    """Async REST adapter for HTTP requests"""
    
    def __init__(self, hostname: str, api_key: str = '', ver: str = 'v1', 
                 ssl_verify: bool = True, timeout: int = 600, logger: Optional[logging.Logger] = None):
        """
        Constructor for AsyncRestAdapter
        :param hostname: Normally, api.thecatapi.com
        :param api_key: (optional) string used for authentication when POSTing or DELETEing
        :param ver: always v1
        :param ssl_verify: Normally set to True, but if having SSL/TLS cert validation issues, can turn off with False
        :param timeout: (optional) Timeout in seconds for API calls
        :param logger: (optional) If your app has a logger, pass it in here.
        """
        self._logger = logger or logging.getLogger(__name__)
        self._version = ver
        self._hostname = hostname
        self.url = "{}/api/{}/".format(hostname, ver)
        self._api_key = api_key
        self._ssl_verify = ssl_verify
        self._timeout = timeout
        self._session: Optional[aiohttp.ClientSession] = None
        
        if not ssl_verify:
            # Disable SSL warnings for aiohttp
            import ssl
            self._ssl_context = ssl.create_default_context()
            self._ssl_context.check_hostname = False
            self._ssl_context.verify_mode = ssl.CERT_NONE
        else:
            self._ssl_context = None
        
    @property
    def hostname(self) -> str:
        """
        Getter for hostname
        :return: hostname
        """
        return self._hostname

    @hostname.setter
    def hostname(self, hostname: str) -> None:
        """
        Setter for hostname
        :param hostname: hostname
        :return: None
        """
        self._hostname = hostname
        self.url = "{}/api/{}/".format(hostname, self._version)
        
    async def __aenter__(self):
        """Async context manager entry"""
        await self._ensure_session()
        return self
        
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit"""
        await self.close()
        
    async def _ensure_session(self):
        """Ensure session is created"""
        if self._session is None or self._session.closed:
            connector = aiohttp.TCPConnector(
                verify_ssl=self._ssl_verify
            )
            timeout = aiohttp.ClientTimeout(total=self._timeout)
            self._session = aiohttp.ClientSession(
                connector=connector,
                timeout=timeout
            )
    
    async def close(self):
        """Close the session"""
        if self._session and not self._session.closed:
            await self._session.close()
            
    async def _do(self, http_method: str, endpoint: str, params: Dict = None, 
                 data: Union[Dict, List] = None, files: Dict = None, content_type: str = "application/json") -> Response:
        """
        Private method to perform API calls
        :param http_method: GET/POST/DELETE
        :param endpoint: endpoint path
        :param params: Dictionary with requests params
        :param data: Dictionary with request data
        :param files: Files to be uploaded
        :param content_type Request content type
        :return: API Response
        """
        await self._ensure_session()
        
        full_url = self.url + endpoint
        headers = {
            'Authorization': f"Bearer {self._api_key}"
        }
        if content_type:
            headers["Content-Type"] = content_type
            
        log_line_pre = f"method={http_method}, url={full_url}, params={params}"
        log_line_post = ', '.join((log_line_pre, "success={}, status_code={}, message={}"))
        
        try:
            self._logger.debug(msg=log_line_pre)
            
            kwargs = {
                'params': params,
                'headers': headers,
                'ssl': self._ssl_context if not self._ssl_verify else None
            }
            
            if files:
                # For file uploads, use FormData and don't set json content type
                form_data = aiohttp.FormData()
                if data:
                    for key, value in data.items():
                        form_data.add_field(key, str(value))
                
                for field_name, file_info in files.items():
                    file_name, file_obj, mime_type = file_info
                    form_data.add_field(field_name, file_obj, filename=file_name, content_type=mime_type)
                
                kwargs['data'] = form_data
                # Remove Content-Type header for multipart uploads
                if 'Content-Type' in headers:
                    del headers['Content-Type']
            elif data:
                if 'Content-Type' in headers and headers['Content-Type'] == 'application/json':
                    kwargs['json'] = data
                else:
                    kwargs['data'] = data
            
            async with self._session.request(http_method, full_url, **kwargs) as response:
                if response.status == 204:
                    # Django is redirecting 204 to OneDep home page
                    return Response(204)
                    
                is_success = 299 >= response.status >= 200
                log_line = log_line_post.format(is_success, response.status, response.reason)
                
                if not is_success:
                    self._logger.error(msg=log_line)
                    raise DepositApiException(response.reason, response.status)
                
                try:
                    data_out = await response.json()
                except Exception as e:
                    self._logger.error(msg=log_line_post.format(False, None, e))
                    raise DepositApiException("Bad JSON in response", 502) from e
                
                self._logger.debug(msg=log_line)
                
                if 'extras' in data_out and 'code' in data_out:
                    if 'invalid_location' in data_out['code'] and 'base_url' in data_out['extras']:
                        self._logger.warning(msg=f"Invalid deposit site, expected is {data_out['extras']['base_url']}")
                        raise InvalidDepositSiteException(data_out['extras']['base_url'])
                
                return Response(response.status, response.reason, data_out)
                
        except aiohttp.ClientError as e:
            self._logger.error(msg=(str(e)))
            raise DepositApiException("Failed to access the API", 403) from e
    
    async def get(self, endpoint: str, params: Dict = None, content_type: str = "application/json") -> Response:
        """
        Perform GET requests
        :param endpoint: endpoint path
        :param params: Dictionary with requests params
        :param content_type Request content type
        :return: API Response
        """
        return await self._do(http_method='GET', endpoint=endpoint, params=params, content_type=content_type)

    async def post(self, endpoint: str, params: Dict = None, data: Union[Dict, List] = None, 
                   files: Dict = None, content_type: str = "application/json") -> Response:
        """
        Perform POST requests
        :param endpoint: endpoint path
        :param params: Dictionary with requests params
        :param data: Dictionary with requests data
        :param files: Files to be uploaded
        :param content_type Request content type
        :return: API response
        """
        return await self._do(http_method='POST', endpoint=endpoint, params=params, data=data, files=files, content_type=content_type)

    async def delete(self, endpoint: str, params: Dict = None, data: Dict = None, content_type: str = "application/json") -> Response:
        """
        Perform DELETE requests
        :param endpoint: endpoint path
        :param params: Dictionary with requests params
        :param data: Dictionary with requests data
        :param content_type Request content type
        :return: API response
        """
        return await self._do(http_method='DELETE', endpoint=endpoint, params=params, data=data, content_type=content_type)


class AsyncDepositApi:
    """Async Deposit API wrapper"""
    
    def __init__(self, hostname: str = None, api_key: str = '', ver: str = 'v1',
                 ssl_verify: bool = True, redirect: bool = True, logger: logging.Logger = None):
        """
        Constructor method for AsyncDepositAPI wrapper
        :param hostname: Site url
        :param api_key: User public API key
        :param ver: version (usually v1)
        :param ssl_verify: Perform a SSL verification? True for production
        :param redirect: Allow site redirects? True for production
        :param logger: Attach a logger
        """
        self._hostname = hostname or "https://deposit.wwpdb.org/deposition"
        self._api_key = api_key
        self._version = ver
        self._ssl_verify = ssl_verify
        self._logger = logger or logging.getLogger(__name__)
        self._redirect = redirect
        self._rest_adapter: Optional[AsyncRestAdapter] = None
        
    async def __aenter__(self):
        """Async context manager entry"""
        await self._ensure_adapter()
        return self
        
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit"""
        await self.close()
        
    async def _ensure_adapter(self):
        """Ensure rest adapter is created"""
        if self._rest_adapter is None:
            self._rest_adapter = AsyncRestAdapter(
                self._hostname, self._api_key, self._version, 
                self._ssl_verify, logger=self._logger
            )
            await self._rest_adapter._ensure_session()
    
    async def close(self):
        """Close the API connection"""
        if self._rest_adapter:
            await self._rest_adapter.close()
            
    async def _connect(self, hostname: str = None) -> None:
        """Connect to a specific hostname"""
        if hostname:
            self._hostname = hostname
        
        # Close existing adapter if it exists
        if self._rest_adapter:
            await self._rest_adapter.close()
            
        # Create new adapter with new hostname
        self._rest_adapter = AsyncRestAdapter(
            self._hostname, self._api_key, self._version, 
            self._ssl_verify, logger=self._logger
        )
        await self._rest_adapter._ensure_session()

    async def _handle_redirect(self, func, *args, **kwargs):
        """Handle redirect logic for decorated methods"""
        try:
            return await func(*args, **kwargs)
        except InvalidDepositSiteException as e:
            if self._redirect:
                await self._connect(e.site)
                return await func(*args, **kwargs)
            else:
                raise e

    async def create_deposition(self, email: str, users: List[str], country: Country,
                              experiments: List[Experiment], password: str = "", **kwargs) -> Deposit:
        """
        General method to create a deposition passing an Experiment object
        :param email: User e-mail
        :param users: List of ORCID ids that can access this deposition
        :param country: Country from enum list
        :param experiments: List of Experiment objects
        :param password: Password
        :return: Response
        """
        await self._ensure_adapter()
        
        data = {
            "email": email,
            "users": users,
            "country": country.value,
            "experiments": [experiment.json() for experiment in experiments]
        }
        if password:
            data["password"] = password

        async def _create():
            response = await self._rest_adapter.post("depositions/new", data=data)
            response_data = response.data
            response_data["dep_id"] = response_data.pop("id")
            return Deposit(**response_data)
            
        return await self._handle_redirect(_create)

    async def create_em_deposition(self, email: str, users: List[str], country: Country, 
                                 subtype: Union[EMSubType, str], coordinates: bool, 
                                 related_emdb: str = None, password: str = "", **kwargs) -> Deposit:
        """
        Create an EM deposition
        :param email: User e-mail
        :param users: List of ORCID ids that can access this deposition
        :param country: Country from enum list
        :param subtype: EM sub type, accepts enum or string
        :param coordinates: Depositing coordinates file?
        :param related_emdb: Related EMDB id
        :param password: Password
        :return: Response
        """
        experiment = Experiment(exp_type="em", coordinates=coordinates, subtype=subtype, related_emdb=related_emdb)
        return await self.create_deposition(email=email, users=users, country=country, experiments=[experiment], password=password)

    async def create_xray_deposition(self, email: str, users: List[str], country: Country, password: str = "", **kwargs) -> Deposit:
        """
        Create an XRAY deposition
        :param email: User e-mail
        :param users: List of ORCID ids that can access this deposition
        :param country: Country from enum list
        :param password: Password
        :return: Response
        """
        experiment = Experiment(exp_type="xray", coordinates=True)
        return await self.create_deposition(email=email, users=users, country=country, experiments=[experiment], password=password)

    async def create_fiber_deposition(self, email: str, users: List[str], country: Country, password: str = "", **kwargs) -> Deposit:
        """
        Create a Fiber diffraction deposition
        :param email: User e-mail
        :param users: List of ORCID ids that can access this deposition
        :param country: Country from enum list
        :param password: Password
        :return: Response
        """
        experiment = Experiment(exp_type="fiber", coordinates=True)
        return await self.create_deposition(email=email, users=users, country=country, experiments=[experiment], password=password)

    async def create_neutron_deposition(self, email: str, users: List[str], country: Country, password: str = "", **kwargs) -> Deposit:
        """
        Create a Neutron diffraction deposition
        :param email: User e-mail
        :param users: List of ORCID ids that can access this deposition
        :param country: Country from enum list
        :param password: Password
        :return: Response
        """
        experiment = Experiment(exp_type="neutron", coordinates=True)
        return await self.create_deposition(email=email, users=users, country=country, experiments=[experiment], password=password)

    async def create_ec_deposition(self, email: str, users: List[str], country: Country, coordinates: bool, 
                                 password: str = "", related_emdb: str = None, sf_only: bool = False, **kwargs) -> Deposit:
        """
        Create an Electron crystallography deposition
        :param email: User e-mail
        :param users: List of ORCID ids that can access this deposition
        :param country: Country from enum list
        :param coordinates: Depositing coordinates file?
        :param password: Password
        :param related_emdb: Related EMDB id
        :param sf_only: Structure factor only?
        :return: Response
        """
        experiment = Experiment(exp_type="ec", related_emdb=related_emdb, coordinates=coordinates, sf_only=sf_only)
        return await self.create_deposition(email=email, users=users, country=country, experiments=[experiment], password=password)

    async def create_nmr_deposition(self, email: str, users: List[str], country: Country, coordinates: bool, 
                                  password: str = "", related_bmrb: str = None, **kwargs) -> Deposit:
        """
        Create a NMR deposition
        :param email: User e-mail
        :param users: List of ORCID ids that can access this deposition
        :param country: Country from enum list
        :param coordinates: Depositing coordinates file?
        :param password: Password
        :param related_bmrb: Related BMRB id
        :return: Response
        """
        experiment = Experiment(exp_type="nmr", related_bmrb=related_bmrb, coordinates=coordinates)
        return await self.create_deposition(email=email, users=users, country=country, experiments=[experiment], password=password)

    async def create_ssnmr_deposition(self, email: str, users: List[str], country: Country, coordinates: bool, 
                                    password: str = "", related_bmrb: str = None, **kwargs) -> Deposit:
        """
        Create a Solid-state NMR E deposition
        :param email: User e-mail
        :param users: List of ORCID ids that can access this deposition
        :param country: Country from enum list
        :param coordinates: Depositing coordinates file?
        :param password: Password
        :param related_bmrb: Related BMRB id
        :return: Response
        """
        experiment = Experiment(exp_type="ssnmr", related_bmrb=related_bmrb, coordinates=coordinates)
        return await self.create_deposition(email=email, users=users, country=country, experiments=[experiment], password=password)

    async def get_deposition(self, dep_id: str) -> Union[Deposit, None]:
        """
        Get deposition from ID
        :param dep_id: Deposition ID
        :return: Deposit
        """
        await self._ensure_adapter()
        
        async def _get():
            response = await self._rest_adapter.get(f"depositions/{dep_id}")
            response_data = response.data
            response_data['dep_id'] = response_data.pop('id')
            return Deposit(**response_data)
            
        return await self._handle_redirect(_get)

    async def get_all_depositions(self) -> List[Deposit]:
        """
        Get all depositions from an user
        :return: List[Deposit]
        """
        await self._ensure_adapter()
        
        async def _get_all():
            depositions = []
            response = await self._rest_adapter.get("depositions/")
            response_data = response.data
            for deposition_json in response_data["items"]:
                deposition_json['dep_id'] = deposition_json.pop('id')
                deposition = Deposit(**deposition_json)
                depositions.append(deposition)
            return depositions
            
        return await self._handle_redirect(_get_all)

    async def get_users(self, dep_id: str) -> List[Depositor]:
        """
        Get users from deposition
        :param dep_id: Deposition ID
        :return: List[Depositor]
        """
        await self._ensure_adapter()
        
        async def _get_users():
            users = []
            response = await self._rest_adapter.get(f"depositions/{dep_id}/users/")
            response_data = response.data
            for user_json in response_data:
                user_json["user_id"] = user_json.pop("id")
                user = Depositor(**user_json)
                users.append(user)
            return users
            
        return await self._handle_redirect(_get_users)

    async def add_user(self, dep_id: str, orcid: Union[List, str]) -> List[Depositor]:
        """
        Grant access from given users to deposition
        :param dep_id: Deposition ID
        :param orcid: Orcid ID or list of Orcid ids
        :return: List of depositors
        """
        await self._ensure_adapter()
        
        async def _add_user():
            users = []
            data = []
            if isinstance(orcid, str):
                data.append({'orcid': orcid})
            elif isinstance(orcid, list):
                for orcid_id in orcid:
                    data.append({'orcid': orcid_id})
            
            response = await self._rest_adapter.post(f"depositions/{dep_id}/users/", data=data)
            response_data = response.data
            for user_json in response_data:
                user_json["user_id"] = user_json.pop("id")
                users.append(Depositor(**user_json))
            return users
            
        return await self._handle_redirect(_add_user)

    async def remove_user(self, dep_id: str, orcid: str) -> bool:
        """
        Remove access from an user to a deposition
        :param dep_id: Deposition id
        :param orcid: Orcid id
        :return: True if successful
        """
        await self._ensure_adapter()
        
        async def _remove_user():
            await self._rest_adapter.delete(f"depositions/{dep_id}/users/{orcid}")
            return True
            
        return await self._handle_redirect(_remove_user)

    async def upload_file(self, dep_id: str, file_path: str, file_type: Union[str, FileType],
                         overwrite: bool = False) -> DepositedFile:
        """
        Upload a file in a deposition
        :param dep_id: Deposition id
        :param file_path: File path
        :param file_type: Deposition file type
        :param overwrite: If true, overwrite all previously uploaded file with the same type
        :return: File response
        """
        await self._ensure_adapter()
        
        if not os.path.exists(file_path):
            raise DepositApiException("Invalid input file", 404)

        file_type_str = file_type.value if isinstance(file_type, FileType) else file_type
        mime_type, _encoding = mimetypes.guess_type(file_path)
        file_name = os.path.basename(file_path)

        async def _upload():
            data = {
                "name": file_name,
                "type": file_type_str
            }

            if overwrite:
                deposited_files = await self.get_files(dep_id)
                # Remove existing files of the same type concurrently
                remove_tasks = []
                for file in deposited_files:
                    if file.file_type.value == file_type_str:
                        remove_tasks.append(self.remove_file(dep_id, file.file_id))
                
                if remove_tasks:
                    await asyncio.gather(*remove_tasks)

            # Read file asynchronously
            async with aio_open(file_path, "rb") as fp:
                file_content = await fp.read()
                files = {"file": (file_name, file_content, mime_type)}
                
                response = await self._rest_adapter.post(f"depositions/{dep_id}/files/", data=data, files=files, content_type="")
                response_data = response.data
                response_data["file_type"] = response_data.pop("type")
                response_data["file_id"] = response_data.pop("id")
                return DepositedFile(**response_data)
                
        return await self._handle_redirect(_upload)

    async def update_metadata(self, dep_id: str, file_id: int, spacing_x: float, spacing_y: float, spacing_z: float,
                            contour: float, description: str) -> DepositedFile:
        """
        Set metadata in a deposition
        :param dep_id: Deposition ID
        :param file_id: File ID
        :param spacing_x: Pixel spacing X
        :param spacing_y: Pixel spacing Y
        :param spacing_z: Pixel spacing Z
        :param contour: Contour level
        :param description: Description
        :return: DepositedFile
        """
        await self._ensure_adapter()
        
        async def _update_metadata():
            data = {
                "voxel": {
                    "spacing": {
                        "x": spacing_x,
                        "y": spacing_y,
                        "z": spacing_z
                    },
                    "contour": contour
                },
                "description": description
            }

            response = await self._rest_adapter.post(f"depositions/{dep_id}/files/{file_id}/metadata", data=data)
            response_data = response.data
            response_data["file_type"] = response_data.pop("type")
            response_data["file_id"] = response_data.pop("id")
            return DepositedFile(**response_data)
            
        return await self._handle_redirect(_update_metadata)

    async def get_files(self, dep_id: str) -> DepositedFilesSet:
        """
        Get all files in deposition
        :param dep_id: Deposition ID
        :return: DepositedFilesSet
        """
        await self._ensure_adapter()
        
        async def _get_files():
            response = await self._rest_adapter.get(f"depositions/{dep_id}/files/")
            response_data = response.data
            return DepositedFilesSet(**response_data)
            
        return await self._handle_redirect(_get_files)

    async def remove_file(self, dep_id: str, file_id: int) -> bool:
        """
        Remove a file from a deposition
        :param dep_id: Deposition ID
        :param file_id: File ID
        :return: True if successful
        """
        await self._ensure_adapter()
        
        async def _remove_file():
            await self._rest_adapter.delete(f"depositions/{dep_id}/files/{file_id}")
            return True
            
        return await self._handle_redirect(_remove_file)

    async def get_status(self, dep_id: str) -> Union[DepositStatus, DepositError]:
        """
        Return the deposition status
        :param dep_id: Deposition ID
        :return: Status
        """
        await self._ensure_adapter()
        
        async def _get_status():
            response = await self._rest_adapter.get(f"depositions/{dep_id}/status")
            response_data = response.data
            try:
                return DepositStatus(**response_data)
            except TypeError:
                return DepositError(**response_data)
                
        return await self._handle_redirect(_get_status)

    async def process(self, dep_id: str, voxel: Dict = None, copy_from_id: str = None, copy_contact: bool = False,
                     copy_authors: bool = False, copy_citation: bool = False, copy_grant: bool = False,
                     copy_em_exp_data: bool = False) -> Union[DepositStatus, DepositError]:
        """
        Trigger file processing
        :param dep_id: Deposition ID
        :param voxel: EM Voxel list
        :param copy_from_id: Copy metadata from another deposition?
        :param copy_contact: Copy contract metadata?
        :param copy_authors: Copy authors?
        :param copy_citation: Copy citation?
        :param copy_grant: Copy grant?
        :param copy_em_exp_data: Copy EM experimental data?
        :return: Status
        """
        await self._ensure_adapter()
        
        async def _process():
            copy_elements = []
            data = voxel or {}

            if copy_from_id:
                if copy_contact:
                    copy_elements.append("contact")
                if copy_authors:
                    copy_elements.append("authors")
                if copy_citation:
                    copy_elements.append("citation")
                if copy_grant:
                    copy_elements.append("grant")
                if copy_em_exp_data:
                    copy_elements.append("em_exp")
                    
                data["related"] = {
                    'id': copy_from_id,
                    'items': copy_elements
                }

            response = await self._rest_adapter.post(f"depositions/{dep_id}/process", data=data)
            response_data = response.data
            try:
                return DepositStatus(**response_data)
            except TypeError:
                return DepositError(**response_data)
                
        return await self._handle_redirect(_process)


# Convenience function for single-use operations
async def create_async_deposit_api(*args, **kwargs) -> AsyncDepositApi:
    """
    Create and initialize an AsyncDepositApi instance
    Usage: async with create_async_deposit_api(...) as api:
    """
    api = AsyncDepositApi(*args, **kwargs)
    await api._ensure_adapter()
    return api