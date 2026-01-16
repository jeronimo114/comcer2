import requests
import datetime
from logger_config import setup_logger
from services.excel import Client

logger = setup_logger()


class CGANService:
    def __init__(self):
        self.login_url = "https://infocgan.cloudmantum.com/api/login"
        self.api_url = "https://api-infocgan.cloudmantum.com/api/"
        self.token = None
        self.session = requests.Session()
        self.api_client = Client()

    def login(self) -> bool:
        try:
            logger.info("Attempting to connect to INFOCGAN API")
            response = self.session.post(
                self.login_url,
                json={"username": "ivan.echeverri", "password": "83006661"},
            )
            response.raise_for_status()
            self.token = response.json()["user"]["token"]
            if self.token:
                logger.info("Token successfully retrieved")
                self.session.headers.update({"Authorization": f"Bearer {self.token}"})
                return True
            logger.error("No token recieved from the login.")
            return False
        except Exception:
            error_message = None
            try:
                error_message = response.json().get("message")
            except Exception:
                error_message = None
            if error_message:
                logger.error("%s (likely incorrect credentials)", error_message)
            else:
                logger.error("Login failed.")
            return False

    def get_lote_detail(self, lote: int) -> dict:
        try:
            logger.info(f"Querying lote number {lote}")
            response = self.session.get(
                f"{self.api_url}batch/{lote}",
                headers=self.session.headers,
            )
            response.raise_for_status()
            logger.info(f"Lote {lote} queried succesfully.")

            # logger.debug(response.json())

            return response.json()
        except Exception as e:
            return None

    def get_lote_individuals(self, lote: int) -> dict:
        try:
            logger.info(f"Querying individuals for lote number {lote}")
            response = self.session.get(
                f"{self.api_url}monitoring/individuals/{lote}",
                headers=self.session.headers,
            )
            response.raise_for_status()
            logger.info("Individuales queried successfully.")
            # logger.debug(response.json())
            return response.json()

        except Exception as e:
            return None

    """
    There is a missmatching name convention, lote number and lote id are completely different
    user deals with lote number while devs deal with lote id to interact with the API.
    """

    def get_batches(self) -> dict:
        try:
            start_date = (
                datetime.datetime.today() - datetime.timedelta(days=30)
            ).strftime("%Y-%m-%d")
            end_date = datetime.datetime.today().strftime("%Y-%m-%d")
            logger.info("Retrieving batches")
            response = self.session.post(
                f"{self.api_url}batch/search",
                headers=self.session.headers,
                data={"startdate": start_date, "enddate": end_date, "specie": 1},
            )
            response.raise_for_status()

            batches = {elem["batch"]: elem["id"] for elem in response.json()["body"]}

            logger.info("Batches queried successfully.")
            # logger.info(batches)
            return batches
        except Exception as e:
            logger.error("Error while retrieving batches.")
            logger.error(e)
