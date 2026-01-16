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

    def get_dispatch_summary_path(self, lote_id: int) -> str:
        """
        Genera el informe de resumen de despacho y retorna el path del Excel.

        Args:
            lote_id: ID numérico del lote (no el código de lote)

        Returns:
            str: Path relativo al archivo (ej: "storage/80-resumen-despacho-xxx.xlsx")
            None: Si hubo error
        """
        try:
            logger.info(f"Requesting dispatch summary for lote_id {lote_id}")
            response = self.session.get(
                f"{self.api_url}summary/dispatch/{lote_id}",
                headers=self.session.headers,
            )
            response.raise_for_status()
            data = response.json()
            path = data.get("body", {}).get("path")
            if path:
                logger.info(f"Dispatch summary path: {path}")
                return path
            else:
                logger.error("No path found in dispatch summary response")
                return None
        except Exception as e:
            logger.error(f"Error getting dispatch summary path: {e}")
            return None

    def download_dispatch_summary(self, path: str) -> bytes:
        """
        Descarga el archivo Excel del resumen de despacho.

        IMPORTANTE: La URL NO incluye /api/ - es directamente:
        https://api-infocgan.cloudmantum.com/{path}

        Args:
            path: Path relativo del archivo (ej: "storage/80-resumen-despacho-xxx.xlsx")

        Returns:
            bytes: Contenido del archivo Excel
            None: Si hubo error
        """
        if not path:
            logger.error("Empty path provided for download")
            return None

        try:
            # URL base SIN /api/
            base_url = "https://api-infocgan.cloudmantum.com"
            download_url = f"{base_url}/{path}"

            logger.info(f"Downloading dispatch summary from: {download_url}")
            response = self.session.get(
                download_url,
                headers=self.session.headers,
                timeout=60
            )
            response.raise_for_status()

            logger.info(f"Downloaded {len(response.content)} bytes")
            return response.content
        except Exception as e:
            logger.error(f"Error downloading dispatch summary: {e}")
            return None

    def get_decomisos_data(self, lote_id: int) -> dict:
        """
        Obtiene los datos de decomisos para un lote.
        Combina: obtener path + descargar + parsear

        Args:
            lote_id: ID numérico del lote

        Returns:
            dict: {"cantidades": [...], "motivos": [...]}
            None: Si hubo error
        """
        try:
            # 1. Obtener path del Excel
            path = self.get_dispatch_summary_path(lote_id)
            if not path:
                logger.error("Could not get dispatch summary path")
                return None

            # 2. Descargar el Excel
            excel_bytes = self.download_dispatch_summary(path)
            if not excel_bytes:
                logger.error("Could not download dispatch summary")
                return None

            # 3. Parsear el Excel
            decomisos_data = self.api_client.parse_decomisos_excel(excel_bytes)
            logger.info(f"Parsed decomisos: {len(decomisos_data.get('cantidades', []))} cantidades, {len(decomisos_data.get('motivos', []))} motivos")

            return decomisos_data
        except Exception as e:
            logger.error(f"Error getting decomisos data: {e}")
            return None
