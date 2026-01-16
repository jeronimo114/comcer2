from flask import (
    Flask,
    render_template,
    flash,
    redirect,
    url_for,
    send_file,
    session,
    jsonify,
)
from forms import LoteForm
from logger_config import setup_logger
from services.api import CGANService
import logging
import colorlog
import time
import os
import zipfile

app = Flask(__name__)
app.config["SECRET_KEY"] = "tu_clave_secreta_aqui"

storage = {"results_lote": [], "results_individuals": []}

# Setup global logger
logger = setup_logger()

cgan_service = CGANService()
cgan_service.login()


@app.route("/", methods=["GET", "POST"])
def home():
    form = LoteForm()
    results_lote = None
    results_individuals = None
    lote = None
    if form.validate_on_submit():
        batches = cgan_service.get_batches()
        try:
            logger.error(batches)

            lote = batches[form.lote.data]
        except KeyError:
            logger.error("Invalid lote")
        if not cgan_service.token or not lote:
            if not cgan_service.login():
                flash("Error de conexión con el servicio")
                return render_template("index.html", form=form)

        results_lote = cgan_service.get_lote_detail(lote)
        results_individuals = cgan_service.get_lote_individuals(lote)
        if results_lote and results_individuals:
            results_lote = results_lote["body"]
            results_individuals = results_individuals["body"]
            cgan_service.api_client.fill_info(results_lote)
            clients = list(set(cgan_service.api_client.clients))
            print(clients)
            session["clients"] = clients
            storage["results_lote"] = results_lote
            storage["results_individuals"] = results_individuals
            storage["lote"] = lote

            # Obtener datos de decomisos
            decomisos_data = cgan_service.get_decomisos_data(lote)
            if decomisos_data:
                storage["decomisos_data"] = decomisos_data
                logger.info(f"Decomisos data obtained for lote {lote}")
            else:
                logger.warning(f"No se pudieron obtener datos de decomisos para lote {lote}")
                storage["decomisos_data"] = None

            return redirect(url_for("loading"))

        else:
            flash("Número de lote incorrecto o no disponible.")

    return render_template(
        "index.html",
        form=form,
        lote=lote,
        data_lote=results_lote,
        data_individuals=results_individuals,
    )


@app.route("/download/<lote>")
def download(lote):
    try:
        lote = cgan_service.api_client.batch
        # Create downloads directory if it doesn't exist
        if not os.path.exists("downloads"):
            os.makedirs("downloads")

        # Path to the folder containing the files
        folder_path = f"./downloads/{lote}"
        # Path for the zip file
        zip_path = f"./downloads/{lote}.zip"

        # Create zip file
        with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zipf:
            # Walk through the directory
            for root, dirs, files in os.walk(folder_path):
                for file in files:
                    # Create the full file path
                    file_path = os.path.join(root, file)
                    # Add file to zip (arcname removes the full path from the zip structure)
                    arcname = os.path.relpath(file_path, folder_path)
                    zipf.write(file_path, arcname)

        logger.info(f"Created zip file: {zip_path}")

        # Send the zip file
        return send_file(zip_path, as_attachment=True, download_name=f"lote_{lote}.zip")

    except Exception as e:
        logger.error(f"Error creating zip file: {str(e)}")
        flash(f"Error al comprimir los archivos: {str(e)}")
        return redirect(url_for("home"))


@app.route("/loading")
def loading():
    lote = storage.get("lote")
    if not lote:
        flash("No hay lote seleccionado.")
        return redirect(url_for("home"))
    return render_template("loading.html", lote=lote)


@app.route("/process", methods=["GET"])
def process_batch():
    try:
        clients = session.get("clients", [])
        results_lote = storage["results_lote"]
        results_individuals = storage["results_individuals"]

        # Escribir decomisos a Google Sheets (una sola vez, no por cliente)
        decomisos_data = storage.get("decomisos_data")
        if decomisos_data:
            logger.info("Escribiendo datos de decomisos a Google Sheets")
            cgan_service.api_client.fill_decomisos(decomisos_data)
        else:
            logger.warning("No hay datos de decomisos para escribir")

        # Aprobar automáticamente todos los clientes
        for client in clients:
            logger.info(f"Aprobando automáticamente cliente: {client}")
            cgan_service.api_client.fill_despacho(results_individuals, client)
            cgan_service.api_client.fill_liquidacion(results_lote, client)
            cgan_service.api_client.download_sheet(client)
            cgan_service.api_client.download_sheet_pdf(client)
            cgan_service.api_client.copy_consecutivo_row(6)
            cgan_service.api_client.download_consecutivos_sheet()

        return jsonify({
            "success": True,
            "redirect": url_for("download_page")
        })
    except Exception as e:
        logger.error(f"Error processing batch: {str(e)}")
        return jsonify({
            "success": False,
            "error": str(e)
        })


@app.route("/complete")
def download_page():
    return render_template("download.html", lote=storage["lote"])


@app.route("/download/consecutivos")
def download_consecutivos():
    try:
        filepath = cgan_service.api_client.download_consecutivos_sheet()
        return send_file(
            filepath, as_attachment=True, download_name=os.path.basename(filepath)
        )
    except Exception as e:
        flash(f"Error al descargar el archivo: {str(e)}")
        return redirect(url_for("home"))


@app.route("/download/report")
def download_report():
    try:
        filepath = cgan_service.api_client.download_sheet_pdf()
        return send_file(
            filepath, as_attachment=True, download_name=os.path.basename(filepath)
        )
    except Exception as e:
        flash(f"Error al descargar el archivo: {str(e)}")
        return redirect(url_for("home"))


if __name__ == "__main__":
    app.run(debug=True, port=8000)
