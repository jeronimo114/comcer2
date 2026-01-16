from flask_wtf import FlaskForm
from wtforms import StringField, SubmitField, SelectField
from wtforms.validators import DataRequired, Regexp


class LoteForm(FlaskForm):
    lote = StringField(
        "Número de Lote",
        validators=[
            DataRequired(),
        ],
    )

    submit = SubmitField("Consultar")
