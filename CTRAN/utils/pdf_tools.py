from weasyprint import HTML,CSS
from weasyprint.text.fonts import FontConfiguration

def generatePDF(htmlString, cssString):
    html=HTML(string=htmlString)
    css=CSS(string=cssString)
    font_config=FontConfiguration()
    return html.render(stylesheets=[css],font_config=font_config)

