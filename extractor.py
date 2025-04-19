import pdfplumber, docx, pandas as pd
from PIL import Image
import pytesseract
import io

pytesseract.pytesseract.tesseract_cmd = r'C:\Program Files\Tesseract-OCR\tesseract.exe'

def extract_text(ext, content):
    if ext == ".pdf":
        with pdfplumber.open(io.BytesIO(content)) as pdf:
            return "".join(page.extract_text() or "" for page in pdf.pages)

    elif ext == ".docx":
        doc = docx.Document(io.BytesIO(content))
        return "\n".join(para.text for para in doc.paragraphs)

    elif ext in [".png", ".jpg", ".jpeg"]:
        image = Image.open(io.BytesIO(content))
        return pytesseract.image_to_string(image)

    elif ext == ".csv":
        df = pd.read_csv(io.BytesIO(content))
        return df.to_string(index=False)

    elif ext == ".txt":
        return content.decode("utf-8")

    else:
        raise ValueError("Unsupported file type.")
