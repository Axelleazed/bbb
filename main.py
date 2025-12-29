from fastapi import FastAPI, HTTPException, BackgroundTasks, Request, Form, UploadFile, File
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, StreamingResponse, HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel
from typing import List, Optional
import requests
import pandas as pd
from datetime import datetime, date
import json
import io
import uuid
import os
import tempfile
import PyPDF2
import time
import re
import uvicorn
import asyncio
import requests
import io
import re
from urllib.parse import urljoin, urlparse
import fitz  # PyMuPDF - better for PDF text extraction (install: pip install PyMuPDF)

app = FastAPI(title="BOAMP Data Extractor Pro", version="3.0.0")

# Create directories if they don't exist
os.makedirs("static", exist_ok=True)
os.makedirs("templates", exist_ok=True)

# Mount static files and templates
app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Models
class ExtractionRequest(BaseModel):
    target_date: str
    max_records: int = 5000
    departments: List[str]

class ExtractionResponse(BaseModel):
    job_id: str
    status: str
    message: str
    total_records: Optional[int] = None
    filtered_records: Optional[int] = None

# Storage for job results
jobs = {}
processing_state = {}

# Main function to get BOAMP records
def get_all_records_for_date(target_date, max_records=5000):
    """Get all records for a specific date with all available fields"""
    url = "https://boamp-datadila.opendatasoft.com/api/explore/v2.1/catalog/datasets/boamp/records"
    all_records = []
    offset = 0
    limit = 100
    
    while len(all_records) < max_records:
        params = {
            'order_by': 'dateparution DESC',
            "type_marche": 'Travaux',
            'limit': limit,
            'offset': offset,
        }

        try:
            response = requests.get(url, params=params)
            response.raise_for_status()
            
            data = response.json()
            records = data.get('results', [])

            if not records:
                break  # No more records

            # Filter records for our target date
            target_records = [record for record in records if record.get('dateparution') == target_date]

            # If we found target records, add them
            if target_records:
                all_records.extend(target_records)

            # Check if we've moved past our target date (since we're sorting DESC)
            if records and records[-1].get('dateparution', '') < target_date:
                break

            offset += limit

            if offset > 10000:
                break
                
        except requests.exceptions.RequestException as e:
            print(f"Error fetching data: {e}")
            break

    return all_records

# Function to create cleaned dataframe
def create_excel_simple(records: List[dict], target_date: str):
    """Simple and robust Excel creation"""
    cleaned_records = []
    for record in records:
        cleaned_record = {}
        for key, value in record.items():
            if isinstance(value, (list, dict)):
                cleaned_record[key] = json.dumps(value, ensure_ascii=False)
            elif value is None:
                cleaned_record[key] = ''
            else:
                cleaned_record[key] = value
        cleaned_records.append(cleaned_record)

    df = pd.DataFrame(cleaned_records)
    return df

def get_predefined_keywords():
    """Return predefined keywords for filtering"""
    return [
        "miroiterie",
        "métallerie",
        "menuiserie extérieure",
        "Travaux de menuiserie et de charpenterie",
        "Pose de portes et de fenêtres et d'éléments accessoires",
        "Pose d'encadrements de portes et de fenêtres",
        "Pose d'encadrements de portes",
        "Pose d'encadrements de fenêtres",
        "Pose de seuils",
        "Poses de portes et de fenêtres",
        "Pose de portes",
        "Pose de fenêtres",
        "Pose de menuiseries métalliques, excepté portes et fenêtres",
        "Travaux de cloisonnement",
        "Installation de volets",
        "Travaux d'installation de stores",
        "Travaux d'installation de vélums",
        "Travaux d'installation de volets roulants",
        "Serrurerie",
        "Services de serrurerie",
        "Menuiserie pour la construction",
        "Travaux de menuiserie",
        "Clôtures",
        "Clôtures de protection",
        "Travaux d'installation de clôtures, de garde-corps et de dispositifs de sécurité",
        "Pose de clôtures",
        "Ascenseurs, skips, monte-charges, escaliers mécaniques et trottoirs roulants",
        "Escaliers mécaniques",
        "Pièces pour ascenseurs, skips ou escaliers mécaniques",
        "Pièces pour escaliers mécaniques",
        "Escaliers",
        "Escaliers pliants",
        "Travaux d'installation d'ascenseurs et d'escaliers mécaniques",
        "Travaux d'installation d'escaliers mécaniques",
        "Services de réparation et d'entretien d'escaliers mécaniques",
        "Services d'installation de matériel de levage et de manutention, excepté ascenseurs et escaliers mécaniques",
        "45420000", "45421100", "45421110", "45421111", "45421112", "45421120", 
        "45421130", "45421131", "45421132", "45421140", "45421141", "45421142", 
        "45421143", "45421144", "45421145", "44316500", "98395000", "44220000", 
        "45421000", "34928200", "34928310", "45340000", "45342000", "42416000", 
        "42416400", "42419500", "42419530", "44233000", "44423220", "45313000", 
        "45313200", "50740000", "51511000",
    ]

def filter_by_keywords(df: pd.DataFrame, keywords: List[str]):
    """Filter DataFrame by keywords"""
    df_str = df.astype(str).apply(lambda x: x.str.lower())
    all_matches = pd.DataFrame()

    for keyword in keywords:
        mask = df_str.apply(lambda x: x.str.contains(keyword.lower(), na=False))
        filtered_df = df[mask.any(axis=1)]
        
        if not filtered_df.empty:
            filtered_df = filtered_df.copy()
            filtered_df["keyword"] = keyword
            all_matches = pd.concat([all_matches, filtered_df], ignore_index=True)

    return all_matches

def remove_duplicates(df: pd.DataFrame, id_column: str, keyword_column: str):
    """Remove duplicates from DataFrame by combining keywords"""
    # Group by ID and combine keywords
    def combine_keywords(group):
        if len(group) > 1:
            # Combine keywords from all rows with the same ID
            combined_keywords = '; '.join(str(keyword) for keyword in group[keyword_column] if pd.notna(keyword) and str(keyword).strip())
            # Keep the first row but update the keyword column with combined values
            first_row = group.iloc[0].copy()
            first_row[keyword_column] = combined_keywords
            return first_row
        else:
            return group.iloc[0]
    
    # Apply the combination logic
    df_clean = df.groupby(id_column).apply(combine_keywords).reset_index(drop=True)
    return df_clean

def filter_by_departments(df, target_departments):
    """Filter dataframe by target departments"""
    if not target_departments:
        return df
    
    indices_a_conserver = []
    departements_trouves = []

    for index, row in df.iterrows():
        code_departement = row.get('code_departement', '')
        departement_trouve = None
        
        # Check if code_departement is a valid list
        if pd.notna(code_departement) and code_departement != "":
            # If it's a string that looks like a list, convert to list
            if isinstance(code_departement, str):
                try:
                    # Remove brackets and quotes, then split
                    code_departement = code_departement.strip('[]').replace('"', '').replace("'", "").split(', ')
                    # Clean elements
                    code_departement = [dep.strip() for dep in code_departement if dep.strip()]
                except:
                    code_departement = []
            elif not isinstance(code_departement, list):
                code_departement = []
            
            # Check if at least one department from the list is in target departments
            for dep in code_departement:
                if dep in target_departments:
                    indices_a_conserver.append(index)
                    departement_trouve = dep  # Store found department
                    break  # Exit loop as soon as we find a match
        
        # Add found department (or None if none)
        departements_trouves.append(departement_trouve)

    # Create new DataFrame with only kept rows
    if indices_a_conserver:
        df_filtre = df.loc[indices_a_conserver].reset_index(drop=True)
        # Add column with found department code
        df_filtre['code_departement_trouve'] = [departements_trouves[i] for i in indices_a_conserver]
    else:
        df_filtre = pd.DataFrame(columns=df.columns)
    
    return df_filtre

def extract_pdf_content(df: pd.DataFrame, process_id: str):
    """Extract PDF content and analyze for lots and visite information"""
    if df.empty:
        return df
    
    df_with_pdf = df.copy()
    df_with_pdf['generated_link'] = ""
    df_with_pdf['pdf_content'] = ""
    df_with_pdf['pdf_status'] = ""
    df_with_pdf['pages_extracted'] = 0
    df_with_pdf['lot_numbers'] = ""
    df_with_pdf['visite_obligatoire'] = ""
    df_with_pdf['keywords_used'] = ""
    # Add these to the list of new columns (around line 410)
    df_with_pdf['extracted_links'] = ""
    df_with_pdf['primary_extracted_link'] = ""
    
    total_records = len(df_with_pdf)
    successful = 0
    errors = 0
    
    processing_state[process_id]['current_step'] = 'pdf_processing'
    processing_state[process_id]['total_records'] = total_records
    processing_state[process_id]['processed_records'] = 0
    
    for index, row in df_with_pdf.iterrows():
        dateparution_str = row.get('dateparution')
        idweb = row.get('idweb', 'N/A')
        keywords_from_row = row.get('keyword', '')
        
        # Update progress
        processing_state[process_id]['processed_records'] = index + 1
        processing_state[process_id]['current_record'] = idweb
        
        if idweb == 'N/A':
            df_with_pdf.at[index, 'pdf_status'] = "Skipped - No ID"
            errors += 1
            continue
            
        try:
            # Parse date
            if isinstance(dateparution_str, str):
                date_formats = ['%Y-%m-%d', '%d/%m/%Y', '%m/%d/%Y', '%d-%m-%Y', '%Y/%m/%d']
                dateparution = None
                for fmt in date_formats:
                    try:
                        dateparution = datetime.strptime(dateparution_str, fmt)
                        break
                    except ValueError:
                        continue
                if dateparution is None:
                    df_with_pdf.at[index, 'pdf_status'] = "Error - Date parsing failed"
                    errors += 1
                    continue
            else:
                dateparution = dateparution_str
            
            # Generate link
            link = f"https://www.boamp.fr/telechargements/FILES/PDF/{dateparution.year}/{dateparution.month:02d}/{idweb}.pdf"
            
            # Add link to DataFrame
            df_with_pdf.at[index, 'generated_link'] = link
            
            # Store keywords used for this row
            df_with_pdf.at[index, 'keywords_used'] = str(keywords_from_row)
            
            # Download and extract PDF content
            try:
                # Download the PDF
                response = requests.get(link, timeout=30)
                response.raise_for_status()
                
                # Save to temporary file
                with tempfile.NamedTemporaryFile(suffix='.pdf', delete=False) as temp_file:
                    temp_file.write(response.content)
                    temp_path = temp_file.name
                
                # Extract text using PyPDF2
                with open(temp_path, 'rb') as pdf_file:
                    pdf_reader = PyPDF2.PdfReader(pdf_file)
                    
                    # Extract text from each page
                    full_text = ""
                    for page_num, page in enumerate(pdf_reader.pages):
                        page_text = page.extract_text()
                        full_text += f"Page {page_num + 1}:\n{page_text}\n\n"
                    
                    # Add PDF content to DataFrame
                    df_with_pdf.at[index, 'pdf_content'] = full_text
                    df_with_pdf.at[index, 'pages_extracted'] = len(pdf_reader.pages)
                    df_with_pdf.at[index, 'pdf_status'] = "Success"
                    
                    
                    # Extract keywords from the row (could be string or list)
                    if isinstance(keywords_from_row, str):
                        # Split by semicolon if it's a combined string from deduplication
                        search_keywords = [k.strip() for k in keywords_from_row.split(';') if k.strip()]
                    else:
                        search_keywords = [str(keywords_from_row)]
                    
                    # Search for lot numbers
                    lot_results = search_keywords_and_find_lot(full_text, search_keywords)
                    if lot_results:
                        unique_lots = set()
                        for result in lot_results:
                            unique_lots.add(f"lot-{result['lot_number']}")
                        df_with_pdf.at[index, 'lot_numbers'] = ', '.join(sorted(unique_lots))
                    
                    # Check for visite obligatoire
                    visite_keywords = ["obligatoires", "obligatoire"]
                    visite_result = check_visite_obligatoire(full_text, visite_keywords)
                    df_with_pdf.at[index, 'visite_obligatoire'] = visite_result
                    
                    successful += 1
                    # Extract links from PDF content
                    try:
                        pdf_links = extract_links_from_pdf_content(link, full_text)
                        if pdf_links:
                            # Store all links (comma-separated)
                            df_with_pdf.at[index, 'extracted_links'] = ', '.join(pdf_links)
                            # Store primary link (first one)
                            df_with_pdf.at[index, 'primary_extracted_link'] = pdf_links[0] if pdf_links else ''
                        else:
                            df_with_pdf.at[index, 'extracted_links'] = ''
                            df_with_pdf.at[index, 'primary_extracted_link'] = ''
                    except Exception as e:
                        print(f"Error extracting links from PDF {link}: {e}")
                        df_with_pdf.at[index, 'extracted_links'] = ''
                        df_with_pdf.at[index, 'primary_extracted_link'] = ''
                
                # Clean up
                os.unlink(temp_path)
                
            except Exception as e:
                error_msg = f"Error processing PDF: {str(e)}"
                df_with_pdf.at[index, 'pdf_content'] = error_msg
                df_with_pdf.at[index, 'pdf_status'] = f"Error: {str(e)}"
                errors += 1
            
            # Add a small delay to be respectful to the server
            time.sleep(0.5)
            
        except Exception as e:
            error_msg = f"Error processing row: {str(e)}"
            df_with_pdf.at[index, 'pdf_content'] = error_msg
            df_with_pdf.at[index, 'pdf_status'] = f"Error: {str(e)}"
            errors += 1
            continue
    
    processing_state[process_id]['status'] = 'completed'
    processing_state[process_id]['result'] = df_with_pdf.to_dict('records')
    
    return df_with_pdf

def extract_links_from_pdf_content(pdf_url: str, pdf_content: str = None) -> List[str]:
    """
    Extract URLs from PDF content, specifically looking for "Documents de marché" links.
    Handles URLs split across multiple lines.
    """
    try:
        # If we already have PDF content from previous extraction, use it
        if pdf_content:
            text = pdf_content
        else:
            # Download and extract PDF text
            response = requests.get(pdf_url, timeout=30)
            response.raise_for_status()
            
            # Use PyMuPDF for better text extraction
            doc = fitz.open(stream=response.content, filetype="pdf")
            text = ""
            for page_num in range(doc.page_count):
                page = doc.load_page(page_num)
                text += page.get_text("text") + "\n"
            doc.close()
        
        # Preprocess text to handle line breaks in URLs
        text = preprocess_pdf_text_for_urls(text)
        
        # Method 1: Look for "Documents de marché" pattern
        documents_urls = extract_documents_de_marche_urls(text)
        if documents_urls:
            print(f"DEBUG: Found Documents de marché URLs: {documents_urls}")
            return documents_urls
        
        # Method 2: Fallback to general URL extraction
        all_urls = extract_all_urls_from_text(text)
        relevant_urls = filter_relevant_urls(all_urls)
        
        return relevant_urls
        
    except Exception as e:
        print(f"Error extracting links from PDF {pdf_url}: {e}")
        return []

def preprocess_pdf_text_for_urls(text: str) -> str:
    """
    Special preprocessing to fix URLs split across lines.
    Specifically handles the pattern where URLs are broken by newlines.
    """
    # First, join lines that are clearly part of a URL
    lines = text.split('\n')
    processed_lines = []
    
    i = 0
    while i < len(lines):
        current_line = lines[i].strip()
        
        # Look for lines ending with common URL parts
        url_indicators = ['http', 'https', 'www.', '.com', '.fr', '.gouv', '.org', '/']
        
        # Check if this line looks like it might contain a URL that continues on next line
        if i + 1 < len(lines):
            next_line = lines[i + 1].strip()
            
            # Case 1: Current line ends with a word that might be part of URL
            if current_line and next_line:
                # Check if current line ends with URL indicator or next line starts with path
                if (any(indicator in current_line.lower() for indicator in url_indicators) and
                    not current_line.endswith(('.', '!', '?', ':', ';'))):
                    
                    # If next line starts with path component, join them
                    if next_line.startswith(('/', 'ent_', 'gen', 'detail', 'do?')):
                        joined = current_line.rstrip() + next_line.lstrip()
                        processed_lines.append(joined)
                        i += 2
                        continue
                
                # Case 2: Handle "https://example.com" on one line and "/path" on next
                if (current_line.startswith(('http://', 'https://')) and 
                    next_line.startswith('/')):
                    joined = current_line + next_line
                    processed_lines.append(joined)
                    i += 2
                    continue
        
        processed_lines.append(current_line)
        i += 1
    
    # Reconstruct with single spaces between lines
    result = ' '.join(processed_lines)
    
    # Fix common URL issues
    result = re.sub(r'(https?://[^\s<>"\']+)\s+(/[^\s<>"\']*)', r'\1\2', result)
    result = re.sub(r'(www\.[^\s<>"\']+)\s+(/[^\s<>"\']*)', r'\1\2', result)
    
    return result

def extract_documents_de_marche_urls(text: str) -> List[str]:
    """
    Specifically extract URLs that appear after "Documents de marché" or similar patterns.
    """
    urls = []
    
    # Pattern 1: Direct "Documents de marché" followed by URL
    patterns = [
        r'Documents de marché\s*[:;]\s*(https?://[^\s<>"\']+)',
        r'Adresse des documents de marché\s*[:;]\s*(https?://[^\s<>"\']+)',
        r'documents de marché\s*[:;]\s*(https?://[^\s<>"\']+)',
        r'documents\s*[:;]\s*(https?://[^\s<>"\']+)',
        r'consultation des documents\s*[:;]\s*(https?://[^\s<>"\']+)',
        r'accès aux documents\s*[:;]\s*(https?://[^\s<>"\']+)',
    ]
    
    for pattern in patterns:
        matches = re.finditer(pattern, text, re.IGNORECASE)
        for match in matches:
            url = match.group(1).strip()
            url = clean_extracted_url(url)
            if url and url.startswith('http'):
                urls.append(url)
    
    # Pattern 2: Look for "achatpublic.com" URLs specifically
    achatpublic_patterns = [
        r'(https?://www\.achatpublic\.com/[^\s<>"\']+)',
        r'(www\.achatpublic\.com/[^\s<>"\']+)',
        r'achatpublic\.com(/[^\s<>"\']+)',
    ]
    
    for pattern in achatpublic_patterns:
        matches = re.finditer(pattern, text, re.IGNORECASE)
        for match in matches:
            url = match.group(1) if 'http' not in match.group(0) else match.group(0)
            if not url.startswith('http'):
                url = 'https://' + url if url.startswith('www.') else 'https://www.' + url
            url = clean_extracted_url(url)
            if url:
                urls.append(url)
    
    # Pattern 3: Find URLs near "marche" keyword
    lines = text.split('\n')
    for i, line in enumerate(lines):
        if 'marche' in line.lower():
            # Look for URLs in this line and next few lines
            search_text = ' '.join(lines[i:i+3])
            url_matches = re.findall(r'(https?://[^\s<>"\']+)', search_text)
            for url in url_matches:
                url = clean_extracted_url(url)
                if url and ('achatpublic' in url or 'marche' in url.lower()):
                    urls.append(url)
    
    # Remove duplicates while preserving order
    seen = set()
    unique_urls = []
    for url in urls:
        if url not in seen:
            seen.add(url)
            unique_urls.append(url)
    
    return unique_urls

def extract_all_urls_from_text(text: str) -> List[str]:
    """Extract all URLs from text using comprehensive patterns"""
    url_patterns = [
        # Standard URLs
        r'https?://(?:[-\w.]|(?:%[\da-fA-F]{2}))+(?:/[-\w.~!$&\'()*+,;=:@%]*)*',
        # URLs without protocol
        r'www\.(?:[-\w.]|(?:%[\da-fA-F]{2}))+(?:/[-\w.~!$&\'()*+,;=:@%]*)*',
        # French procurement platforms
        r'(?:[-\w.]|(?:%[\da-fA-F]{2}))+\.(?:achatpublic|marches-publics|boamp|plateforme)\.(?:fr|com)(?:/[-\w.~!$&\'()*+,;=:@%]*)*',
        # Path-only URLs (common in PDFs)
        r'(?:/[-\w.~!$&\'()*+,;=:@%]+)+',
    ]
    
    all_urls = []
    
    for pattern in url_patterns:
        matches = re.finditer(pattern, text, re.IGNORECASE)
        for match in matches:
            url = match.group()
            url = clean_extracted_url(url)
            if url:
                all_urls.append(url)
    
    return all_urls

def clean_extracted_url(url: str) -> str:
    """Clean and normalize extracted URL"""
    if not url:
        return ""
    
    # Remove trailing punctuation
    url = re.sub(r'[.,;:!?)\]}]+$', '', url)
    # Remove leading punctuation
    url = re.sub(r'^[(\[{]+', '', url)
    
    # Handle path-only URLs
    if url.startswith('/') and not url.startswith('//'):
        # Check if it looks like a path from achatpublic.com
        if '/sdm/' in url or '/ent/' in url:
            url = 'https://www.achatpublic.com' + url
    
    # Add protocol if missing
    if url.startswith('www.') and not url.startswith('http'):
        url = 'https://' + url
    elif url.startswith('achatpublic.com'):
        url = 'https://www.' + url
    
    # Validate URL format
    try:
        parsed = urlparse(url)
        if parsed.scheme and parsed.netloc:
            return url
    except:
        pass
    
    return ""

def filter_relevant_urls(urls: List[str]) -> List[str]:
    """Filter URLs for procurement relevance"""
    relevant = []
    
    procurement_keywords = [
        'marche', 'appel', 'offre', 'soumission', 'avis',
        'procedure', 'consultation', 'tender', 'bid',
        'commande', 'achat', 'public', 'boamp', 'plateforme',
        'demat', 'candidature', 'dossier', 'documents'
    ]
    
    procurement_domains = [
        'achatpublic.com',
        'marches-publics.gouv.fr',
        'boamp.fr',
        'plateforme.economie.gouv.fr',
        'centraledesmarches.com',
        'demarches-simplifiees.fr',
        'e-marchespublics.gouv.fr'
    ]
    
    for url in urls:
        url_lower = url.lower()
        
        # Check for procurement domains
        parsed = urlparse(url)
        domain = parsed.netloc.lower()
        
        if any(proc_domain in domain for proc_domain in procurement_domains):
            relevant.append(url)
            continue
        
        # Check for procurement keywords in URL
        if any(keyword in url_lower for keyword in procurement_keywords):
            relevant.append(url)
            continue
        
        # Check for French government domains
        if '.gouv.fr' in domain or '.gouv.' in domain:
            relevant.append(url)
    
    # Sort by relevance (achatpublic.com first, then other procurement sites)
    relevant.sort(key=lambda x: (
        'achatpublic.com' not in x,
        'marches-publics' not in x,
        'boamp' not in x,
        -len(x)
    ))
    
    return relevant

def preprocess_pdf_text(text: str) -> str:
    """Preprocess PDF text to join multi-line URLs"""
    lines = text.split('\n')
    processed_lines = []
    
    i = 0
    while i < len(lines):
        line = lines[i].strip()
        
        # Check if line ends with hyphen (indicating word split)
        if line.endswith('-') and i + 1 < len(lines):
            next_line = lines[i + 1].strip()
            joined = line[:-1] + next_line  # Remove hyphen and join
            processed_lines.append(joined)
            i += 2
        elif line and i + 1 < len(lines):
            next_line = lines[i + 1].strip()
            # Check if this line looks like start of URL and next line continues it
            if (line.startswith(('http://', 'https://', 'www.')) and 
                not line.endswith(('.', ';', ':', ')'))):
                joined = line + next_line
                processed_lines.append(joined)
                i += 2
            else:
                processed_lines.append(line)
                i += 1
        else:
            processed_lines.append(line)
            i += 1
    
    # Reconstruct text
    reconstructed = ' '.join(processed_lines)
    
    # Fix common URL issues
    reconstructed = re.sub(r'(https?://[^\s]+)\s+([^\s]+)', r'\1\2', reconstructed)
    reconstructed = re.sub(r'(www\.[^\s]+)\s+([^\s]+)', r'\1\2', reconstructed)
    
    return reconstructed

def extract_urls_from_text(text: str) -> List[str]:
    """Extract URLs from text using multiple patterns"""
    url_patterns = [
        # Standard URL pattern
        r'https?://(?:[-\w.]|(?:%[\da-fA-F]{2}))+(?:/[-\w.~!$&\'()*+,;=:@%]*)*',
        # URLs without protocol
        r'www\.(?:[-\w.]|(?:%[\da-fA-F]{2}))+(?:/[-\w.~!$&\'()*+,;=:@%]*)*',
        # French government domains
        r'(?:[-\w.]|(?:%[\da-fA-F]{2}))+\.(?:fr|gouv\.fr|gouv|eu|com|org)(?:/[-\w.~!$&\'()*+,;=:@%]*)*',
    ]
    
    all_urls = []
    for pattern in url_patterns:
        matches = re.finditer(pattern, text, re.IGNORECASE)
        for match in matches:
            url = match.group()
            # Clean up URL
            url = clean_url(url)
            if url:
                all_urls.append(url)
    
    # Remove duplicates while preserving order
    seen = set()
    unique_urls = []
    for url in all_urls:
        if url not in seen:
            seen.add(url)
            unique_urls.append(url)
    
    return unique_urls

def clean_url(url: str) -> str:
    """Clean and normalize URL"""
    # Remove trailing punctuation
    url = re.sub(r'[.,;:!?)\]}]+$', '', url)
    # Remove leading punctuation
    url = re.sub(r'^[(\[{]+', '', url)
    
    # Add protocol if missing for www URLs
    if url.startswith('www.') and not url.startswith('http'):
        url = 'http://' + url
    
    return url

def filter_relevant_urls(urls: List[str]) -> List[str]:
    """Filter URLs for procurement relevance"""
    relevant = []
    
    procurement_keywords = [
        'marche', 'appel', 'offre', 'soumission', 'avis',
        'procedure', 'consultation', 'tender', 'bid',
        'commande', 'achat', 'public', 'boamp', 'plateforme',
        'demat', 'candidature', 'dossier'
    ]
    
    for url in urls:
        url_lower = url.lower()
        
        # Check for procurement keywords
        if any(keyword in url_lower for keyword in procurement_keywords):
            relevant.append(url)
        
        # Check for French government domains
        elif '.gouv.fr' in url_lower or '.gouv.' in url_lower:
            relevant.append(url)
    
    return relevant

def search_keywords_and_find_lot(text: str, keywords: List[str]):
    """
    Search for keywords in PDF text and find ALL lot numbers that appear before them
    """
    try:
        results = []
        
        # Search for each keyword
        for keyword in keywords:
            # Find all occurrences of the keyword
            keyword_matches = list(re.finditer(re.escape(keyword), text, re.IGNORECASE))
            
            for match in keyword_matches:
                keyword_position = match.start()
                
                # Extract more text before the keyword (look back up to 500 characters)
                text_before = text[max(0, keyword_position - 1000):keyword_position]
                
                # Improved lot pattern to catch more formats
                lot_patterns = [
                    r'(lot|LOT)\s*[:\-\s]*\s*(\d+[-\w]*)',  # lot: 123, LOT-456, lot 789
                    r'(Lot\s*\d+)',  # Lot 123
                    r'(lot\s*\d+)',  # lot 123
                    r'\b(\d+)\s*-\s*Lot',  # 123 - Lot
                    r'\b(LOT\s*[A-Z]*\d+)',  # LOT A123, LOT 456
                ]
                
                all_lot_matches = []
                
                for pattern in lot_patterns:
                    matches = re.findall(pattern, text_before, re.IGNORECASE)
                    for match_tuple in matches:
                        if isinstance(match_tuple, tuple):
                            # For patterns that capture groups
                            lot_number = match_tuple[1] if len(match_tuple) > 1 else match_tuple[0]
                        else:
                            # For patterns that capture directly
                            lot_number = match_tuple
                        
                        # Clean up the lot number
                        lot_number = re.sub(r'^(lot|LOT)\s*', '', lot_number, flags=re.IGNORECASE)
                        lot_number = lot_number.strip(' :-\t')
                        
                        if lot_number and lot_number not in [lm[0] for lm in all_lot_matches]:
                            all_lot_matches.append((lot_number, pattern))
                
                # Remove duplicates while preserving order
                unique_lots = []
                seen = set()
                for lot_num, pattern in all_lot_matches:
                    if lot_num not in seen:
                        seen.add(lot_num)
                        unique_lots.append(lot_num)
                
                if unique_lots:
                    for lot_number in unique_lots:
                        results.append({
                            'keyword': keyword,
                            'lot_number': lot_number
                        })
        
        return results
            
    except Exception as e:
        return []

def check_visite_obligatoire(text: str, keywords: List[str]):
    """
    Search for keywords in PDF text and check if 'visite' appears before them
    """
    try:
        # Search for each keyword
        for keyword in keywords:
            # Find all occurrences of the keyword
            keyword_matches = list(re.finditer(re.escape(keyword), text, re.IGNORECASE))
            
            for match in keyword_matches:
                keyword_position = match.start()
                
                # Extract text before the keyword (look back up to 500 characters)
                text_before = text[max(0, keyword_position - 500):keyword_position]
                
                # Check if "visite" appears before the keyword
                visite_patterns = [r"visites", r"visite"]
                
                for pattern in visite_patterns:
                    if re.search(pattern, text_before, re.IGNORECASE):
                        return "yes"
        
        return "no"
            
    except Exception as e:
        return "no"
def debug_pdf_extraction(pdf_url: str):
    """Debug function to see what URLs are being extracted"""
    print(f"\n=== DEBUG for {pdf_url} ===")
    
    # Download PDF
    response = requests.get(pdf_url, timeout=30)
    
    # Extract text
    doc = fitz.open(stream=response.content, filetype="pdf")
    text = ""
    for page in doc:
        text += page.get_text() + "\n"
    doc.close()
    
    print(f"PDF Text Sample (first 2000 chars):\n{text[:2000]}\n")
    
    # Look for specific patterns
    lines = text.split('\n')
    for i, line in enumerate(lines):
        if 'Documents' in line or 'documents' in line or 'marche' in line.lower():
            print(f"Line {i}: {line}")
            # Show next line too
            if i + 1 < len(lines):
                print(f"Line {i+1}: {lines[i+1]}")
    
    # Extract URLs
    urls = extract_documents_de_marche_urls(text)
    print(f"\nExtracted URLs: {urls}")
    
    return urls
@app.get("/", response_class=HTMLResponse)
async def read_root(request: Request):
    """Serve the main page"""
    predefined_keywords = get_predefined_keywords()
    return templates.TemplateResponse("index.html", {
        "request": request,
        "predefined_keywords": predefined_keywords,
        "today": date.today().isoformat()
    })

@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {"status": "healthy", "timestamp": datetime.now().isoformat()}

@app.post("/process")
async def process_data(
    target_date: str = Form(...),
    selected_keywords: List[str] = Form(...),
    custom_keywords: str = Form(""),
    selected_departments: str = Form("")  # New parameter for departments from map
):
    """Start the data processing"""
    process_id = f"process_{int(time.time())}_{uuid.uuid4().hex[:8]}"
    
    # Combine keywords
    all_keywords = selected_keywords.copy()
    if custom_keywords:
        custom_keywords_list = [k.strip() for k in custom_keywords.split('\n') if k.strip()]
        all_keywords.extend(custom_keywords_list)
    
    if not all_keywords:
        raise HTTPException(status_code=400, detail="Please select at least one keyword")
    
    # Parse selected departments from comma-separated string
    if selected_departments:
        target_departments_list = [dept.strip() for dept in selected_departments.split(',') if dept.strip()]
    else:
        target_departments_list = []
    
    if not target_departments_list:
        raise HTTPException(status_code=400, detail="Please select at least one department")
    
    # Initialize processing state
    processing_state[process_id] = {
        'status': 'starting',
        'current_step': 'initializing',
        'total_records': 0,
        'processed_records': 0,
        'current_record': '',
        'result': None,
        'keywords': all_keywords,
        'target_date': target_date,
        'departments': target_departments_list,
        'summary_table': []
    }
    
    # Run processing in background
    asyncio.create_task(run_processing(process_id, target_date, all_keywords, target_departments_list))
    
    return JSONResponse({
        "process_id": process_id, 
        "status": "started",
        "message": f"Processing started for {len(target_departments_list)} departments"
    })

async def run_processing(process_id: str, target_date: str, all_keywords: List[str], target_departments_list: List[str]):
    """Run the full processing in background"""
    try:
        MAX_RECORDS = 10000
        
        # Step 1: Extract data
        processing_state[process_id]['current_step'] = 'data_extraction'
        processing_state[process_id]['status'] = 'processing'
        
        all_records = get_all_records_for_date(target_date, MAX_RECORDS)
        
        if not all_records:
            processing_state[process_id]['status'] = 'completed'
            processing_state[process_id]['message'] = f"No records found for date {target_date}"
            return
        
        processing_state[process_id]['total_records'] = len(all_records)
        
        # Create DataFrame
        df = create_excel_simple(all_records, target_date)

        # Step 2: Filter by keywords
        processing_state[process_id]['current_step'] = 'keyword_filtering'
        filtered_df = filter_by_keywords(df, all_keywords)

        if filtered_df.empty:
            processing_state[process_id]['status'] = 'completed'
            processing_state[process_id]['message'] = "No matches found for the selected keywords"
            return
        
        # Step 3: Remove duplicates
        processing_state[process_id]['current_step'] = 'deduplication'
        available_columns = filtered_df.columns.tolist()
        id_column = available_columns[0]
        keyword_column = available_columns[-1]
        df_clean = remove_duplicates(filtered_df, id_column, keyword_column)
        
        # Step 4: Filter by selected departments from map
        processing_state[process_id]['current_step'] = 'department_filtering'
        df_final = filter_by_departments(df_clean, target_departments_list)
        
        if df_final.empty:
            processing_state[process_id]['status'] = 'completed'
            processing_state[process_id]['message'] = f"No records found for selected departments: {', '.join(target_departments_list)}"
            return
        
        # Step 5: Process PDFs
        processing_state[process_id]['current_step'] = 'pdf_processing'
        processed_df = extract_pdf_content(df_final, process_id)
        
        # Create summary table
         # Update the summary_table creation to include extracted link
        # In the run_processing function, update the summary_table creation:
        summary_table = pd.DataFrame({
            "Keywords": processed_df.get('keyword', 'N/A'),
            'Acheteur': processed_df.get('nomacheteur', 'N/A'),
            'Objet': processed_df.get('objet', 'N/A'),
            'Lots': processed_df.get('lot_numbers', ''),
            'Visite Obligatoire': processed_df.get('visite_obligatoire', 'no'),
            'Département': processed_df.get('code_departement_trouve', processed_df.get('code_departement', 'N/A')),
            'Date Limite': processed_df.get('datelimitereponse', 'Pas Mentionné'),
            'PDF Link': processed_df.get('generated_link', 'N/A'),
            'Extracted Link': processed_df.get('primary_extracted_link', '')  # This should now contain the correct URL
        })
        
        processing_state[process_id]['summary_table'] = summary_table.to_dict('records')
        processing_state[process_id]['status'] = 'completed'
        processing_state[process_id]['message'] = f"Processing completed. Found {len(summary_table)} records."
        
    except Exception as e:
        processing_state[process_id]['status'] = 'error'
        processing_state[process_id]['error'] = str(e)
        print(f"Error in processing: {e}")

@app.get("/progress/{process_id}")
async def get_progress(process_id: str):
    """Get processing progress"""
    if process_id not in processing_state:
        raise HTTPException(status_code=404, detail="Process not found")
    
    return JSONResponse(processing_state[process_id])

@app.get("/download/{process_id}")
async def download_results(process_id: str):
    """Download results as Excel file"""
    if process_id not in processing_state:
        raise HTTPException(status_code=404, detail="Process not found")
    
    if processing_state[process_id]['status'] != 'completed':
        raise HTTPException(status_code=400, detail="Process not completed")
    
    # Convert result back to DataFrame
    result_data = processing_state[process_id].get('result', [])
    if not result_data:
        raise HTTPException(status_code=404, detail="No data available")
    
    df = pd.DataFrame(result_data)
    
    # Create Excel file in memory
    excel_buffer = io.BytesIO()
    df.to_excel(excel_buffer, index=False, engine='openpyxl')
    excel_buffer.seek(0)
    
    target_date = processing_state[process_id].get('target_date', 'unknown')
    filename = f"BOAMP_Full_Results_{target_date}_{datetime.now().strftime('%H%M%S')}.xlsx"
    
    return StreamingResponse(
        excel_buffer,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f"attachment; filename={filename}"}
    )

@app.get("/download-summary/{process_id}")
async def download_summary(process_id: str):
    """Download summary table as CSV"""
    if process_id not in processing_state:
        raise HTTPException(status_code=404, detail="Process not found")
    
    if processing_state[process_id]['status'] != 'completed':
        raise HTTPException(status_code=400, detail="Process not completed")
    
    # Get summary table
    summary_data = processing_state[process_id].get('summary_table', [])
    if not summary_data:
        raise HTTPException(status_code=404, detail="No summary data available")
    
    df = pd.DataFrame(summary_data)
    
    # Create CSV in memory
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False, encoding='utf-8-sig')
    csv_buffer.seek(0)
    
    target_date = processing_state[process_id].get('target_date', 'unknown')
    filename = f"BOAMP_Summary_{target_date}_{datetime.now().strftime('%H%M%S')}.csv"
    
    return StreamingResponse(
        io.BytesIO(csv_buffer.getvalue().encode('utf-8-sig')),
        media_type="text/csv",
        headers={"Content-Disposition": f"attachment; filename={filename}"}
    )
@app.post("/api/extract-pdf-link")
async def extract_pdf_link_api(
    pdf_url: str = Form(...),
    process_id: str = Form(None)
):
    """API endpoint to extract links from a PDF"""
    try:
        if not pdf_url or pdf_url == 'N/A':
            return JSONResponse({
                "success": False,
                "error": "No PDF URL provided"
            })
        
        # Extract links
        links = extract_links_from_pdf_content(pdf_url)
        primary_link = links[0] if links else None
        
        return JSONResponse({
            "success": True,
            "pdf_url": pdf_url,
            "extracted_links": links,
            "primary_link": primary_link,
            "count": len(links)
        })
        
    except Exception as e:
        return JSONResponse({
            "success": False,
            "error": str(e),
            "pdf_url": pdf_url
        })
if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)