import json
from pathlib import Path
from bs4 import BeautifulSoup
from collections import defaultdict
from nltk.stem.porter import PorterStemmer
from sacremoses import MosesTokenizer
import string
import re
from urllib.parse import urldefrag

# Example structure for a token in the index
# {'token': {'url': 'http://...', 'position': 12345, 'flags': {'title': True, 'bold': False, ...}}}

# Define tags to be parsed from HTML
tags = ['title', 'h1', 'h2', 'h3', 'b', 'strong', 'em', 'i', 'a']

def default_flags(): # Create a dictionary with all tags set to False
    return {tag: False for tag in tags}

# Initialize the stemmer and tokenizer
stemmer = PorterStemmer()
tokenizer = MosesTokenizer()
    
def normalize(url):
    url = urldefrag(url)[0].lower()
    
    # Handle links that are like "www.ics.uci.edu" by prefixing them with a scheme.
    if url.startswith("www."):
        url = "https://" + url
    
    # Remove trailing slash after a filename (like .php/)
    if re.search(r'\.\w+/$', url):
        url = url[:-1]

    return url

def read_json_chunk(file, chunk_size=20*1024*1024):  # 20MB FIX THE CHUNKING LATER
    chunk = file.read(chunk_size)
    brace_level = 0
    i = 0

    for i, char in enumerate(chunk):
        if char == '{':
            brace_level += 1
        elif char == '}':
            brace_level -= 1
            if brace_level == 0:
                break

    return chunk[:i+1]  # Return a valid JSON string

def create_intermediate_files(intermediate_dir):
    intermediate_path = Path(intermediate_dir)
    intermediate_path.mkdir(parents=True, exist_ok=True)

    merged_data = {}

    partial_index_files = list(intermediate_path.glob('../partial_index*.json'))

    for file_path in partial_index_files:
        with file_path.open('r') as file:
            while True:
                chunk = read_json_chunk(file)
                if not chunk:  # End of file
                    break

                partial_index = json.loads(chunk)
                for token, entries in partial_index.items():
                    alphanumeric_char = token[0] if token and token[0].isalnum() else "other"

                    if alphanumeric_char not in merged_data:
                        merged_data[alphanumeric_char] = {}

                    if token not in merged_data[alphanumeric_char]:
                        merged_data[alphanumeric_char][token] = []

                    merged_data[alphanumeric_char][token].extend(entries)

    for alphanumeric_char, tokens_data in merged_data.items():
        token_file = intermediate_path / f"{alphanumeric_char}.json"
        with token_file.open('w') as tf:
            for token, entries in sorted(tokens_data.items()):
                entry_json = {
                    "token": token,
                    "entries": entries
                }
                json.dump(entry_json, tf)
                tf.write("\n")

def merge_and_sort_token_data(intermediate_dir, final_index_dir): #MODIFY THIS TO JUST APPEND THE FILES IN THE RIGHT ORDER LATER
    final_index_file = Path(final_index_dir) / "final_index.json"

    # Create the final index file if it doesn't exist
    if not final_index_file.exists():
        final_index_file.touch()

    # Process each alphanumeric character
    for char in string.ascii_lowercase + string.digits:
        token_file = Path(intermediate_dir) / f"{char}.json"
        if token_file.exists():
            # Read and sort the data from the token_file
            with token_file.open('r') as tf:
                data = [json.loads(line) for line in tf]

            # Sort the data by the 'token' key
            sorted_data = sorted(data, key=lambda x: x["token"])

            # Append sorted data to the final index file
            with final_index_file.open('a') as f:
                for entry in sorted_data:
                    json.dump(entry, f)
                    f.write("\n")

def store_doc_id_map(doc_id_map, filename="doc_id_map.json"):
    file_path = Path(filename)
    with file_path.open('w') as f:
        json.dump(doc_id_map, f, indent=0, separators=(", ", ": "))

def storeIndices(index, index_num):
    file_path = Path(f"partial_index{index_num}.json")
    try:
        with file_path.open('w') as f:
            # No indentation, no spaces
            json.dump(index, f, separators=(',', ':'))
    except IOError as e:
        print(f"Error writing to file {file_path}: {e}")

# parse HTML, flag the tokens accordingly, keep track of token position
def parse_html(file_content):
    try:
        soup = BeautifulSoup(file_content, 'html.parser')
        text_elements = defaultdict(list)
        token_position = 0
    except Exception as e:
        print(f"Error parsing HTML content: {e}")
        return defaultdict(list)

    for tag in soup.find_all(True):
        is_title = tag.name == 'title'
        tokens = tokenizer.tokenize(tag.get_text(), escape=False)
        stemmed_tokens = [stemmer.stem(token) for token in tokens if token.isalnum()]

        for token in stemmed_tokens:
            flag_data = {}  # Use an empty dictionary to store only true flags
            if is_title:
                flag_data['title'] = True
            if tag.name in tags:
                flag_data[tag.name] = True

            # Only store flag_data if it's not empty
            if flag_data:
                text_elements[token].append({'position': token_position, 'flags': flag_data})
            token_position += 1

    return text_elements

# Create partial indices
def make_index(base_path, test_limit, doc_threshold=1000):
    partial_index = defaultdict(list)
    doc_id_map = {}
    seen_urls = set()
    doc_id = 0
    file_count = 0
    index_count = 1  # Starting index number try to remove reduncancy with doc_id later

    base_path = Path(base_path)

    for subdirectory in base_path.iterdir():
        if subdirectory.is_dir():
            for file_path in subdirectory.glob('*.json'):
                if file_count >= test_limit:
                    break

                try:
                    with file_path.open('r', encoding='utf-8') as f:
                        data = json.load(f)

                    url = normalize(data.get('url'))

                    if url in seen_urls:
                        continue

                    seen_urls.add(url)
                    print(f"Processing URL: {url}")

                    if url not in doc_id_map:
                        doc_id_map[url] = doc_id
                        current_doc_id = doc_id
                        doc_id += 1
                    else:
                        current_doc_id = doc_id_map[url]

                    tokens = parse_html(data['content'])
                    for token, occurrences in tokens.items():
                        for occurrence in occurrences:
                            partial_index[token].append({
                                'doc_id': current_doc_id, 
                                'position': occurrence['position'], 
                                'flags': occurrence['flags']
                            })

                except Exception as e:
                    print(f"Error processing file {file_path}: {e}")

                file_count += 1

                if doc_id % doc_threshold == 0 and doc_id != 0:
                    store_sorted_indices(partial_index, index_count)
                    partial_index.clear()
                    index_count += 1

    if partial_index:
        store_sorted_indices(partial_index, index_count)

    return doc_id_map

def store_sorted_indices(partial_index, index_num):
    sorted_index = {k: partial_index[k] for k in sorted(partial_index)}
    file_path = Path(f"partial_index{index_num}.json")
    with file_path.open('w') as f:
        json.dump(sorted_index, f)

if __name__ == "__main__":
    test_limit = 1000000  # Set this to the number of files you want to process for testing
    base_path = 'C:/Users/sabin/Documents/cs 121/ANALYST'

    doc_id_map = make_index(base_path, test_limit)
    store_doc_id_map(doc_id_map)

    intermediate_dir = 'C:/Users/sabin/Documents/cs 121/A3 1/intermediate'
    create_intermediate_files(intermediate_dir)

    final_index_dir = 'C:/Users/sabin/Documents/cs 121/A3 1'
    merge_and_sort_token_data(intermediate_dir, final_index_dir)


